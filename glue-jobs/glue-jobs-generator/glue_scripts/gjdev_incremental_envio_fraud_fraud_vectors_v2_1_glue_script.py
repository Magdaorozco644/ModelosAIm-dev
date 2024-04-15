
import boto3, json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, date_format
import awswrangler as wr
from datetime import date

# Contexto
sc = SparkContext()
spark = SparkSession(sc)
glueContext = GlueContext(spark)

today = date.today()

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# Get max date in athena
df = wr.athena.read_sql_query(sql="select coalesce(cast(max(DATE_CREATED) as varchar), cast(date_add('hour', -5, from_unixtime(cast(to_unixtime(current_timestamp) AS bigint))) as varchar)) as max_date from viamericas.fraud_vectors_v2_1", database="viamericas")

athena_max_date = df['max_date'].tolist()[0]

def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    secret_=json.loads(secret)
    return secret_

# Obtener credenciales para SQL Server
secret_name = "SQLSERVER-CREDENTIALS"
region_name = "us-east-1"
secret = get_secret(secret_name, region_name)

jdbc_viamericas = "jdbc:sqlserver://172.17.13.45:1433;database=Envio"
qryStr = f"(SELECT [PAYOUT_C],[IN_RANGE],[%TX_BRANCITY],[ID],[WEEKDAY_6.0],[ID_SALES_REPRESENTATIVE],[PAYOUT_X],[WEEKDAY_2],[WEEKDAY_4.0],[WEEKDAY_3],[TRANSACTION_UNIQUE],[PAYOUT_6],[PAYOUT_1],[IDPAYER_FRAUD],[WEEKDAY_2.0],[PAYOUT_P],[WEEKDAY_5.0],[IDLOCATION_FRAUD],[RECEIVER_TRANSACTION_COUNT],[ID_COUNTRY_RECEIVER],[ID_BRANCH],[PAYOUT_7],[WEEKDAY_5],[TX_BRANCITY],[ADDRESS_RECEIVER_TRANSACTION_COUNT],[PAYOUT_0],[PAYOUT_M],[ID_RECEIVER],[WEEKDAY_6],[HOUR_SENDER],[WEEKDAY_4],[RECEIVER_FRAUD],[BRANCH_WORKING_DAYS],[SENDER_DAYS_TO_LAST_TRANSACTION],[PAYOUT_5],[PAYOUT_T],[PAYOUT_N],[SENDER_MINUTES_SINCE_LAST_TRANSACTION],[PAYOUT_4],[WEEKDAY_1],[PAYOUT_3],[PAYOUT_S],[WEEKDAY_3.0],[DATE_CREATED],[SENDER_FRAUD],[WEEKDAY_1.0],[PAYOUT_2],[MODE_PAY_RECEIVER],[PAYOUT_D],[NET_AMOUNT_RECEIVER],[PAYOUT_O],[SENDER_SENDING_DAYS],[BRANCH_MINUTES_SINCE_LAST_TRANSACTION], convert(date, [DATE_CREATED]) as day FROM envio.fraud.fraud_vectors_v2_1 WHERE  [DATE_CREATED] >= '{today} 00:00:00') x"

jdbcDF = spark.read.format('jdbc')\
        .option('url', jdbc_viamericas)\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
        .option('dbtable', qryStr )\
        .option("user", secret['username'])\
        .option("password", secret['password'])\
        .option('partitionColumn', 'DATE_CREATED')\
        .option("lowerBound", f'{today} 00:00:00')\
        .option("upperBound", f'{today} 23:59:59')\
        .option("numPartitions", 10)\
        .option("fetchsize", 1000)\
        .load()

number_of_rows = jdbcDF.count()

print(f'number of rows obtained for date higher than {today}: {number_of_rows}')

# jdbcDF = jdbcDF.withColumn('day', date_format('DATE_CREATED', 'yyyy-MM-dd'))

if number_of_rows > 0: 
    # Definir la ruta de salida en S3
    s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/fraud/fraud_vectors_v2_1/"

    # Escribir el DataFrame en formato Parquet en S3
    jdbcDF.write.partitionBy("day").parquet(s3_output_path, mode="overwrite")
    
    print(f'Data for the day: {today} written succesfully')
else:
    print(f'No data for the date: {today}')
    