
import boto3, json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, date_format
import awswrangler as wr

# Contexto
sc = SparkContext()
spark = SparkSession(sc)
glueContext = GlueContext(spark)

spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# Get max date in athena
df = wr.athena.read_sql_query(sql="select cast(max(DATE_CREATED) as varchar) as max_date from viamericas.fraud_vectors_v2_1", database="viamericas")

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
qryStr = f"(SELECT [SENDER_SENDING_DAYS],[PAYOUT_7],[ID_COUNTRY_RECEIVER],[NET_AMOUNT_RECEIVER],[PAYOUT_D],[BRANCH_WORKING_DAYS],[IDLOCATION_FRAUD],[WEEKDAY_2],[WEEKDAY_5],[ID_BRANCH],[SENDER_DAYS_TO_LAST_TRANSACTION],[SENDER_MINUTES_SINCE_LAST_TRANSACTION],[TRANSACTION_UNIQUE],[WEEKDAY_2.0],[PAYOUT_X],[SENDER_FRAUD],[PAYOUT_5],[PAYOUT_N],[WEEKDAY_4.0],[HOUR_SENDER],[IN_RANGE],[ID],[IDPAYER_FRAUD],[ID_RECEIVER],[WEEKDAY_4],[BRANCH_MINUTES_SINCE_LAST_TRANSACTION],[%TX_BRANCITY],[TX_BRANCITY],[WEEKDAY_5.0],[DATE_CREATED],[ID_SALES_REPRESENTATIVE],[PAYOUT_M],[PAYOUT_4],[PAYOUT_T],[ADDRESS_RECEIVER_TRANSACTION_COUNT],[PAYOUT_6],[PAYOUT_0],[MODE_PAY_RECEIVER],[WEEKDAY_1.0],[PAYOUT_O],[PAYOUT_2],[RECEIVER_FRAUD],[PAYOUT_C],[PAYOUT_1],[WEEKDAY_1],[WEEKDAY_3.0],[PAYOUT_P],[WEEKDAY_3],[RECEIVER_TRANSACTION_COUNT],[WEEKDAY_6.0],[PAYOUT_3],[PAYOUT_S],[WEEKDAY_6] FROM envio.fraud.fraud_vectors_v2_1 WHERE DATE_CREATED > '{athena_max_date}') x"

jdbcDF = spark.read.format('jdbc')\
        .option('url', jdbc_viamericas)\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
        .option('dbtable', qryStr )\
        .option("user", secret['username'])\
        .option("password", secret['password'])\
        .option("numPartitions", 10)\
        .option("fetchsize", 1000)\
        .load()

print(f'number of rows obtained for date higher than {athena_max_date}: {jdbcDF.count()}')

jdbcDF = jdbcDF.withColumn('day', date_format('DATE_CREATED', 'yyyy-MM-dd'))

# Definir la ruta de salida en S3
s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/fraud/fraud_vectors_v2_1/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.partitionBy("day").parquet(s3_output_path, mode="append")
    