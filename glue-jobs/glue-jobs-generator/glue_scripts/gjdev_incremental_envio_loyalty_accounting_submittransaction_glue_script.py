
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
df = wr.athena.read_sql_query(sql="select coalesce(cast(max(MODIFICATION_DATE) as varchar), cast(date_add('hour', -5, from_unixtime(cast(to_unixtime(current_timestamp) AS bigint))) as varchar)) as max_date from viamericas.accounting_submittransaction", database="viamericas")

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
qryStr = f"(SELECT [is_cancelled],[modification_date],[creation_date],[id], convert(date, [MODIFICATION_DATE]) as day FROM envio.loyalty.accounting_submittransaction WHERE  [MODIFICATION_DATE] >= '{today} 00:00:00') x"

jdbcDF = spark.read.format('jdbc')\
        .option('url', jdbc_viamericas)\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
        .option('dbtable', qryStr )\
        .option("user", secret['username'])\
        .option("password", secret['password'])\
        .option('partitionColumn', 'MODIFICATION_DATE')\
        .option("lowerBound", f'{today} 00:00:00')\
        .option("upperBound", f'{today} 23:59:59')\
        .option("numPartitions", 10)\
        .option("fetchsize", 1000)\
        .load()

number_of_rows = jdbcDF.count()

print(f'number of rows obtained for date higher than {today}: {number_of_rows}')

# jdbcDF = jdbcDF.withColumn('day', date_format('MODIFICATION_DATE', 'yyyy-MM-dd'))

if number_of_rows > 0: 
    # Definir la ruta de salida en S3
    s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/loyalty/accounting_submittransaction/"

    # Escribir el DataFrame en formato Parquet en S3
    jdbcDF.write.partitionBy("day").parquet(s3_output_path, mode="overwrite")
    
    print(f'Data for the day: {today} written succesfully')
else:
    print(f'No data for the date: {today}')
    