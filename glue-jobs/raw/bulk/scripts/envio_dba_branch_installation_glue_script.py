
import boto3, json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date

# Contexto
sc = SparkContext()
spark = SparkSession(sc)
glueContext = GlueContext(spark)
glueContext.setTempDir("s3://viamericas-datalake-dev-us-east-1-283731589572-athena/gluetmp/")


spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

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
qryStr = f"(SELECT [ID_MAIN_BRANCH] ,[ACTIVE_VIAKOM] ,[ACTIVE_CENTRAL] ,[NOW_INSTALLATION_DATE] ,[DATE_BILLPAYMENT] ,[DATE_DEBIT_CARD] ,[DATE_TOPUPS] ,[ACTIVE_RP2_0] ,[FIRST_INSTALLATION_DATE] ,[ACTIVE_PC] ,[ACTIVE_BILLPAYMENT] ,[DATE_VIAKOM] ,[DATE_VIASAFE] ,[ACTIVE_DEBIT_CARD] ,[TYPE] ,[ACTIVE_DATE_RP] ,[ACTIVE_VIATAB] ,[DATE_RP] ,[ID_BRANCH] ,[ID_LOCATION] ,[ACTIVE_VIASAFE] ,[ACTIVE_MONEYORDER] ,[DATE_VIACHECK] ,[ACTIVE_TOPUPS] ,[DATE_VIATAB] ,[DATE_PC] ,[DATE_CENTRAL] ,[ACTIVE_VIACHECK] ,[DATE_MONEYORDER] ,[DATE_RP2_0] FROM envio.dba.branch_installation) x"

jdbcDF = spark.read.format('jdbc')\
        .option('url', jdbc_viamericas)\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
        .option('dbtable', qryStr )\
        .option("user", secret['username'])\
        .option("password", secret['password'])\
        .option("numPartitions", 10)\
        .option("fetchsize", 1000)\
        .load()

# Definir la ruta de salida en S3
s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/branch_installation/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.parquet(s3_output_path, mode="overwrite")
    