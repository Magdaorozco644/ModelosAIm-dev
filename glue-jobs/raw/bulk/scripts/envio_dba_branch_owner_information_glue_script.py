
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
qryStr = f"(SELECT [OWNER_CREDITSCORE] ,[ZIPCODE_V] ,[OWNER_CREDITSCORE_DATE] ,[MAILADD_ZIPCODE_V] ,[MAILADD_ZIPCODE] ,[MAILADD_NAME_CITY] ,[MAILADD_ZIP4CODE] ,[NAME_STATE] ,[CELLULAR_NUMBER] ,[LEGAL_NAME] ,[OWNER_ADDRESS] ,[ID_NUMBER] ,[ID_STATE] ,[OWNER2_NAME] ,[OWNER_LNAME] ,[ID_CITY] ,[NAME_CITY] ,[OWNER_MNAME] ,[NAME_1099] ,[TIME_INBUSINESS] ,[OWNER3_CREDITSCORE] ,[OWNER4_NAME] ,[EIN] ,[PREEXISTING_UCC] ,[MAILADD_STREET] ,[OWNER_SLNAME] ,[COMPANY_TYPE] ,[CONTRACT_VER] ,[OWNER4_CREDITSCORE] ,[PHONE_1] ,[ID_BRANCH] ,[SYNC_COMPASS_COD_IND] ,[UCC_CREATED_DATE] ,[SYNC_COMPASS] ,[OWNER3_NAME] ,[ZIP_CODE] ,[OWNER2_CREDITSCORE] ,[NUMBER_1099] ,[MAILADD_ID_STATE] ,[PHONE_2] ,[OWNER_NICKNAME] ,[PRINTCHECKAS] ,[ZIP4_CODE] ,[FILE_UCC] ,[OWNER_FNAME] ,[UCC_EXPIRES_DATE] ,[COSIGNER_NAME] ,[ID_COMPANY_TYPE] ,[TAX_ID] ,[PREEXISTING_UCC_COMMENTS] ,[ID_TYPE] ,[OWNER_SSN] ,[ID_EXP_DATE] FROM envio.dba.branch_owner_information) x"

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
s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/branch_owner_information/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.parquet(s3_output_path, mode="overwrite")
    