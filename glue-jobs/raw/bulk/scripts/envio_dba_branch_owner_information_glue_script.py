
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
qryStr = f"(SELECT [MAILADD_ID_STATE] ,[NUMBER_1099] ,[ID_TYPE] ,[MAILADD_ZIPCODE_V] ,[TIME_INBUSINESS] ,[PHONE_1] ,[OWNER3_NAME] ,[OWNER_SLNAME] ,[ID_NUMBER] ,[NAME_CITY] ,[ZIP_CODE] ,[COSIGNER_NAME] ,[MAILADD_ZIP4CODE] ,[OWNER4_CREDITSCORE] ,[ID_EXP_DATE] ,[ZIPCODE_V] ,[OWNER_NICKNAME] ,[LEGAL_NAME] ,[ID_STATE] ,[CONTRACT_VER] ,[NAME_1099] ,[ZIP4_CODE] ,[PREEXISTING_UCC_COMMENTS] ,[SYNC_COMPASS] ,[EIN] ,[OWNER_ADDRESS] ,[COMPANY_TYPE] ,[CELLULAR_NUMBER] ,[SYNC_COMPASS_COD_IND] ,[OWNER_CREDITSCORE_DATE] ,[PRINTCHECKAS] ,[ID_COMPANY_TYPE] ,[OWNER2_NAME] ,[OWNER2_CREDITSCORE] ,[OWNER_SSN] ,[OWNER_FNAME] ,[ID_CITY] ,[MAILADD_ZIPCODE] ,[OWNER_CREDITSCORE] ,[PHONE_2] ,[OWNER_LNAME] ,[MAILADD_NAME_CITY] ,[NAME_STATE] ,[MAILADD_STREET] ,[FILE_UCC] ,[ID_BRANCH] ,[OWNER4_NAME] ,[OWNER_MNAME] ,[UCC_EXPIRES_DATE] ,[PREEXISTING_UCC] ,[OWNER3_CREDITSCORE] ,[UCC_CREATED_DATE] ,[TAX_ID] FROM envio.dba.branch_owner_information) x"

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
    