
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
qryStr = f"(SELECT [COSIGNER_NAME] ,[MAILADD_STREET] ,[ID_NUMBER] ,[CONTRACT_VER] ,[OWNER_SLNAME] ,[TIME_INBUSINESS] ,[OWNER_CREDITSCORE] ,[PHONE_2] ,[OWNER2_NAME] ,[OWNER_CREDITSCORE_DATE] ,[UCC_EXPIRES_DATE] ,[NAME_STATE] ,[PRINTCHECKAS] ,[ID_COMPANY_TYPE] ,[TAX_ID] ,[MAILADD_ZIPCODE] ,[MAILADD_ZIPCODE_V] ,[MAILADD_ZIP4CODE] ,[OWNER4_CREDITSCORE] ,[OWNER_LNAME] ,[EIN] ,[NAME_CITY] ,[PREEXISTING_UCC] ,[OWNER3_NAME] ,[PHONE_1] ,[PREEXISTING_UCC_COMMENTS] ,[ID_BRANCH] ,[OWNER3_CREDITSCORE] ,[OWNER_NICKNAME] ,[ID_STATE] ,[ID_EXP_DATE] ,[SYNC_COMPASS] ,[OWNER_FNAME] ,[OWNER4_NAME] ,[SYNC_COMPASS_COD_IND] ,[NAME_1099] ,[MAILADD_ID_STATE] ,[COMPANY_TYPE] ,[ID_CITY] ,[LEGAL_NAME] ,[CELLULAR_NUMBER] ,[NUMBER_1099] ,[ZIP_CODE] ,[ZIP4_CODE] ,[ZIPCODE_V] ,[UCC_CREATED_DATE] ,[OWNER2_CREDITSCORE] ,[MAILADD_NAME_CITY] ,[FILE_UCC] ,[OWNER_MNAME] ,[ID_TYPE] ,[OWNER_SSN] ,[OWNER_ADDRESS] FROM envio.dba.branch_owner_information) x"

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
    