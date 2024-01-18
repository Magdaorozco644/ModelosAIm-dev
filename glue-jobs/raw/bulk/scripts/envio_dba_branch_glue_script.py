
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
qryStr = f"(SELECT [IS_BANK] ,[PHONE2_BRANCH] ,[INSTALLATION_TYPE] ,[LAST_INACTIVATION_TYPE] ,[FORMA_PAGO_BRANCH] ,[ID_COMPANY] ,[LAST_CHANGE] ,[IS_CENPOS_AVAIBLE] ,[ID_STATE] ,[ID_CITY] ,[ZIP_BRANCH_V] ,[ADDRESS_BRANCH] ,[ID_CHAIN] ,[ID_TYPE_BRANCH] ,[PAYER_CAPTION] ,[NAME_BRANCH] ,[DATE_INSERTED] ,[ADDRESS2_BRANCH] ,[id_flag_branch] ,[PAYER_LOCATION_OWN] ,[GEO_GOOGLE] ,[ZIP_BRANCH] ,[CURRENCY_PAY_BRANCH] ,[FAX_BRANCH] ,[IS_VIP] ,[LONGITUD] ,[ID_COUNTRY] ,[ID_STATUS_BRANCH] ,[ID_BRANCH] ,[LOCATION_NOTES] ,[USER_NAME] ,[PAYOR_CODE] ,[USU_CRE_BRANCH] ,[PHONE1_BRANCH] ,[DATE_CLOSE] ,[ID_PAYER_NETWORK] ,[ID_GROUP_NETWORK] ,[DATE_OPEN] ,[LATITUD] ,[PRODUCT_TYPE] ,[GEO_PRECISION] ,[ID_LOCATION] ,[ID_CURRENCY_SOURCE] ,[ZIP4CODE_BRANCH] ,[DATE_CRE_BRANCH] ,[LAST_STATUS_COMMENT] ,[ID_MAIN_BRANCH] ,[BUSINESS_HOURS] ,[IS_MONEY_ORDERS] FROM envio.dba.branch) x"

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
s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/branch/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.parquet(s3_output_path, mode="overwrite")
    