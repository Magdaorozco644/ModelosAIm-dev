
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
qryStr = f"(SELECT [ALTERNATIVE_ID_BRANCH] ,[MODEM_BRANCH] ,[DATE_CLOSE] ,[ID_COMPANY] ,[PAYER_CAPTION] ,[USE_ALTERNATIVE] ,[ID_STATUS_BRANCH] ,[IS_CENPOS_AVAIBLE] ,[GEO_PRECISION] ,[IS_MONEY_ORDERS] ,[ADDRESS2_BRANCH] ,[ID_TYPE_BRANCH] ,[CURRENCY_PAY_BRANCH] ,[IS_BANK] ,[TIMELIMITPRINTPIN] ,[PRODUCT_TYPE] ,[BUSINESS_HOURS] ,[ID_STATE] ,[USER_NAME] ,[FORMA_PAGO_BRANCH] ,[GEO_GOOGLE] ,[LAST_CHANGE] ,[LONGITUD] ,[PHONE2_BRANCH] ,[SYNC_COMPASS] ,[DATE_OPEN] ,[DATE_CRE_BRANCH] ,[ID_GROUP_NETWORK] ,[PHONE1_BRANCH] ,[SYNC_ASSETS] ,[LAST_INACTIVATION_TYPE] ,[INSTALLATION_TYPE] ,[LAST_STATUS_COMMENT] ,[id_flag_branch] ,[PAYER_LOCATION_OWN] ,[ALT_FAX_ROUTE] ,[NAME_BRANCH] ,[ID_FLAG_BRANCH_TEMP] ,[PREPARE_PIN] ,[ZIP4CODE_BRANCH] ,[ID_CITY] ,[ID_COUNTRY] ,[IS_VIP] ,[ADDRESS_BRANCH] ,[LATITUD] ,[LOCATION_NOTES] ,[ID_LOCATION] ,[ID_BRANCH] ,[ID_CURRENCY_SOURCE] ,[USU_CRE_BRANCH] ,[DATE_INSERTED] ,[ID_CHAIN] ,[PAYOR_CODE] ,[ID_PAYER_NETWORK] ,[FAX_BRANCH] ,[ZIP_BRANCH] ,[ID_MAIN_BRANCH] ,[ZIP_BRANCH_V] FROM envio.dba.branch) x"

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
    