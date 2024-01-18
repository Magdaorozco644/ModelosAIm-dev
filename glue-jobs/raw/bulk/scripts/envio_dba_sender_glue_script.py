
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
qryStr = f"(SELECT [SEN_EMPLOYER_PHONE_COUNTRY_CODE] ,[PHONE1_COUNTRY_CODE] ,[PHONE2_TYPE] ,[SEN_EMPLOYER_ADDR] ,[SEN_EMPLOYER_CITY] ,[ID_SENDER_GLOBAL] ,[SEN_ACCROUTING] ,[SEN_EMPLOYER_ZIP_CODE_V] ,[ID_JOB] ,[SSN_AVAILABLE] ,[SEN_ACCREFERENCIAS] ,[SEN_EMPLOYER_ID_COUNTRY] ,[ZIP_SENDER] ,[SEN_ACCBANK] ,[SEN_FUNDSWILLBEUSEDFOR] ,[EXPIRATION_DATE_ID] ,[ID_TYPE_ID_SENDER] ,[ID_STATE] ,[FLAG_PHONE1_ACTIVATE] ,[PROMOTIONAL_MESSAGES] ,[DOB_SENDER] ,[ID_CITY] ,[ID_FLAG_BILLPAYMENT] ,[ID_STATE_ISSUER_ID] ,[PHONE2_COUNTRY_CODE] ,[UUID] ,[SEN_EMPLOYER_ID_CITY] ,[ZIP_SENDER_V] ,[SEN_PAYMENTTYPE] ,[INSERTED_DATE] ,[SEN_RELATIONSHIP] ,[ID_COUNTRY] ,[ID_SENDER] ,[SEN_EMPLOYER_NAME] ,[ID_BRANCH] ,[VIACARD] ,[SEN_SOURCEOFFUNDS] ,[PASSPORT_AVAILABLE] ,[COUNTRY_BIRTH_SENDER] ,[ID_COUNTRY_ISSUER_ID] ,[PHONE1_TYPE] ,[SEN_EMPLOYER_ID_STATE] ,[SEN_EMPLOYER_PHONE] ,[ID_COUNTRY_ISSUER_PASSPORT] ,[SEN_EMPLOYER_PHONE_V] ,[SEN_LANGUAGE] ,[CITY_SENDER] ,[ID_INDUSTRY] ,[linked_id_sender] ,[SEN_EMPLOYER_ZIP_CODE] ,[RECEIPTTO] ,[PHONE1_SENDER_MOBILE_PROVIDER] ,[TYPE_NOTIFICATION_ID] ,[OCCUPATION_SENDER] ,[STATE_SENDER] ,[CITY_SENDER_OLD] ,[PREFERRED_COUNTRY] ,[SEN_ACCTYPE] ,[IS_HIDE] FROM envio.dba.sender) x"

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
s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/sender/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.parquet(s3_output_path, mode="overwrite")
    