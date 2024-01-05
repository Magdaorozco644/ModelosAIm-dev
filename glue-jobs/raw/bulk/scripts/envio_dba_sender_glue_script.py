
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
qryStr = f"(SELECT [ID_COUNTRY_ISSUER_PASSPORT] ,[IDOLOGY] ,[ID_COUNTRY] ,[PASSPORT_AVAILABLE] ,[PROMOTIONAL_MESSAGES] ,[SEN_EMPLOYER_PHONE] ,[SEN_EMPLOYER_PHONE_V] ,[SEN_MNAME] ,[SEN_ACCBANK] ,[ID_COUNTRY_ISSUER_ID] ,[SEN_LNAME] ,[DOB_SENDER] ,[SEN_PAYMENTTYPE] ,[NAME_SENDER] ,[PHONE1_COUNTRY_CODE] ,[PHONE1_TYPE] ,[PHONE2_SENDER] ,[ID_FLAG_BILLPAYMENT] ,[ID_JOB] ,[SEN_ACCNUMBER] ,[SEN_EMPLOYER_ZIP_CODE] ,[CITY_SENDER] ,[SEN_EMPLOYER_ZIP_CODE_V] ,[SEN_SOURCEOFFUNDS] ,[STATE_SENDER] ,[ID_BRANCH] ,[PHONE1_SENDER] ,[EMAIL_SENDER] ,[TYPE_NOTIFICATION_ID] ,[SEN_ACCROUTING] ,[SEN_LANGUAGE] ,[ADD_BITMAP_SENDER] ,[PHONE1_SENDER_MOBILE_PROVIDER] ,[PASSPORT_SENDER] ,[SEN_ACCTYPE] ,[OCCUPATION_SENDER] ,[NUMBER_ID_SENDER] ,[ADDRES_SENDER] ,[SEN_EMPLOYER_ADDR] ,[SEN_RELATIONSHIP] ,[INSERTED_DATE] ,[ID_INDUSTRY] ,[SEN_SLNAME] ,[VIACARD] ,[UUID] ,[CITY_SENDER_OLD] ,[SEN_EMPLOYER_PHONE_COUNTRY_CODE] ,[COUNTRY_BIRTH_SENDER] ,[RECEIPTTO] ,[ID_TYPE_ID_SENDER] ,[SEN_EMPLOYER_CITY] ,[ID_STATE] ,[SEN_EMPLOYER_ID_CITY] ,[I_AUTHORIZATION] ,[PHONE2_TYPE] ,[SSN_AVAILABLE] ,[ZIP_SENDER_V] ,[ID_SENDER_GLOBAL] ,[ADDRES2_SENDER] ,[PHONE2_COUNTRY_CODE] ,[ID_STATE_ISSUER_ID] ,[FLAG_PHONE1_ACTIVATE] ,[ID_CITY] ,[EXPIRATION_DATE_ID] ,[SEN_EMPLOYER_ID_STATE] ,[PREFERRED_COUNTRY] ,[SEN_ACCREFERENCIAS] ,[SEN_FNAME] ,[SEN_EMPLOYER_NAME] ,[SSN_SENDER] ,[linked_id_sender] ,[ID_SENDER] ,[SEN_FUNDSWILLBEUSEDFOR] ,[ZIP_SENDER] ,[SEN_EMPLOYER_ID_COUNTRY] ,[IS_HIDE] FROM envio.dba.sender) x"

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
    