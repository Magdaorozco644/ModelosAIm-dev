
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
qryStr = f"(SELECT [IS_HIDE] ,[ID_FLAG_BILLPAYMENT] ,[PASSPORT_AVAILABLE] ,[SEN_SLNAME] ,[ADDRES2_SENDER] ,[PHONE1_COUNTRY_CODE] ,[RECEIPTTO] ,[ID_COUNTRY_ISSUER_ID] ,[INSERTED_DATE] ,[SEN_EMPLOYER_PHONE_V] ,[SEN_EMPLOYER_ZIP_CODE_V] ,[ID_SENDER_GLOBAL] ,[SEN_EMPLOYER_CITY] ,[SEN_EMPLOYER_NAME] ,[CITY_SENDER] ,[SEN_PAYMENTTYPE] ,[SEN_RELATIONSHIP] ,[ID_CITY] ,[ADDRES_SENDER] ,[SSN_AVAILABLE] ,[PHONE2_TYPE] ,[SEN_EMPLOYER_PHONE_COUNTRY_CODE] ,[UUID] ,[NAME_SENDER] ,[SEN_ACCROUTING] ,[SEN_FUNDSWILLBEUSEDFOR] ,[STATE_SENDER] ,[ID_JOB] ,[SEN_EMPLOYER_ADDR] ,[PHONE2_SENDER] ,[SEN_ACCTYPE] ,[SEN_ACCREFERENCIAS] ,[ID_TYPE_ID_SENDER] ,[SEN_EMPLOYER_ID_COUNTRY] ,[VIACARD] ,[FLAG_PHONE1_ACTIVATE] ,[PASSPORT_SENDER] ,[COUNTRY_BIRTH_SENDER] ,[PHONE1_SENDER_MOBILE_PROVIDER] ,[SEN_ACCNUMBER] ,[SEN_EMPLOYER_ID_CITY] ,[CITY_SENDER_OLD] ,[SEN_EMPLOYER_ID_STATE] ,[PREFERRED_COUNTRY] ,[PROMOTIONAL_MESSAGES] ,[linked_id_sender] ,[TYPE_NOTIFICATION_ID] ,[PHONE2_COUNTRY_CODE] ,[I_AUTHORIZATION] ,[SSN_SENDER] ,[ID_COUNTRY_ISSUER_PASSPORT] ,[DOB_SENDER] ,[ID_STATE_ISSUER_ID] ,[SEN_SOURCEOFFUNDS] ,[ID_BRANCH] ,[PHONE1_SENDER] ,[PHONE1_TYPE] ,[ADD_BITMAP_SENDER] ,[EXPIRATION_DATE_ID] ,[ID_INDUSTRY] ,[SEN_FNAME] ,[SEN_EMPLOYER_ZIP_CODE] ,[SEN_ACCBANK] ,[SEN_LANGUAGE] ,[SEN_LNAME] ,[ZIP_SENDER] ,[ZIP_SENDER_V] ,[NUMBER_ID_SENDER] ,[OCCUPATION_SENDER] ,[SEN_EMPLOYER_PHONE] ,[SEN_MNAME] ,[IDOLOGY] ,[ID_SENDER] ,[EMAIL_SENDER] ,[ID_COUNTRY] ,[ID_STATE] FROM envio.dba.sender) x"

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
    