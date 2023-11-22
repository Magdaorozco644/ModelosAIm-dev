
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
qryStr = f"(SELECT [EXPIRATION_DATE_ID] ,[ID_TYPE_ID_SENDER] ,[SEN_EMPLOYER_NAME] ,[PASSPORT_AVAILABLE] ,[PHONE2_SENDER] ,[SSN_SENDER] ,[EMAIL_SENDER] ,[ID_SENDER] ,[SEN_EMPLOYER_ID_COUNTRY] ,[linked_id_sender] ,[I_AUTHORIZATION] ,[ID_INDUSTRY] ,[SEN_LNAME] ,[ID_STATE_ISSUER_ID] ,[UUID] ,[SEN_SLNAME] ,[ID_JOB] ,[PASSPORT_SENDER] ,[SEN_EMPLOYER_ADDR] ,[PHONE2_COUNTRY_CODE] ,[ID_COUNTRY] ,[SEN_EMPLOYER_PHONE_COUNTRY_CODE] ,[SEN_EMPLOYER_ZIP_CODE_V] ,[SEN_EMPLOYER_ID_STATE] ,[SEN_EMPLOYER_ZIP_CODE] ,[ID_SENDER_GLOBAL] ,[OCCUPATION_SENDER] ,[ZIP_SENDER_V] ,[ADDRES_SENDER] ,[SEN_ACCNUMBER] ,[CITY_SENDER_OLD] ,[ADD_BITMAP_SENDER] ,[SEN_FUNDSWILLBEUSEDFOR] ,[COUNTRY_BIRTH_SENDER] ,[IS_HIDE] ,[PHONE1_COUNTRY_CODE] ,[DOB_SENDER] ,[SEN_EMPLOYER_ID_CITY] ,[ID_BRANCH] ,[NUMBER_ID_SENDER] ,[STATE_SENDER] ,[SEN_EMPLOYER_PHONE] ,[PHONE1_SENDER] ,[RECEIPTTO] ,[SEN_ACCBANK] ,[PHONE1_TYPE] ,[SEN_SOURCEOFFUNDS] ,[PREFERRED_COUNTRY] ,[ID_COUNTRY_ISSUER_ID] ,[ID_STATE] ,[SEN_ACCREFERENCIAS] ,[ID_COUNTRY_ISSUER_PASSPORT] ,[PHONE1_SENDER_MOBILE_PROVIDER] ,[ZIP_SENDER] ,[PHONE2_TYPE] ,[SEN_MNAME] ,[PROMOTIONAL_MESSAGES] ,[SSN_AVAILABLE] ,[ADDRES2_SENDER] ,[ID_CITY] ,[TYPE_NOTIFICATION_ID] ,[CITY_SENDER] ,[IDOLOGY] ,[FLAG_PHONE1_ACTIVATE] ,[SEN_ACCTYPE] ,[SEN_FNAME] ,[NAME_SENDER] ,[SEN_PAYMENTTYPE] ,[SEN_EMPLOYER_CITY] ,[SEN_EMPLOYER_PHONE_V] ,[VIACARD] ,[SEN_ACCROUTING] ,[SEN_RELATIONSHIP] ,[INSERTED_DATE] ,[SEN_LANGUAGE] ,[ID_FLAG_BILLPAYMENT] FROM envio.dba.sender) x"

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
    