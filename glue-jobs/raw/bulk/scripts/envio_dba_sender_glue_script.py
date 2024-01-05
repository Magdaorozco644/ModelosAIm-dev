
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
qryStr = f"(SELECT [UUID] ,[IS_HIDE] ,[linked_id_sender] ,[DOB_SENDER] ,[OCCUPATION_SENDER] ,[PHONE2_COUNTRY_CODE] ,[SEN_ACCNUMBER] ,[SSN_AVAILABLE] ,[PHONE1_TYPE] ,[ZIP_SENDER] ,[IDOLOGY] ,[PREFERRED_COUNTRY] ,[I_AUTHORIZATION] ,[SEN_LNAME] ,[CITY_SENDER] ,[CITY_SENDER_OLD] ,[SEN_ACCTYPE] ,[ID_STATE_ISSUER_ID] ,[VIACARD] ,[STATE_SENDER] ,[EMAIL_SENDER] ,[ID_SENDER] ,[ID_SENDER_GLOBAL] ,[SEN_EMPLOYER_NAME] ,[SEN_EMPLOYER_PHONE_COUNTRY_CODE] ,[SEN_PAYMENTTYPE] ,[ID_STATE] ,[SEN_RELATIONSHIP] ,[SEN_EMPLOYER_ADDR] ,[SEN_MNAME] ,[SEN_FUNDSWILLBEUSEDFOR] ,[PROMOTIONAL_MESSAGES] ,[SEN_SLNAME] ,[SEN_EMPLOYER_PHONE_V] ,[COUNTRY_BIRTH_SENDER] ,[ID_JOB] ,[SEN_EMPLOYER_PHONE] ,[ADDRES2_SENDER] ,[INSERTED_DATE] ,[ID_COUNTRY_ISSUER_ID] ,[ID_COUNTRY_ISSUER_PASSPORT] ,[NAME_SENDER] ,[ID_TYPE_ID_SENDER] ,[TYPE_NOTIFICATION_ID] ,[PASSPORT_SENDER] ,[PHONE1_SENDER] ,[ZIP_SENDER_V] ,[SEN_FNAME] ,[ADDRES_SENDER] ,[NUMBER_ID_SENDER] ,[ID_INDUSTRY] ,[PHONE2_TYPE] ,[SEN_ACCROUTING] ,[SEN_EMPLOYER_CITY] ,[SEN_EMPLOYER_ID_COUNTRY] ,[PASSPORT_AVAILABLE] ,[SEN_EMPLOYER_ZIP_CODE_V] ,[PHONE1_SENDER_MOBILE_PROVIDER] ,[ID_CITY] ,[ID_COUNTRY] ,[SEN_ACCREFERENCIAS] ,[SEN_EMPLOYER_ZIP_CODE] ,[SSN_SENDER] ,[EXPIRATION_DATE_ID] ,[ADD_BITMAP_SENDER] ,[PHONE1_COUNTRY_CODE] ,[ID_BRANCH] ,[SEN_EMPLOYER_ID_STATE] ,[SEN_LANGUAGE] ,[SEN_ACCBANK] ,[SEN_SOURCEOFFUNDS] ,[SEN_EMPLOYER_ID_CITY] ,[PHONE2_SENDER] ,[FLAG_PHONE1_ACTIVATE] ,[RECEIPTTO] ,[ID_FLAG_BILLPAYMENT] FROM envio.dba.sender) x"

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
    