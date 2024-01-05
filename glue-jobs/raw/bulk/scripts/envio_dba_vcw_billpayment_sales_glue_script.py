
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
qryStr = f"(SELECT [LOYALTY_BALANCE] ,[ADDITIONAL_COMISSION] ,[FIXDEBIT] ,[PRECASH_RHTTN] ,[TRANSACTION_DATE] ,[ID_BILLPAYMENT_SALES] ,[PROVIDER_MESSAGE_LIST] ,[AGENCY_COMMISSION_BY_CHECK] ,[ID_SENDER] ,[PROVIDER_RECEIPT] ,[RESPONSE_CODE] ,[RELATED_SALES_MIGRATION] ,[CUSTOMER_DISCOUNT_AMOUNT] ,[IsFixedFee] ,[PROVIDER_FEE] ,[TOTAL] ,[CUSTOMER_FEE] ,[PRINTRECEIPT] ,[STATUS] ,[ID_BILLPAYMENT_VIONE_SALES] ,[AMOUNT] ,[ACCOUNT_NUMBER] ,[LOYALTY_SUBMIT] ,[ID_BILLER] ,[PRECASH_HTTN] ,[IS_LOYALTY] ,[RESPONSE_MESSAGE] ,[PRECASH_AUTH_NUMBER] ,[PRECASH_SPAN] ,[DELIVERY_TIME_DESCRIPTION] ,[RELATED_TRANSACTIONID] ,[EMPLOYEE_ID] ,[AGENCY_PERCENTAGE] ,[PRECASH_TANSACTIONID] ,[ID_BRANCH_SENDER] ,[PRECASH_PAN] ,[MSRPFee] ,[ID_BRANCH] ,[LOYALTY_OLD_BALANCE] ,[PRECASH_TRANSACTIONDATE] ,[VIAMERICAS_FEE] ,[LOYALTY_SPEND] ,[ADDITIONAL_FEE] ,[LOYALTY_POINTS] FROM envio.dba.vcw_billpayment_sales) x"

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
s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/vcw_billpayment_sales/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.parquet(s3_output_path, mode="overwrite")
    