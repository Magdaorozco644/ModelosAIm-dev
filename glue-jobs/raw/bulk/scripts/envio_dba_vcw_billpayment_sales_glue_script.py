
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
qryStr = f"(SELECT [LOYALTY_SUBMIT] ,[TRANSACTION_DATE] ,[LOYALTY_SPEND] ,[PRECASH_TRANSACTIONDATE] ,[AMOUNT] ,[PRECASH_AUTH_NUMBER] ,[RESPONSE_CODE] ,[VIAMERICAS_FEE] ,[ID_BILLER] ,[PRECASH_SPAN] ,[AGENCY_COMMISSION_BY_CHECK] ,[PROVIDER_MESSAGE_LIST] ,[PRECASH_HTTN] ,[STATUS] ,[AGENCY_PERCENTAGE] ,[RELATED_TRANSACTIONID] ,[ACCOUNT_NUMBER] ,[IsFixedFee] ,[PRINTRECEIPT] ,[PROVIDER_RECEIPT] ,[RELATED_SALES_MIGRATION] ,[PRECASH_RHTTN] ,[PRECASH_PAN] ,[LOYALTY_POINTS] ,[CUSTOMER_DISCOUNT_AMOUNT] ,[ADDITIONAL_COMISSION] ,[ID_BILLPAYMENT_VIONE_SALES] ,[ID_BRANCH] ,[LOYALTY_BALANCE] ,[PROVIDER_FEE] ,[RESPONSE_MESSAGE] ,[ID_BILLPAYMENT_SALES] ,[ADDITIONAL_FEE] ,[PRECASH_TANSACTIONID] ,[EMPLOYEE_ID] ,[LOYALTY_OLD_BALANCE] ,[TOTAL] ,[FIXDEBIT] ,[DELIVERY_TIME_DESCRIPTION] ,[ID_BRANCH_SENDER] ,[ID_SENDER] ,[CUSTOMER_FEE] ,[MSRPFee] ,[IS_LOYALTY] FROM envio.dba.vcw_billpayment_sales) x"

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
    