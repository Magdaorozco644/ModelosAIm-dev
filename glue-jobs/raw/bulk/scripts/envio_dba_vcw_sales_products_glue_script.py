
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
qryStr = f"(SELECT [USER_IP] ,[PIN_RECEIPT] ,[LOYALTY_OLD_BALANCE] ,[DiscountVia] ,[IDBRANCH] ,[EMPLOYEEID] ,[FORCE] ,[DISCOUNT_BRANCH] ,[GAIN_BRANCH] ,[PromoAmount] ,[LOYALTY_BALANCE] ,[LOYALTY_SPEND] ,[TOTALAMOUNT] ,[IS_LOYALTY] ,[LOCALAMOUNT] ,[TAXAMOUNT] ,[TRANSACTIONID] ,[TotalProvider] ,[TRAN_FEE] ,[STATUSID] ,[RelatedTransactionId] ,[CUSTOMER_PHONE] ,[CF_CUSTOMER] ,[EXCHANGERATE] ,[BARCODENUMBER] ,[CF_VIAMERICAS] ,[CONSECUTIVETRANSACTIONDAILYPAYEE] ,[ID_COST_GATEWAY] ,[COUNTRYAREACODE] ,[isVerifiedAccount] ,[ID_CUSTOMER_PHONE] ,[isViaOne] ,[SALESID] ,[TotalViamericas] ,[PHONENUMBER] ,[FIX_VIAMERICAS] ,[IDCURRENCY] ,[IMPRESO] ,[CLOSING_AMOUNT] ,[RESPONSEID] ,[ACCESS_NUMBER] ,[PROMO_INFORMATION] ,[ID_SENDER] ,[AGENT_COMMISSION_BY_CHECK] ,[FEE_PREPAY] ,[GAIN_VIAMERICAS] ,[CONTROLNUMBERPIN] ,[RESPONSEMESSAGE] ,[LOYALTY_POINTS] ,[RED_PHONE] ,[PINSTATUS] ,[LOYALTY_SUBMIT] ,[ID_CARRIERS_BY_PROVIDERS] ,[PINSALEID] ,[AMOUNT] ,[DATE] ,[ID_PRODUCT] ,[FIX_BRANCH] ,[DISCOUNT_VIAMERICAS] ,[SECURITY_NUMBER] ,[ID_BILL_BY_SENDER] ,[PhantomPromo] ,[DATETRANSACTION] ,[ID_PRODUCT_TYPE] FROM envio.dba.vcw_sales_products) x"

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
s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/vcw_sales_products/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.parquet(s3_output_path, mode="overwrite")
    