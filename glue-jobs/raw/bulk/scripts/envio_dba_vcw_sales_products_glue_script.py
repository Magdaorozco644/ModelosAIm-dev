
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
qryStr = f"(SELECT [PINSALEID] ,[ID_PRODUCT] ,[ID_PRODUCT_TYPE] ,[CF_VIAMERICAS] ,[DISCOUNT_VIAMERICAS] ,[IMPRESO] ,[GAIN_BRANCH] ,[PHONENUMBER] ,[RESPONSEID] ,[EMPLOYEEID] ,[IDCURRENCY] ,[BARCODENUMBER] ,[CLOSING_AMOUNT] ,[CUSTOMER_PHONE] ,[PIN_RECEIPT] ,[ID_SENDER] ,[TotalViamericas] ,[TRAN_FEE] ,[PROMO_INFORMATION] ,[TotalProvider] ,[PINSTATUS] ,[TRANSACTIONID] ,[LOYALTY_POINTS] ,[RED_PHONE] ,[SECURITY_NUMBER] ,[IS_LOYALTY] ,[GAIN_VIAMERICAS] ,[AMOUNT] ,[LOYALTY_BALANCE] ,[RESPONSEMESSAGE] ,[FIX_BRANCH] ,[CF_CUSTOMER] ,[TOTALAMOUNT] ,[FEE_PREPAY] ,[DISCOUNT_BRANCH] ,[TAXAMOUNT] ,[EXCHANGERATE] ,[LOYALTY_SPEND] ,[ID_CARRIERS_BY_PROVIDERS] ,[SALESID] ,[IDBRANCH] ,[CONTROLNUMBERPIN] ,[FORCE] ,[ID_CUSTOMER_PHONE] ,[FIX_VIAMERICAS] ,[DATE] ,[ID_BILL_BY_SENDER] ,[LOYALTY_OLD_BALANCE] ,[PhantomPromo] ,[ID_COST_GATEWAY] ,[LOYALTY_SUBMIT] ,[LOCALAMOUNT] ,[COUNTRYAREACODE] ,[ACCESS_NUMBER] ,[CONSECUTIVETRANSACTIONDAILYPAYEE] ,[isViaOne] ,[USER_IP] ,[AGENT_COMMISSION_BY_CHECK] ,[DiscountVia] ,[isVerifiedAccount] ,[PromoAmount] ,[RelatedTransactionId] ,[DATETRANSACTION] ,[STATUSID] FROM envio.dba.vcw_sales_products) x"

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
    