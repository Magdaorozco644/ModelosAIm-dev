
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
qryStr = f"(SELECT [LOYALTY_SUBMIT] ,[SALESID] ,[FORCE] ,[ID_CARRIERS_BY_PROVIDERS] ,[LOYALTY_SPEND] ,[IDCURRENCY] ,[ID_PRODUCT_TYPE] ,[TOTALAMOUNT] ,[CLOSING_AMOUNT] ,[AMOUNT] ,[EMPLOYEEID] ,[CF_CUSTOMER] ,[FIX_BRANCH] ,[PINSALEID] ,[ID_PRODUCT] ,[DATE] ,[PINSTATUS] ,[PHONENUMBER] ,[GAIN_VIAMERICAS] ,[GAIN_BRANCH] ,[FEE_PREPAY] ,[isViaOne] ,[CONSECUTIVETRANSACTIONDAILYPAYEE] ,[ID_COST_GATEWAY] ,[RelatedTransactionId] ,[RED_PHONE] ,[USER_IP] ,[IMPRESO] ,[BARCODENUMBER] ,[RESPONSEMESSAGE] ,[PromoAmount] ,[CUSTOMER_PHONE] ,[STATUSID] ,[PROMO_INFORMATION] ,[DISCOUNT_BRANCH] ,[RESPONSEID] ,[LOYALTY_POINTS] ,[TRAN_FEE] ,[PhantomPromo] ,[TotalViamericas] ,[ID_BILL_BY_SENDER] ,[DISCOUNT_VIAMERICAS] ,[LOYALTY_BALANCE] ,[IDBRANCH] ,[isVerifiedAccount] ,[AGENT_COMMISSION_BY_CHECK] ,[DATETRANSACTION] ,[ACCESS_NUMBER] ,[TAXAMOUNT] ,[FIX_VIAMERICAS] ,[DiscountVia] ,[LOYALTY_OLD_BALANCE] ,[TotalProvider] ,[EXCHANGERATE] ,[PIN_RECEIPT] ,[SECURITY_NUMBER] ,[TRANSACTIONID] ,[LOCALAMOUNT] ,[ID_CUSTOMER_PHONE] ,[ID_SENDER] ,[COUNTRYAREACODE] ,[CONTROLNUMBERPIN] ,[CF_VIAMERICAS] ,[IS_LOYALTY] FROM envio.dba.vcw_sales_products) x"

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
    