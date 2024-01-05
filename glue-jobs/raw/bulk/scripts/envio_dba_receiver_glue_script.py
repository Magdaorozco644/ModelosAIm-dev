
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
qryStr = f"(SELECT [REBATE_AMOUNT] ,[ACCULINK_TRANID] ,[APPS] ,[ID_AD] ,[ID_MAIN_BRANCH_SENT] ,[ADDITIONAL_INFORMATION] ,[ID_MODIFICATION_REQ] ,[ID_RECEIVER] ,[acc_typeid] ,[FOREX_CALC] ,[DISCOUNT]  ,[ORIGINAL_RATE]  ,[TYPEID]  ,[ID_COUNTRY_RECEIVER] ,[EXPIRED_RATE] ,[FX_RECEIVER] ,[FOREX_ESTIMATED] ,[LOYALTY_RATE_COST] ,[ID_BRANCH] ,[SOURCE_TELEX_COMPANY]  ,[ID_MODIFICATION_REASON] ,[RATE_CHANGE_RECEIVER]  ,[MAINTENANCE_FEE] ,[ID_FUNDS_OTHER_NAME] ,[SOURCE] ,[ID_SENDER] ,[DATE_TRANS_PAYEE] ,[BRANCH_NAME_CASHIER] ,[ZIP_RECEIVER] ,[REC_SENACCTYPE] ,[AGENT_COMM_PROFIT] ,[FUNDING_FEE] ,[TOTAL_MODO_PAGO_COMP] ,[HANDLING_RECEIVER] ,[SOURCE_FEE_RATE] ,[BRANCH_CASHIER] ,[DEST_COUNTRY_TAX] ,[RECEIVER_DATE_AVAILABLE]  ,[FX_RATE_ORIGINATOR] ,[PAYER_REFERENCENO] ,[SPECIAL_COMMENT] ,[ID_MAIN_BRANCH_EXPIRED] ,[SOURCE_FEE_AMOUNT]  ,[BRANCH_PAY_RECEIVER_ORIGINAL] ,[ID_CASHIER] ,[RATE_AT_INSERT] ,[DATE_CANCEL] ,[ID_FLAG_RECEIVER] ,[ID_CURRENCY_SOURCE] ,[REF_RECEIVER] ,[FOREX_GAIN_RATE] ,[ID_RECIPIENT] ,[ORIGINATOR_BUYING_RATE] ,[MODE_PAY_RECEIVER] ,[SOURCE_TAXES] ,[TELEX_RECEIVER] ,[FOREX_FIRST_ESTIMATED] ,[RATE_BASE_AT_INSERT] ,[STATE_TAX]  ,[SOURCE_CURRENCY_AMOUNT]  ,[NET_AMOUNT_RECEIVER] ,[BRANCH_PAY_RECEIVER] ,[REFERAL_COMISSION_FIXED] ,[FEE_RATE] ,[TOTAL_PAY_RECEIVER] ,[SOURCE_ORIGINAL_RATE] ,[SOURCE_EXCHANGE_RECEIVER] ,[RECIPIENT_DATE_INSERTED]  ,[COUPON_CODE] ,[ID_FUNDS_NAME] ,[DATE_CANCELATION_REQ]  ,[TELEX_COMPANY] ,[TOTAL_MODO_PAGO] ,[EXCHANGE_RECEIVER] ,[DATE_EXPIRED] ,[ORIGINAL_FEE] ,[ORIGINATOR_TRANSACTION_ID] ,[EXCHANGE_COMPANY] ,[REFERAL_COMISSION_PERCENTAGE] ,[DEBIT_CARD_NUMBER]  ,[BANK_RECEIVER]  ,[PORC_COMISION_RECEIVER] ,[TOTAL_DIFERENCE] ,[FX_RATE_CUSTOMER] ,[SOURCE_TELEX_RECEIVER] ,[ID_STATE_RECEIVER] ,[ID_PAYMENT]  ,[FOREX_ESTIMATED_RATE] ,[MOD_PAY_CURRENCY] ,[SOURCE_EXCHANGE_COMPANY] ,[DATE_RECEIVER] ,[ID_CANCELATION_REQ] ,[FOREX_GAIN] ,[typed_date] ,[COMMISSION_PAYEE] ,[FOREX_FIRST_ESTIMATED_RATE]  ,[ORDER_EXPIRED] ,[DATE_MODIFICATION_REQ] ,[ID_FUND] ,[SOURCE_TOTAL_RECEIVER] ,[PAYMENT_DATE] ,[ID_CITY_RECEIVER] ,[ID_CURRENY] ,[TOTAL_RECEIVER] FROM envio.dba.receiver) x"

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
s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/receiver/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.parquet(s3_output_path, mode="overwrite")
    