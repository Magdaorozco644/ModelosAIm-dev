
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
qryStr = f"(SELECT [RECIPIENT_DATE_INSERTED] ,[SOURCE_TELEX_RECEIVER] ,[ID_CURRENCY_SOURCE] ,[ID_MODIFICATION_REASON] ,[REC_PAYMENTTYPE] ,[BRANCH_CASHIER] ,[ID_BRANCH] ,[TOLL_FREE] ,[_ID_COUNTRY_ORI] ,[REC_SENACCTYPE] ,[ID_CURRENY] ,[ID_FLAG_RECEIVER] ,[ID_RECEIVER] ,[TELEX_RECEIVER] ,[DATE_DEPOSIT] ,[DATE_RECEIVER] ,[SOURCE_TELEX_COMPANY] ,[ID_CASHIER] ,[ORIGINAL_FEE] ,[RATE_BASE_AT_INSERT] ,[FX_RECEIVER] ,[id_basis_rec] ,[SYNC_COMPASS] ,[EXCHANGE_RECEIVER] ,[HANDLING_RECEIVER] ,[STATE_TAX] ,[BANK_RECEIVER] ,[NUMID] ,[COMMISSION_PAYEE] ,[PORC_COMISION_RECEIVER] ,[ID_FUNDS_NAME] ,[TOTAL_DIFERENCE] ,[ACC_RECEIVER] ,[CLAVE_RECEIVER] ,[DATE_CANCELATION_REQ] ,[ID_STATE_RECEIVER] ,[FOREX_GAIN] ,[REF_RECEIVER] ,[ACCULINK_TRANID] ,[REC_ACCBANK] ,[rec_fname] ,[NET_AMOUNT_RECEIVER] ,[acc_typeid] ,[CLOSING_AGENT] ,[ID_PAYMENT] ,[FOREX_ESTIMATED] ,[TELEX_COMPANY] ,[TOTAL_MODO_PAGO_COMP] ,[REBATE_AMOUNT] ,[STATUS_PAGO_PAYEE] ,[DISCOUNT] ,[FUNDING_FEE] ,[REC_ACCNUMBER] ,[ORIGINAL_RATE] ,[SOURCE_EXCHANGE_COMPANY] ,[PAYMENT_DATE] ,[ID_MAIN_BRANCH_SENT] ,[BRANCH_NAME_CASHIER] ,[COMMISSION_PAYEE_PESOS] ,[SOURCE] ,[FX_RATE_ORIGINATOR] ,[RECEIVER_DATE_AVAILABLE] ,[ID_CANCELATION_REQ] ,[SOURCE_TAXES] ,[EXPIRED_RATE] ,[ID_FUNDS_OTHER_NAME] ,[PAYER_REFERENCENO] ,[rec_mname] ,[SOURCE_TOTAL_RECEIVER] ,[typed_date] ,[COUPON_CODE] ,[PIN_NUMBER] ,[TRANS_RECEIVER] ,[ID_MAIN_BRANCH_EXPIRED] ,[PHONE2_RECEIVER] ,[ID_RECIPIENT] ,[rec_slname] ,[AGENT_COMM_PROFIT] ,[NOTES_RECEIVER] ,[REFERAL_COMISSION_PERCENTAGE] ,[BRANCH_PAY_RECEIVER] ,[DATE_EXPIRED] ,[ADDRESS_RECEIVER] ,[FOREX_FIRST_ESTIMATED] ,[id_receiver_unique] ,[SOURCE_ORIGINAL_RATE] ,[FOREX_CALC] ,[TOTAL_MODO_PAGO] ,[TOTAL_RECEIVER] ,[SOURCE_EXCHANGE_RECEIVER] ,[DATE_CANCEL] ,[ORDER_EXPIRED] ,[PHONE1_RECEIVER] ,[STATUS_PAGO_AGENT] ,[MOD_PAY_CURRENCY] ,[FX_RATE_CUSTOMER] ,[ID_CITY_RECEIVER] ,[URGENCY_RECEIVER] ,[TYPEID] ,[ORIGINATOR_BUYING_RATE] ,[rec_synch] ,[RATE_AT_INSERT] ,[ID_FUND] ,[FOREX_FIRST_ESTIMATED_RATE] ,[RECOMEND_RECEIVER] ,[_ID_STATE_ORI] ,[APPS] ,[ZIP_RECEIVER] ,[SOURCE_FEE_RATE] ,[DATE_MODIFICATION_REQ] ,[ORIGINATOR_TRANSACTION_ID] ,[rec_createacc] ,[RATE_CHANGE_RECEIVER] ,[DATE_TRANS_PAYEE] ,[ID_MODIFICATION_REQ] ,[REC_ACCROUTING] ,[FOREX_GAIN_RATE] ,[SPECIAL_COMMENT] ,[ADDITIONAL_INFORMATION] ,[CLOSING_PAYEE] ,[SOURCE_CURRENCY_AMOUNT] ,[DEST_COUNTRY_TAX] ,[type_basis_rec] ,[NAME_RECEIVER] ,[MAINTENANCE_FEE] ,[_ID_CITY_ORI] ,[rec_lname] ,[FOREX_ESTIMATED_RATE] ,[DEBIT_CARD_NUMBER] ,[email_receiver] ,[LOYALTY_RATE_COST] ,[BRANCH_PAY_RECEIVER_ORIGINAL] ,[ID_COUNTRY_RECEIVER] ,[TOTAL_PAY_RECEIVER] ,[REFERAL_COMISSION_FIXED] ,[TIME_RECEIVER] ,[FLAG_TEMP] ,[ID_AD] ,[MODE_PAY_RECEIVER] ,[FEE_RATE] ,[ID_SENDER] ,[SOURCE_FEE_AMOUNT] ,[EXCHANGE_COMPANY] FROM envio.dba.receiver) x"

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
    