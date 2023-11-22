
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
qryStr = f"(SELECT [ID_MAIN_BRANCH_SENT] ,[HANDLING_RECEIVER] ,[BRANCH_NAME_CASHIER] ,[ORIGINATOR_TRANSACTION_ID] ,[rec_synch] ,[ID_FUNDS_NAME] ,[TIME_RECEIVER] ,[APPS] ,[SOURCE_EXCHANGE_COMPANY] ,[LOYALTY_RATE_COST] ,[FOREX_GAIN] ,[REF_RECEIVER] ,[TOTAL_MODO_PAGO] ,[SOURCE_CURRENCY_AMOUNT] ,[ACC_RECEIVER] ,[ID_CITY_RECEIVER] ,[DATE_CANCELATION_REQ] ,[ID_CURRENY] ,[BRANCH_PAY_RECEIVER_ORIGINAL] ,[ID_MODIFICATION_REASON] ,[FLAG_TEMP] ,[rec_mname] ,[SYNC_COMPASS] ,[_ID_STATE_ORI] ,[TYPEID] ,[NAME_RECEIVER] ,[EXCHANGE_RECEIVER] ,[RECOMEND_RECEIVER] ,[TRANS_RECEIVER] ,[REC_PAYMENTTYPE] ,[ID_CASHIER] ,[DEST_COUNTRY_TAX] ,[CLAVE_RECEIVER] ,[id_receiver_unique] ,[ID_STATE_RECEIVER] ,[FOREX_FIRST_ESTIMATED] ,[AGENT_COMM_PROFIT] ,[REC_SENACCTYPE] ,[ACCULINK_TRANID] ,[ID_CURRENCY_SOURCE] ,[DEBIT_CARD_NUMBER] ,[FUNDING_FEE] ,[SOURCE_TAXES] ,[ORIGINATOR_BUYING_RATE] ,[_ID_COUNTRY_ORI] ,[RATE_CHANGE_RECEIVER] ,[acc_typeid] ,[DATE_EXPIRED] ,[MODE_PAY_RECEIVER] ,[RATE_AT_INSERT] ,[PHONE2_RECEIVER] ,[rec_createacc] ,[STATE_TAX] ,[EXCHANGE_COMPANY] ,[email_receiver] ,[ID_BRANCH] ,[PHONE1_RECEIVER] ,[ID_PAYMENT] ,[COUPON_CODE] ,[ID_COUNTRY_RECEIVER] ,[SOURCE] ,[id_basis_rec] ,[FOREX_ESTIMATED_RATE] ,[TELEX_COMPANY] ,[ADDRESS_RECEIVER] ,[RECIPIENT_DATE_INSERTED] ,[DISCOUNT] ,[DATE_MODIFICATION_REQ] ,[ID_CANCELATION_REQ] ,[FX_RATE_ORIGINATOR] ,[COMMISSION_PAYEE_PESOS] ,[STATUS_PAGO_AGENT] ,[rec_slname] ,[MAINTENANCE_FEE] ,[ZIP_RECEIVER] ,[TOTAL_PAY_RECEIVER] ,[ORIGINAL_FEE] ,[ID_AD] ,[PAYMENT_DATE] ,[REC_ACCBANK] ,[RECEIVER_DATE_AVAILABLE] ,[REFERAL_COMISSION_FIXED] ,[TELEX_RECEIVER] ,[DATE_DEPOSIT] ,[ID_SENDER] ,[ORIGINAL_RATE] ,[FX_RECEIVER] ,[PORC_COMISION_RECEIVER] ,[CLOSING_AGENT] ,[rec_lname] ,[PIN_NUMBER] ,[ID_FLAG_RECEIVER] ,[FEE_RATE] ,[FOREX_CALC] ,[ID_MODIFICATION_REQ] ,[DATE_TRANS_PAYEE] ,[EXPIRED_RATE] ,[_ID_CITY_ORI] ,[BANK_RECEIVER] ,[type_basis_rec] ,[NUMID] ,[PAYER_REFERENCENO] ,[FX_RATE_CUSTOMER] ,[CLOSING_PAYEE] ,[REC_ACCNUMBER] ,[ID_MAIN_BRANCH_EXPIRED] ,[rec_fname] ,[SOURCE_TELEX_RECEIVER] ,[STATUS_PAGO_PAYEE] ,[BRANCH_PAY_RECEIVER] ,[SPECIAL_COMMENT] ,[SOURCE_EXCHANGE_RECEIVER] ,[TOTAL_MODO_PAGO_COMP] ,[typed_date] ,[NOTES_RECEIVER] ,[ORDER_EXPIRED] ,[SOURCE_ORIGINAL_RATE] ,[ID_RECEIVER] ,[REC_ACCROUTING] ,[FOREX_ESTIMATED] ,[DATE_CANCEL] ,[SOURCE_FEE_RATE] ,[REBATE_AMOUNT] ,[TOTAL_DIFERENCE] ,[SOURCE_TELEX_COMPANY] ,[ID_RECIPIENT] ,[NET_AMOUNT_RECEIVER] ,[ID_FUND] ,[RATE_BASE_AT_INSERT] ,[BRANCH_CASHIER] ,[TOTAL_RECEIVER] ,[FOREX_FIRST_ESTIMATED_RATE] ,[ID_FUNDS_OTHER_NAME] ,[TOLL_FREE] ,[SOURCE_TOTAL_RECEIVER] ,[REFERAL_COMISSION_PERCENTAGE] ,[SOURCE_FEE_AMOUNT] ,[COMMISSION_PAYEE] ,[DATE_RECEIVER] ,[ADDITIONAL_INFORMATION] ,[MOD_PAY_CURRENCY] ,[URGENCY_RECEIVER] ,[FOREX_GAIN_RATE] FROM envio.dba.receiver) x"

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
    