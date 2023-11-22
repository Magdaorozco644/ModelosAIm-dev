
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
qryStr = f"(SELECT [PAYER_DEPOSITS_VERIFY_ACC_HOLDER] ,[NEXT_EXCHANGE] ,[PAYER_MAX_AMOUNT_NOTES] ,[FIN_ACHACCROUTING] ,[PAYER_PICKUP_INSTRUCTIONS] ,[PAYER_CAN_INTERFASE] ,[FIN_VIACHECK_COMXCHK] ,[TIME_TRANS] ,[FIN_ACHACCTYPEID_COM] ,[DEBT_LIMIT] ,[AllowInstantMod] ,[CHECK_RATE_ONLINE] ,[PAYER_NAT_CLOSING] ,[PAYER_CALLCENTER_WEEKEND_HOURS] ,[FTPFILEVERSION] ,[FIN_DEPIDBANCO] ,[PAYER_CANCELATION_RESPONSE_IN_MINUTES] ,[FIN_MONTHLYCLOSING] ,[PAYER_ALLOW_MOD] ,[FIN_COLLECTMETHOD_2] ,[FIN_SCHEDULE] ,[MO_SEASON_LIMIT] ,[PAYER_ORDER_AVAILABILITY_IN_MINUTES_WKNDS] ,[ALLOW_BEN_CHANGES] ,[FIN_CAT_CARTERA] ,[PAYER_LIMIT_PER_DAY] ,[MO_DAILY_LIMIT] ,[FIN_STATUS] ,[NAME_MAIN_BRANCH] ,[SYNC_COMPASS] ,[CREDIT_GROUP] ,[PAYER_LIMIT_PER_MONTH] ,[GROUP_TYPE] ,[PAYER_LIMIT_PER_YEAR] ,[NEXT_EXCHANGE_VALID_FROM] ,[FIN_VIACHECK_PERGR3000] ,[NEXT_EXCHANGE_VALID_TO] ,[PAYER_WEEKEND_HOURS] ,[FIN_VICHECK_NEXT_DATE_FEES] ,[TOP_CREDIT_WKND] ,[DEBT_LIMIT_MO] ,[FIN_COMMISSIONMETHOD] ,[FIN_RCC_ALLOWED] ,[EMAIL_NOTIFICATIONS] ,[TRANSMITION_STATUS] ,[FLAG_COM_CHAIN] ,[ID_CURRENCY_SOURCE] ,[DEBITO_GROUP] ,[PAYER_MANUAL_PAYMENTS] ,[PAYER_WEB_INTERFASE] ,[FIN_CAT_CREDIT] ,[IS_BANK] ,[PAYER_MOD_CHARGE] ,[EMAIL_OPERATIONS] ,[MODEM_GROUP] ,[FAX_NOTIFICATIONS] ,[PAYER_MAX_AMOUNT] ,[PAYER_REQUIRES_FOLIO_TO_PICKUP] ,[PAYER_NOTIFIES_BENEFICIARY] ,[FIN_STATUSCOMMENTS] ,[FIN_COLLECTMETHOD] ,[ALTERNATIVE_COUNT] ,[FIN_DSO] ,[FIN_AMTTOBERECONCILEDNOTES] ,[FLAG_GRUPO] ,[DEBT_LIMIT_WKND] ,[DIFERENCE_EXCHANGE] ,[PAYER_CALLCENTER_BUSINESS_HOURS] ,[DATE_CREA_MAIN_BRANCH] ,[PAYER_NETWORK_PAYMENT] ,[DEBIT_GROUP] ,[PAYER_TIME_TO_SEND_PAYMENT_RECEIPT] ,[PAYER_TYPE_CLOSING_BALANCE] ,[CREDITO_GROUP] ,[CONS_LAST_TRANS] ,[DEBT_LIMIT_WKND_MO] ,[ALLOW_DEP_FAX_NOT] ,[FIN_CLOSINGDAYCODE] ,[PAYER_BUSINESS_HOURS] ,[ID_MAIN_BRANCH] ,[ID_PHASE] ,[MODE_TRANS_GROUP] ,[PASTDUE_INACTIVATION] ,[PAYER_ID_REQUIREMENTS] ,[FIN_ACHACCNUMBER_COM] ,[REVIEW_CREDIT_TOP_ON] ,[TYPE_GROUP] ,[PAYER_ORDER_EXPIRATION_IN_DAYS] ,[PAYER_MODIFICATION_RESPONSE_IN_MINUTES] ,[FIN_AJUSTEDDSO] ,[FIN_ACHACCTYPEID] ,[PAYER_REQUIRE_ID_TO_PICKUP] ,[PRIORIDAD_GROUP_BRANCH] ,[FIN_VIACHECK_PERXTRSRY] ,[TOP_CREDIT] ,[BALANCE_GROUP] ,[FIN_AMTTOBERECONCILED] ,[FIN_CAT_VIACHECK] ,[ID_CASHIER_MODDEBITLIMIT] ,[FIN_DEPDEPOSITCODE] ,[USES_FOLIO] ,[DIR_INPUT] ,[DIR_OUTPUT] ,[PAYER_LIMIT_PER_YEAR_NOTES] ,[PAYER_LIMIT_PER_MONTH_NOTES] ,[ALTERNATIVE_TOP] ,[BRANCHES] ,[FIN_VIACHECK_COMXCHKGR3000] ,[ID_COUNTRY_SOURCE] ,[FIN_VIACHECK_VALFEE] ,[PAYER_CANCELATION_RESPONSE_IN_MINUTES_WKNDS] ,[PAYER_TYPE_CLOSING] ,[STATE_GROUP_BRANCH] ,[EMAIL_GROUP] ,[PAYER_IDENTIFIES_ORDER_BY] ,[PAYER_TYPE_FUNDING] ,[PAYER_CAN_CHARGE] ,[PAYER_LIMIT_PER_DAY_NOTES] ,[FIN_VIACHECK_COMXCHKTRSRY] ,[FIN_ACHACCNUMBER] ,[FIN_DEPDESCRIPTION] ,[PAYER_ORDER_AVAILABILITY_IN_MINUTES] ,[EMAIL_NOTIFICATIONS_TYPE] ,[FIN_ACHACCROUTING_COM] ,[FIN_VIACHECK_PERXCHK] ,[ID_PAYER_COMMISSION_MODE] ,[PAYER_MOD_INTERFASE] ,[FIN_DEPIDCUENTABANCO] ,[FIN_YEARLYINTERESTRATE] ,[PAYER_MODIFICATION_RESPONSE_IN_MINUTES_WKNDS] ,[BALANCE_MATCH] ,[GRO_COMPANYIDFOLIO] ,[ID_BRANCH_DEPOSITS] ,[DAILY_STATEMENT_TIME] ,[PAYER_MIN_AMOUNT] FROM envio.dba.group_branch) x"

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
s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/group_branch/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.parquet(s3_output_path, mode="overwrite")
    