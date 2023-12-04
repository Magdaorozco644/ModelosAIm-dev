
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
qryStr = f"(SELECT [PAYER_ORDER_EXPIRATION_IN_DAYS] ,[FIN_COLLECTMETHOD_2] ,[MO_DAILY_LIMIT] ,[PAYER_IDENTIFIES_ORDER_BY] ,[GRO_COMPANYIDFOLIO] ,[DIR_INPUT] ,[PAYER_NAT_CLOSING] ,[CREDIT_GROUP] ,[FIN_ACHACCROUTING_COM] ,[PAYER_CANCELATION_RESPONSE_IN_MINUTES] ,[PAYER_CAN_INTERFASE] ,[FIN_STATUSCOMMENTS] ,[FIN_VIACHECK_COMXCHKGR3000] ,[FIN_AMTTOBERECONCILEDNOTES] ,[FIN_CAT_CARTERA] ,[PAYER_TYPE_FUNDING] ,[PAYER_CANCELATION_RESPONSE_IN_MINUTES_WKNDS] ,[PAYER_LIMIT_PER_DAY] ,[NAME_MAIN_BRANCH] ,[PAYER_NOTIFIES_BENEFICIARY] ,[MODE_TRANS_GROUP] ,[SYNC_COMPASS] ,[PAYER_REQUIRE_ID_TO_PICKUP] ,[MO_SEASON_LIMIT] ,[MODEM_GROUP] ,[TOP_CREDIT] ,[DEBT_LIMIT_MO] ,[FIN_CAT_VIACHECK] ,[FTPFILEVERSION] ,[PAYER_ORDER_AVAILABILITY_IN_MINUTES_WKNDS] ,[TOP_CREDIT_WKND] ,[FIN_VICHECK_NEXT_DATE_FEES] ,[PAYER_CALLCENTER_BUSINESS_HOURS] ,[FIN_COLLECTMETHOD] ,[DAILY_STATEMENT_TIME] ,[FIN_MONTHLYCLOSING] ,[PAYER_LIMIT_PER_YEAR] ,[FIN_CLOSINGDAYCODE] ,[FIN_DEPDEPOSITCODE] ,[PAYER_ORDER_AVAILABILITY_IN_MINUTES] ,[ALLOW_BEN_CHANGES] ,[DEBITO_GROUP] ,[PAYER_PICKUP_INSTRUCTIONS] ,[NEXT_EXCHANGE_VALID_FROM] ,[ID_MAIN_BRANCH] ,[BRANCHES] ,[EMAIL_OPERATIONS] ,[TYPE_GROUP] ,[IS_BANK] ,[PAYER_LIMIT_PER_DAY_NOTES] ,[PAYER_TYPE_CLOSING_BALANCE] ,[PAYER_BUSINESS_HOURS] ,[DIR_OUTPUT] ,[FIN_DSO] ,[FIN_VIACHECK_VALFEE] ,[DATE_CREA_MAIN_BRANCH] ,[NEXT_EXCHANGE] ,[FIN_ACHACCTYPEID] ,[FIN_ACHACCTYPEID_COM] ,[PAYER_LIMIT_PER_MONTH_NOTES] ,[BALANCE_MATCH] ,[FIN_AMTTOBERECONCILED] ,[FIN_DEPDESCRIPTION] ,[FIN_CAT_CREDIT] ,[FIN_STATUS] ,[PAYER_ID_REQUIREMENTS] ,[ID_CURRENCY_SOURCE] ,[PAYER_MIN_AMOUNT] ,[PAYER_MODIFICATION_RESPONSE_IN_MINUTES_WKNDS] ,[FIN_AJUSTEDDSO] ,[PAYER_MOD_INTERFASE] ,[FIN_ACHACCNUMBER] ,[PAYER_WEEKEND_HOURS] ,[PAYER_ALLOW_MOD] ,[FIN_YEARLYINTERESTRATE] ,[FIN_RCC_ALLOWED] ,[NEXT_EXCHANGE_VALID_TO] ,[PAYER_MANUAL_PAYMENTS] ,[PAYER_TYPE_CLOSING] ,[FIN_ACHACCROUTING] ,[PAYER_DEPOSITS_VERIFY_ACC_HOLDER] ,[AllowInstantMod] ,[FIN_VIACHECK_COMXCHK] ,[REVIEW_CREDIT_TOP_ON] ,[PRIORIDAD_GROUP_BRANCH] ,[FAX_NOTIFICATIONS] ,[PAYER_TIME_TO_SEND_PAYMENT_RECEIPT] ,[PASTDUE_INACTIVATION] ,[ID_PHASE] ,[FIN_VIACHECK_PERGR3000] ,[ID_CASHIER_MODDEBITLIMIT] ,[BALANCE_GROUP] ,[PAYER_MAX_AMOUNT] ,[FLAG_COM_CHAIN] ,[DEBIT_GROUP] ,[FIN_VIACHECK_PERXTRSRY] ,[DEBT_LIMIT_WKND_MO] ,[TIME_TRANS] ,[FLAG_GRUPO] ,[PAYER_CALLCENTER_WEEKEND_HOURS] ,[PAYER_NETWORK_PAYMENT] ,[PAYER_CAN_CHARGE] ,[CHECK_RATE_ONLINE] ,[PAYER_LIMIT_PER_YEAR_NOTES] ,[CREDITO_GROUP] ,[FIN_DEPIDCUENTABANCO] ,[EMAIL_GROUP] ,[ALLOW_DEP_FAX_NOT] ,[STATE_GROUP_BRANCH] ,[PAYER_MAX_AMOUNT_NOTES] ,[EMAIL_NOTIFICATIONS] ,[TRANSMITION_STATUS] ,[PAYER_WEB_INTERFASE] ,[EMAIL_NOTIFICATIONS_TYPE] ,[FIN_SCHEDULE] ,[DEBT_LIMIT_WKND] ,[PAYER_REQUIRES_FOLIO_TO_PICKUP] ,[PAYER_LIMIT_PER_MONTH] ,[ID_PAYER_COMMISSION_MODE] ,[DIFERENCE_EXCHANGE] ,[FIN_VIACHECK_COMXCHKTRSRY] ,[ALTERNATIVE_TOP] ,[CONS_LAST_TRANS] ,[PAYER_MOD_CHARGE] ,[USES_FOLIO] ,[GROUP_TYPE] ,[ID_COUNTRY_SOURCE] ,[FIN_COMMISSIONMETHOD] ,[ID_BRANCH_DEPOSITS] ,[FIN_ACHACCNUMBER_COM] ,[FIN_DEPIDBANCO] ,[DEBT_LIMIT] ,[FIN_VIACHECK_PERXCHK] ,[ALTERNATIVE_COUNT] ,[PAYER_MODIFICATION_RESPONSE_IN_MINUTES] FROM envio.dba.group_branch) x"

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
    