
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
qryStr = f"(SELECT [PAYER_REQUIRE_ID_TO_PICKUP] ,[BALANCE_GROUP] ,[FIN_MONTHLYCLOSING] ,[GROUP_TYPE] ,[FIN_YEARLYINTERESTRATE] ,[PAYER_MOD_CHARGE] ,[FIN_ACHACCTYPEID_COM] ,[FTPFILEVERSION] ,[FIN_AMTTOBERECONCILED] ,[TIME_TRANS] ,[PAYER_MODIFICATION_RESPONSE_IN_MINUTES] ,[PAYER_LIMIT_PER_MONTH] ,[BALANCE_MATCH] ,[ID_PHASE] ,[ID_CASHIER_MODDEBITLIMIT] ,[PASTDUE_INACTIVATION] ,[CREDITO_GROUP] ,[SYNC_COMPASS] ,[PAYER_LIMIT_PER_DAY_NOTES] ,[FIN_VIACHECK_VALFEE] ,[DEBT_LIMIT_MO] ,[PAYER_CANCELATION_RESPONSE_IN_MINUTES] ,[ID_CURRENCY_SOURCE] ,[NEXT_EXCHANGE_VALID_TO] ,[FIN_COLLECTMETHOD] ,[PAYER_ORDER_AVAILABILITY_IN_MINUTES_WKNDS] ,[PAYER_TYPE_CLOSING_BALANCE] ,[PAYER_REQUIRES_FOLIO_TO_PICKUP] ,[CONS_LAST_TRANS] ,[PAYER_NAT_CLOSING] ,[IS_BANK] ,[DEBT_LIMIT_WKND_MO] ,[MODE_TRANS_GROUP] ,[PAYER_LIMIT_PER_DAY] ,[DEBT_LIMIT_WKND] ,[FLAG_GRUPO] ,[PAYER_WEB_INTERFASE] ,[PAYER_TIME_TO_SEND_PAYMENT_RECEIPT] ,[PAYER_LIMIT_PER_YEAR_NOTES] ,[PAYER_MOD_INTERFASE] ,[FIN_VIACHECK_PERGR3000] ,[PAYER_ALLOW_MOD] ,[DEBIT_GROUP] ,[FIN_VIACHECK_COMXCHKTRSRY] ,[PAYER_ORDER_AVAILABILITY_IN_MINUTES] ,[GRO_COMPANYIDFOLIO] ,[FLAG_COM_CHAIN] ,[NEXT_EXCHANGE] ,[FIN_RCC_ALLOWED] ,[DIR_INPUT] ,[EMAIL_OPERATIONS] ,[FIN_ACHACCNUMBER] ,[FIN_AMTTOBERECONCILEDNOTES] ,[FIN_VIACHECK_COMXCHKGR3000] ,[FIN_DEPIDBANCO] ,[USES_FOLIO] ,[ALLOW_BEN_CHANGES] ,[FIN_STATUSCOMMENTS] ,[PAYER_WEEKEND_HOURS] ,[ID_BRANCH_DEPOSITS] ,[PAYER_MAX_AMOUNT] ,[PAYER_IDENTIFIES_ORDER_BY] ,[FIN_CAT_CREDIT] ,[ID_MAIN_BRANCH] ,[PRIORIDAD_GROUP_BRANCH] ,[ALLOW_DEP_FAX_NOT] ,[PAYER_MANUAL_PAYMENTS] ,[EMAIL_NOTIFICATIONS_TYPE] ,[FIN_CAT_CARTERA] ,[PAYER_CAN_INTERFASE] ,[AllowInstantMod] ,[DAILY_STATEMENT_TIME] ,[FIN_COMMISSIONMETHOD] ,[FIN_DEPDESCRIPTION] ,[STATE_GROUP_BRANCH] ,[PAYER_ID_REQUIREMENTS] ,[FAX_NOTIFICATIONS] ,[PAYER_ORDER_EXPIRATION_IN_DAYS] ,[ALTERNATIVE_COUNT] ,[ID_COUNTRY_SOURCE] ,[ALTERNATIVE_TOP] ,[FIN_ACHACCROUTING_COM] ,[PAYER_MAX_AMOUNT_NOTES] ,[DEBITO_GROUP] ,[TOP_CREDIT_WKND] ,[FIN_ACHACCNUMBER_COM] ,[FIN_SCHEDULE] ,[MO_DAILY_LIMIT] ,[CREDIT_GROUP] ,[BRANCHES] ,[FIN_CLOSINGDAYCODE] ,[FIN_ACHACCTYPEID] ,[NEXT_EXCHANGE_VALID_FROM] ,[FIN_CAT_VIACHECK] ,[FIN_DEPIDCUENTABANCO] ,[PAYER_CAN_CHARGE] ,[REVIEW_CREDIT_TOP_ON] ,[PAYER_CALLCENTER_BUSINESS_HOURS] ,[PAYER_TYPE_FUNDING] ,[ID_PAYER_COMMISSION_MODE] ,[MO_SEASON_LIMIT] ,[PAYER_CANCELATION_RESPONSE_IN_MINUTES_WKNDS] ,[FIN_VIACHECK_COMXCHK] ,[FIN_STATUS] ,[PAYER_LIMIT_PER_MONTH_NOTES] ,[FIN_DEPDEPOSITCODE] ,[PAYER_NOTIFIES_BENEFICIARY] ,[DIR_OUTPUT] ,[DATE_CREA_MAIN_BRANCH] ,[EMAIL_GROUP] ,[PAYER_DEPOSITS_VERIFY_ACC_HOLDER] ,[NAME_MAIN_BRANCH] ,[PAYER_CALLCENTER_WEEKEND_HOURS] ,[FIN_COLLECTMETHOD_2] ,[TOP_CREDIT] ,[TYPE_GROUP] ,[EMAIL_NOTIFICATIONS] ,[PAYER_MODIFICATION_RESPONSE_IN_MINUTES_WKNDS] ,[FIN_AJUSTEDDSO] ,[MODEM_GROUP] ,[DEBT_LIMIT] ,[PAYER_MIN_AMOUNT] ,[CHECK_RATE_ONLINE] ,[FIN_VIACHECK_PERXTRSRY] ,[PAYER_BUSINESS_HOURS] ,[PAYER_NETWORK_PAYMENT] ,[DIFERENCE_EXCHANGE] ,[PAYER_LIMIT_PER_YEAR] ,[TRANSMITION_STATUS] ,[FIN_ACHACCROUTING] ,[FIN_DSO] ,[FIN_VICHECK_NEXT_DATE_FEES] ,[FIN_VIACHECK_PERXCHK] ,[PAYER_TYPE_CLOSING] ,[PAYER_PICKUP_INSTRUCTIONS] FROM envio.dba.group_branch) x"

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
    