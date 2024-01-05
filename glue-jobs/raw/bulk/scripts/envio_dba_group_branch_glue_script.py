
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
qryStr = f"(SELECT [FLAG_COM_CHAIN] ,[FIN_VIACHECK_COMXCHK] ,[ALTERNATIVE_COUNT] ,[PAYER_CANCELATION_RESPONSE_IN_MINUTES] ,[DEBT_LIMIT] ,[PAYER_WEEKEND_HOURS] ,[DEBT_LIMIT_MO] ,[BALANCE_MATCH] ,[ID_CURRENCY_SOURCE] ,[ALLOW_DEP_FAX_NOT] ,[FIN_VIACHECK_PERXTRSRY] ,[FIN_ACHACCNUMBER] ,[DEBT_LIMIT_WKND_MO] ,[ID_PHASE] ,[PAYER_ORDER_AVAILABILITY_IN_MINUTES_WKNDS] ,[ALLOW_BEN_CHANGES] ,[FIN_VIACHECK_COMXCHKGR3000] ,[TYPE_GROUP] ,[FIN_YEARLYINTERESTRATE] ,[BRANCHES] ,[GRO_COMPANYIDFOLIO] ,[FIN_COLLECTMETHOD] ,[IS_BANK] ,[PAYER_TYPE_CLOSING] ,[FIN_COLLECTMETHOD_2] ,[FLAG_GRUPO] ,[FIN_DEPIDCUENTABANCO] ,[NEXT_EXCHANGE] ,[AllowInstantMod] ,[DEBT_LIMIT_WKND] ,[FIN_DSO] ,[FIN_ACHACCTYPEID] ,[SYNC_COMPASS] ,[PAYER_LIMIT_PER_DAY] ,[FIN_VIACHECK_VALFEE] ,[PAYER_MAX_AMOUNT] ,[PAYER_CAN_CHARGE] ,[GROUP_TYPE] ,[FIN_VIACHECK_COMXCHKTRSRY] ,[EMAIL_GROUP] ,[TOP_CREDIT_WKND] ,[PAYER_ORDER_EXPIRATION_IN_DAYS] ,[DAILY_STATEMENT_TIME] ,[PAYER_LIMIT_PER_YEAR_NOTES] ,[PAYER_ID_REQUIREMENTS] ,[PAYER_MOD_INTERFASE] ,[PAYER_NOTIFIES_BENEFICIARY] ,[PAYER_TYPE_FUNDING] ,[TIME_TRANS] ,[PAYER_MOD_CHARGE] ,[PAYER_REQUIRE_ID_TO_PICKUP] ,[NEXT_EXCHANGE_VALID_TO] ,[PAYER_TYPE_CLOSING_BALANCE] ,[PAYER_MANUAL_PAYMENTS] ,[PAYER_WEB_INTERFASE] ,[FAX_NOTIFICATIONS] ,[FIN_AJUSTEDDSO] ,[NAME_MAIN_BRANCH] ,[FIN_CAT_CREDIT] ,[EMAIL_OPERATIONS] ,[FIN_RCC_ALLOWED] ,[PAYER_MIN_AMOUNT] ,[ID_BRANCH_DEPOSITS] ,[PAYER_LIMIT_PER_YEAR] ,[ID_CASHIER_MODDEBITLIMIT] ,[FIN_DEPDEPOSITCODE] ,[PAYER_TIME_TO_SEND_PAYMENT_RECEIPT] ,[DEBITO_GROUP] ,[FIN_CLOSINGDAYCODE] ,[PASTDUE_INACTIVATION] ,[PAYER_MODIFICATION_RESPONSE_IN_MINUTES] ,[MO_DAILY_LIMIT] ,[PAYER_MAX_AMOUNT_NOTES] ,[BALANCE_GROUP] ,[PAYER_ORDER_AVAILABILITY_IN_MINUTES] ,[USES_FOLIO] ,[FIN_CAT_VIACHECK] ,[FIN_AMTTOBERECONCILEDNOTES] ,[FIN_ACHACCROUTING] ,[FIN_STATUSCOMMENTS] ,[ID_COUNTRY_SOURCE] ,[PAYER_DEPOSITS_VERIFY_ACC_HOLDER] ,[FIN_COMMISSIONMETHOD] ,[FIN_VIACHECK_PERXCHK] ,[PAYER_NAT_CLOSING] ,[FIN_ACHACCTYPEID_COM] ,[TOP_CREDIT] ,[FIN_DEPIDBANCO] ,[EMAIL_NOTIFICATIONS_TYPE] ,[ALTERNATIVE_TOP] ,[PAYER_PICKUP_INSTRUCTIONS] ,[MODE_TRANS_GROUP] ,[PAYER_LIMIT_PER_MONTH] ,[DIR_INPUT] ,[FIN_MONTHLYCLOSING] ,[PAYER_CALLCENTER_WEEKEND_HOURS] ,[CREDIT_GROUP] ,[NEXT_EXCHANGE_VALID_FROM] ,[PAYER_REQUIRES_FOLIO_TO_PICKUP] ,[PAYER_CALLCENTER_BUSINESS_HOURS] ,[PAYER_NETWORK_PAYMENT] ,[PAYER_CAN_INTERFASE] ,[MODEM_GROUP] ,[PAYER_MODIFICATION_RESPONSE_IN_MINUTES_WKNDS] ,[PAYER_CANCELATION_RESPONSE_IN_MINUTES_WKNDS] ,[EMAIL_NOTIFICATIONS] ,[PAYER_BUSINESS_HOURS] ,[FIN_STATUS] ,[FIN_SCHEDULE] ,[CREDITO_GROUP] ,[REVIEW_CREDIT_TOP_ON] ,[FIN_DEPDESCRIPTION] ,[PAYER_IDENTIFIES_ORDER_BY] ,[FIN_ACHACCROUTING_COM] ,[DATE_CREA_MAIN_BRANCH] ,[PRIORIDAD_GROUP_BRANCH] ,[PAYER_LIMIT_PER_MONTH_NOTES] ,[DIFERENCE_EXCHANGE] ,[MO_SEASON_LIMIT] ,[FIN_VICHECK_NEXT_DATE_FEES] ,[FIN_VIACHECK_PERGR3000] ,[DEBIT_GROUP] ,[CHECK_RATE_ONLINE] ,[FTPFILEVERSION] ,[ID_PAYER_COMMISSION_MODE] ,[PAYER_ALLOW_MOD] ,[STATE_GROUP_BRANCH] ,[FIN_AMTTOBERECONCILED] ,[TRANSMITION_STATUS] ,[DIR_OUTPUT] ,[CONS_LAST_TRANS] ,[FIN_ACHACCNUMBER_COM] ,[ID_MAIN_BRANCH] ,[FIN_CAT_CARTERA] ,[PAYER_LIMIT_PER_DAY_NOTES] FROM envio.dba.group_branch) x"

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
    