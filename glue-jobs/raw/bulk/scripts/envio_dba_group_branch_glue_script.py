
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
qryStr = f"(SELECT [FIN_DEPDESCRIPTION] ,[PAYER_LIMIT_PER_DAY_NOTES] ,[FIN_YEARLYINTERESTRATE] ,[PAYER_MOD_INTERFASE] ,[PAYER_REQUIRE_ID_TO_PICKUP] ,[FIN_ACHACCROUTING_COM] ,[FIN_ACHACCNUMBER_COM] ,[FIN_CLOSINGDAYCODE] ,[NEXT_EXCHANGE] ,[PAYER_IDENTIFIES_ORDER_BY] ,[GROUP_TYPE] ,[DIR_OUTPUT] ,[EMAIL_NOTIFICATIONS] ,[PAYER_CAN_INTERFASE] ,[FIN_COLLECTMETHOD] ,[FIN_DEPDEPOSITCODE] ,[PAYER_TYPE_CLOSING_BALANCE] ,[CREDITO_GROUP] ,[ID_PAYER_COMMISSION_MODE] ,[ID_CURRENCY_SOURCE] ,[DEBIT_GROUP] ,[DEBITO_GROUP] ,[PRIORIDAD_GROUP_BRANCH] ,[PAYER_ORDER_EXPIRATION_IN_DAYS] ,[FIN_AMTTOBERECONCILED] ,[MODE_TRANS_GROUP] ,[FIN_COMMISSIONMETHOD] ,[FIN_VIACHECK_PERXTRSRY] ,[PAYER_LIMIT_PER_MONTH_NOTES] ,[FIN_SCHEDULE] ,[FIN_STATUSCOMMENTS] ,[PAYER_WEEKEND_HOURS] ,[EMAIL_OPERATIONS] ,[ALLOW_DEP_FAX_NOT] ,[FIN_ACHACCROUTING] ,[PAYER_NAT_CLOSING] ,[BRANCHES] ,[NAME_MAIN_BRANCH] ,[ALTERNATIVE_TOP] ,[BALANCE_MATCH] ,[PAYER_TIME_TO_SEND_PAYMENT_RECEIPT] ,[ID_PHASE] ,[GRO_COMPANYIDFOLIO] ,[MO_SEASON_LIMIT] ,[PAYER_MAX_AMOUNT_NOTES] ,[AllowInstantMod] ,[EMAIL_NOTIFICATIONS_TYPE] ,[FLAG_COM_CHAIN] ,[PAYER_ORDER_AVAILABILITY_IN_MINUTES] ,[PAYER_REQUIRES_FOLIO_TO_PICKUP] ,[FIN_AMTTOBERECONCILEDNOTES] ,[PAYER_CANCELATION_RESPONSE_IN_MINUTES_WKNDS] ,[PAYER_NETWORK_PAYMENT] ,[FIN_ACHACCTYPEID] ,[DATE_CREA_MAIN_BRANCH] ,[PAYER_ORDER_AVAILABILITY_IN_MINUTES_WKNDS] ,[DEBT_LIMIT_WKND] ,[FIN_VICHECK_NEXT_DATE_FEES] ,[BALANCE_GROUP] ,[NEXT_EXCHANGE_VALID_TO] ,[PAYER_MIN_AMOUNT] ,[PAYER_MODIFICATION_RESPONSE_IN_MINUTES_WKNDS] ,[DEBT_LIMIT] ,[EMAIL_GROUP] ,[FIN_ACHACCTYPEID_COM] ,[REVIEW_CREDIT_TOP_ON] ,[PAYER_DEPOSITS_VERIFY_ACC_HOLDER] ,[PAYER_MAX_AMOUNT] ,[CREDIT_GROUP] ,[ID_BRANCH_DEPOSITS] ,[MO_DAILY_LIMIT] ,[IS_BANK] ,[PAYER_LIMIT_PER_DAY] ,[FIN_VIACHECK_COMXCHKGR3000] ,[FAX_NOTIFICATIONS] ,[SYNC_COMPASS] ,[PAYER_LIMIT_PER_MONTH] ,[NEXT_EXCHANGE_VALID_FROM] ,[FIN_VIACHECK_COMXCHK] ,[FIN_VIACHECK_PERGR3000] ,[ALLOW_BEN_CHANGES] ,[TIME_TRANS] ,[ID_COUNTRY_SOURCE] ,[FIN_DSO] ,[DIR_INPUT] ,[ALTERNATIVE_COUNT] ,[FIN_ACHACCNUMBER] ,[PAYER_TYPE_CLOSING] ,[PAYER_CALLCENTER_BUSINESS_HOURS] ,[FIN_VIACHECK_PERXCHK] ,[FIN_CAT_CREDIT] ,[CONS_LAST_TRANS] ,[TOP_CREDIT] ,[PAYER_CALLCENTER_WEEKEND_HOURS] ,[ID_MAIN_BRANCH] ,[PAYER_PICKUP_INSTRUCTIONS] ,[DEBT_LIMIT_WKND_MO] ,[PAYER_MODIFICATION_RESPONSE_IN_MINUTES] ,[PAYER_MOD_CHARGE] ,[DEBT_LIMIT_MO] ,[DAILY_STATEMENT_TIME] ,[PAYER_BUSINESS_HOURS] ,[ID_CASHIER_MODDEBITLIMIT] ,[FIN_DEPIDCUENTABANCO] ,[FLAG_GRUPO] ,[PAYER_CANCELATION_RESPONSE_IN_MINUTES] ,[FIN_VIACHECK_COMXCHKTRSRY] ,[PAYER_TYPE_FUNDING] ,[FIN_MONTHLYCLOSING] ,[PASTDUE_INACTIVATION] ,[PAYER_LIMIT_PER_YEAR] ,[PAYER_CAN_CHARGE] ,[PAYER_WEB_INTERFASE] ,[FIN_CAT_CARTERA] ,[PAYER_ID_REQUIREMENTS] ,[PAYER_MANUAL_PAYMENTS] ,[CHECK_RATE_ONLINE] ,[DIFERENCE_EXCHANGE] ,[TOP_CREDIT_WKND] ,[PAYER_LIMIT_PER_YEAR_NOTES] ,[STATE_GROUP_BRANCH] ,[USES_FOLIO] ,[FTPFILEVERSION] ,[MODEM_GROUP] ,[FIN_CAT_VIACHECK] ,[TYPE_GROUP] ,[FIN_STATUS] ,[FIN_RCC_ALLOWED] ,[FIN_COLLECTMETHOD_2] ,[PAYER_ALLOW_MOD] ,[FIN_VIACHECK_VALFEE] ,[FIN_DEPIDBANCO] ,[TRANSMITION_STATUS] ,[FIN_AJUSTEDDSO] ,[PAYER_NOTIFIES_BENEFICIARY] FROM envio.dba.group_branch) x"

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
    