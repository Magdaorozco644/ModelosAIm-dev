
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
qryStr = f"(SELECT [CDSTATE] ,[RESTRICCION] ,[GVERIFY_TERMS_RISK] ,[DSESCANER_CHEQUE] ,[DEPOSIT_CHECKS] ,[POFEE] ,[PROCESS_CHECKS] ,[SCANNER_URL] ,[SNVIACHECK] ,[TIPOCUENTAID] ,[RiskFree] ,[FINGERPRINT_REQUIRED] ,[ROUTING_VALIDATION] ,[NIVEL_SEGURIDAD] ,[ALLOW_VIRTUAL_SCANNER] ,[NUDEPOSITO_CHEQUE] ,[ReprocessIRD] ,[RISK_MANAGEMENT] ,[ID_FINCEN_TYPE] ,[VERIFICATION_RISK_FEE] ,[VRCOBRO_VERIFICACION] ,[SNVIACHECKPLUS] ,[IS_GVERIFY] ,[FINCEN_REGISTRATION_DATE] ,[SNAPERTURACAJA] ,[SNVIACHECKLIGHT] ,[COMPLETED_FINCEN] ,[ACCOUNT_VALIDATION] ,[EXPERIENCE_DATE] ,[ACCOUNT_VALIDATION_DEPOSIT] ,[IS_ECHECKS] ,[RETURNED_FINAL_ITEM_FEE] ,[GVERIFY_TERMS] ,[Date_Installed] ,[DSESCANER_ID] ,[ROUTING_VALIDATION_DEPOSIT] ,[CUSTOMER_PHOTO_REQUIRED] ,[EXPIRATION_DATE] ,[MAX_AMOUNT_ONHOLD_RESTRICTION] ,[CDMONTOMAXIMOPORCHEQUE] ,[CUSTOMER_ID_REQUIRED] ,[CDMONTOMAXIMODIA] ,[FINCEN_EXPIRATION_DATE] ,[CUSTOMER_AMASSED] ,[ECHECK_RETURNED_ITEM_FEE] ,[GVerifyFree] ,[RETURNED_ITEM_FEE] ,[CDAGENCIA] ,[VRALIMENTAR_CAJA] ,[ENABLE_ROUTE_VALIDATION] ,[VERIFICATION_FEE] ,[SNLOG] ,[SCANNER_URL_2OPTION] ,[IS_VIP] ,[SNVALIDACIONHUELLA] ,[OWN_LICENSE] ,[VRNUMERO_HUELLAS] ,[SNENDOSO] FROM viachecks.dbo.tvcp_param_agencias) x"

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
s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/viachecks/dbo/tvcp_param_agencias/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.parquet(s3_output_path, mode="overwrite")
    