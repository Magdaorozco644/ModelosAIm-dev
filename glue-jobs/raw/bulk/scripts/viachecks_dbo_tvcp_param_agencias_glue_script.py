
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
qryStr = f"(SELECT [ENABLE_ROUTE_VALIDATION] ,[FINCEN_REGISTRATION_DATE] ,[FINGERPRINT_REQUIRED] ,[TIPOCUENTAID] ,[SNVIACHECKLIGHT] ,[ACCOUNT_VALIDATION_DEPOSIT] ,[Date_Installed] ,[COMPLETED_FINCEN] ,[RISK_MANAGEMENT] ,[ReprocessIRD] ,[VERIFICATION_RISK_FEE] ,[GVerifyFree] ,[ID_FINCEN_TYPE] ,[VERIFICATION_FEE] ,[CDSTATE] ,[ALLOW_VIRTUAL_SCANNER] ,[RESTRICCION] ,[GVERIFY_TERMS] ,[SNLOG] ,[POFEE] ,[CUSTOMER_AMASSED] ,[VRCOBRO_VERIFICACION] ,[GVERIFY_TERMS_RISK] ,[CDMONTOMAXIMODIA] ,[ECHECK_RETURNED_ITEM_FEE] ,[SNENDOSO] ,[DSESCANER_ID] ,[IS_GVERIFY] ,[RETURNED_ITEM_FEE] ,[EXPIRATION_DATE] ,[RiskFree] ,[CUSTOMER_PHOTO_REQUIRED] ,[DEPOSIT_CHECKS] ,[SCANNER_URL_2OPTION] ,[DSESCANER_CHEQUE] ,[SNVIACHECK] ,[NIVEL_SEGURIDAD] ,[EXPERIENCE_DATE] ,[PROCESS_CHECKS] ,[IS_VIP] ,[CDMONTOMAXIMOPORCHEQUE] ,[VRALIMENTAR_CAJA] ,[ROUTING_VALIDATION] ,[OWN_LICENSE] ,[ACCOUNT_VALIDATION] ,[RETURNED_FINAL_ITEM_FEE] ,[SNAPERTURACAJA] ,[NUDEPOSITO_CHEQUE] ,[MAX_AMOUNT_ONHOLD_RESTRICTION] ,[SCANNER_URL] ,[SNVIACHECKPLUS] ,[IS_ECHECKS] ,[CUSTOMER_ID_REQUIRED] ,[SNVALIDACIONHUELLA] ,[VRNUMERO_HUELLAS] ,[FINCEN_EXPIRATION_DATE] ,[ROUTING_VALIDATION_DEPOSIT] ,[CDAGENCIA] FROM viachecks.dbo.tvcp_param_agencias) x"

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
    