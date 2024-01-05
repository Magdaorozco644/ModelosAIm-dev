
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
qryStr = f"(SELECT [RETURNED_ITEM_FEE] ,[CDMONTOMAXIMOPORCHEQUE] ,[SNLOG] ,[SCANNER_URL] ,[CUSTOMER_ID_REQUIRED] ,[NUDEPOSITO_CHEQUE] ,[VERIFICATION_FEE] ,[EXPIRATION_DATE] ,[FINCEN_REGISTRATION_DATE] ,[Date_Installed] ,[TIPOCUENTAID] ,[ALLOW_VIRTUAL_SCANNER] ,[OWN_LICENSE] ,[SNVALIDACIONHUELLA] ,[PROCESS_CHECKS] ,[VRNUMERO_HUELLAS] ,[SNVIACHECKLIGHT] ,[EXPERIENCE_DATE] ,[VERIFICATION_RISK_FEE] ,[FINGERPRINT_REQUIRED] ,[VRALIMENTAR_CAJA] ,[DSESCANER_CHEQUE] ,[GVERIFY_TERMS_RISK] ,[FINCEN_EXPIRATION_DATE] ,[ECHECK_RETURNED_ITEM_FEE] ,[ACCOUNT_VALIDATION] ,[RISK_MANAGEMENT] ,[SNENDOSO] ,[MAX_AMOUNT_ONHOLD_RESTRICTION] ,[RiskFree] ,[SCANNER_URL_2OPTION] ,[SNVIACHECK] ,[CDSTATE] ,[DEPOSIT_CHECKS] ,[IS_GVERIFY] ,[VRCOBRO_VERIFICACION] ,[ROUTING_VALIDATION_DEPOSIT] ,[NIVEL_SEGURIDAD] ,[ID_FINCEN_TYPE] ,[CUSTOMER_AMASSED] ,[IS_VIP] ,[DSESCANER_ID] ,[GVERIFY_TERMS] ,[ROUTING_VALIDATION] ,[ENABLE_ROUTE_VALIDATION] ,[CDMONTOMAXIMODIA] ,[COMPLETED_FINCEN] ,[RETURNED_FINAL_ITEM_FEE] ,[POFEE] ,[SNAPERTURACAJA] ,[CUSTOMER_PHOTO_REQUIRED] ,[ACCOUNT_VALIDATION_DEPOSIT] ,[GVerifyFree] ,[IS_ECHECKS] ,[CDAGENCIA] ,[SNVIACHECKPLUS] ,[RESTRICCION] ,[ReprocessIRD] FROM viachecks.dbo.tvcp_param_agencias) x"

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
    