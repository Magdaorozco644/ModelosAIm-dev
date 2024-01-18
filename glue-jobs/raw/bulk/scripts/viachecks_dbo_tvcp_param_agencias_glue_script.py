
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
qryStr = f"(SELECT [VERIFICATION_FEE] ,[FINGERPRINT_REQUIRED] ,[VRCOBRO_VERIFICACION] ,[NUDEPOSITO_CHEQUE] ,[GVERIFY_TERMS_RISK] ,[CDAGENCIA] ,[RISK_MANAGEMENT] ,[TIPOCUENTAID] ,[SNAPERTURACAJA] ,[CDMONTOMAXIMODIA] ,[SCANNER_URL_2OPTION] ,[SCANNER_URL] ,[DEPOSIT_CHECKS] ,[SNLOG] ,[CDMONTOMAXIMOPORCHEQUE] ,[ReprocessIRD] ,[PROCESS_CHECKS] ,[SNVIACHECK] ,[DSESCANER_CHEQUE] ,[ACCOUNT_VALIDATION] ,[ID_FINCEN_TYPE] ,[RiskFree] ,[ALLOW_VIRTUAL_SCANNER] ,[CUSTOMER_AMASSED] ,[SNVALIDACIONHUELLA] ,[EXPERIENCE_DATE] ,[COMPLETED_FINCEN] ,[ROUTING_VALIDATION] ,[IS_ECHECKS] ,[EXPIRATION_DATE] ,[GVerifyFree] ,[IS_VIP] ,[RETURNED_ITEM_FEE] ,[VRNUMERO_HUELLAS] ,[VERIFICATION_RISK_FEE] ,[ECHECK_RETURNED_ITEM_FEE] ,[ENABLE_ROUTE_VALIDATION] ,[IS_GVERIFY] ,[NIVEL_SEGURIDAD] ,[CUSTOMER_PHOTO_REQUIRED] ,[POFEE] ,[VRALIMENTAR_CAJA] ,[SNVIACHECKLIGHT] ,[GVERIFY_TERMS] ,[CUSTOMER_ID_REQUIRED] ,[ACCOUNT_VALIDATION_DEPOSIT] ,[RETURNED_FINAL_ITEM_FEE] ,[FINCEN_EXPIRATION_DATE] ,[CDSTATE] ,[SNVIACHECKPLUS] ,[ROUTING_VALIDATION_DEPOSIT] ,[RESTRICCION] ,[DSESCANER_ID] ,[OWN_LICENSE] ,[MAX_AMOUNT_ONHOLD_RESTRICTION] ,[FINCEN_REGISTRATION_DATE] ,[Date_Installed] ,[SNENDOSO] FROM viachecks.dbo.tvcp_param_agencias) x"

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
    