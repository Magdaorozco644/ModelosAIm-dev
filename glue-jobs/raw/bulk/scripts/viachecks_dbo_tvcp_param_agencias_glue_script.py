
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
qryStr = f"(SELECT [MAX_AMOUNT_ONHOLD_RESTRICTION] ,[EXPERIENCE_DATE] ,[FINGERPRINT_REQUIRED] ,[ID_FINCEN_TYPE] ,[SNLOG] ,[COMPLETED_FINCEN] ,[TIPOCUENTAID] ,[ACCOUNT_VALIDATION_DEPOSIT] ,[PROCESS_CHECKS] ,[OWN_LICENSE] ,[RISK_MANAGEMENT] ,[ROUTING_VALIDATION] ,[SCANNER_URL] ,[NUDEPOSITO_CHEQUE] ,[POFEE] ,[CDSTATE] ,[ACCOUNT_VALIDATION] ,[ALLOW_VIRTUAL_SCANNER] ,[RETURNED_FINAL_ITEM_FEE] ,[SNVIACHECKPLUS] ,[IS_ECHECKS] ,[Date_Installed] ,[SNENDOSO] ,[GVerifyFree] ,[NIVEL_SEGURIDAD] ,[IS_GVERIFY] ,[GVERIFY_TERMS] ,[RESTRICCION] ,[CDMONTOMAXIMODIA] ,[CUSTOMER_ID_REQUIRED] ,[VERIFICATION_FEE] ,[RiskFree] ,[FINCEN_EXPIRATION_DATE] ,[VRNUMERO_HUELLAS] ,[SNVIACHECKLIGHT] ,[VRALIMENTAR_CAJA] ,[ENABLE_ROUTE_VALIDATION] ,[CDMONTOMAXIMOPORCHEQUE] ,[DSESCANER_CHEQUE] ,[VRCOBRO_VERIFICACION] ,[ROUTING_VALIDATION_DEPOSIT] ,[CDAGENCIA] ,[CUSTOMER_PHOTO_REQUIRED] ,[GVERIFY_TERMS_RISK] ,[SCANNER_URL_2OPTION] ,[EXPIRATION_DATE] ,[DEPOSIT_CHECKS] ,[SNAPERTURACAJA] ,[FINCEN_REGISTRATION_DATE] ,[IS_VIP] ,[ECHECK_RETURNED_ITEM_FEE] ,[SNVALIDACIONHUELLA] ,[CUSTOMER_AMASSED] ,[DSESCANER_ID] ,[SNVIACHECK] ,[RETURNED_ITEM_FEE] ,[ReprocessIRD] ,[VERIFICATION_RISK_FEE] FROM viachecks.dbo.tvcp_param_agencias) x"

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
    