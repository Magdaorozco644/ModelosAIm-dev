
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
qryStr = f"(SELECT [TRANSACTION_UNIQUE] ,[MODE_PAY_RECEIVER] ,[PAYOUT_0] ,[WEEKDAY_4.0] ,[NET_AMOUNT_RECEIVER] ,[SENDER_FRAUD] ,[PAYOUT_P] ,[PAYOUT_6] ,[PAYOUT_T] ,[WEEKDAY_3] ,[ID_RECEIVER] ,[ID_SALES_REPRESENTATIVE] ,[PAYOUT_1] ,[WEEKDAY_3.0] ,[PAYOUT_X] ,[PAYOUT_M] ,[RECEIVER_TRANSACTION_COUNT] ,[WEEKDAY_5] ,[ID_COUNTRY_RECEIVER] ,[IN_RANGE] ,[DATE_CREATED] ,[WEEKDAY_6.0] ,[RECEIVER_FRAUD] ,[PAYOUT_5] ,[PAYOUT_4] ,[PAYOUT_N] ,[PAYOUT_2] ,[ADDRESS_RECEIVER_TRANSACTION_COUNT] ,[SENDER_MINUTES_SINCE_LAST_TRANSACTION] ,[BRANCH_MINUTES_SINCE_LAST_TRANSACTION] ,[ID_BRANCH] ,[TX_BRANCITY] ,[ID] ,[SENDER_DAYS_TO_LAST_TRANSACTION] ,[BRANCH_WORKING_DAYS] ,[PAYOUT_3] ,[PAYOUT_O] ,[WEEKDAY_2] ,[WEEKDAY_2.0] ,[WEEKDAY_5.0] ,[WEEKDAY_1.0] ,[WEEKDAY_1] ,[PAYOUT_D] ,[SENDER_SENDING_DAYS] ,[IDLOCATION_FRAUD] ,[IDPAYER_FRAUD] ,[PAYOUT_7] ,[PAYOUT_C] ,[WEEKDAY_6] ,[PAYOUT_S] ,[WEEKDAY_4] ,[HOUR_SENDER] ,[%TX_BRANCITY] FROM envio.fraud.fraud_vectors_v2_1) x"

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
s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/fraud/fraud_vectors_v2_1/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.parquet(s3_output_path, mode="overwrite")
    