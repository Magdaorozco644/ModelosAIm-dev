
import boto3, json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, date_format
import awswrangler as wr
from datetime import date

# Contexto
sc = SparkContext()
spark = SparkSession(sc)
glueContext = GlueContext(spark)

today = date.today()

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# Get max date in athena
df = wr.athena.read_sql_query(sql="select coalesce(cast(max(LAST_UPDATED) as varchar), cast(date_add('hour', -5, from_unixtime(cast(to_unixtime(current_timestamp) AS bigint))) as varchar)) as max_date from viamericas.receiver", database="viamericas")

athena_max_date = df['max_date'].tolist()[0]

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
qryStr = f"(SELECT [AGENT_COMM_PROFIT],[FOREX_ESTIMATED],[ID_CASHIER],[REC_ACCROUTING],[ID_MODIFICATION_REQ],[DATE_EXPIRED],[FOREX_FIRST_ESTIMATED],[PORC_COMISION_RECEIVER],[FOREX_GAIN],[ID_BRANCH],[FOREX_FIRST_ESTIMATED_RATE],[RATE_CHANGE_RECEIVER],[REBATE_AMOUNT],[DATE_RECEIVER],[SOURCE_TELEX_RECEIVER],[APPS],[FX_RATE_CUSTOMER],[DATE_TRANS_PAYEE],[REC_SENACCTYPE],[ID_CITY_RECEIVER],[SOURCE_FEE_RATE],[TOTAL_MODO_PAGO_COMP],[ID_FUNDS_OTHER_NAME],[EXCHANGE_RECEIVER],[DISCOUNT],[DEST_COUNTRY_TAX],[ID_AD],[LOYALTY_RATE_COST],[MODE_PAY_RECEIVER],[CLAVE_RECEIVER],[PAYER_REFERENCENO],[STATE_TAX],[SOURCE_TELEX_COMPANY],[FEE_RATE],[ID_COUNTRY_RECEIVER],[PAYMENT_DATE],[ACCULINK_TRANID],[id_receiver_unique],[BRANCH_PAY_RECEIVER],[SOURCE_EXCHANGE_COMPANY],[REFERAL_COMISSION_PERCENTAGE],[DATE_DEPOSIT],[ID_RECEIVER],[ZIP_RECEIVER],[ID_CURRENY],[TOTAL_MODO_PAGO],[RATE_AT_INSERT],[ID_PAYMENT],[ID_FUND],[RATE_BASE_AT_INSERT],[LAST_UPDATED],[RECOMEND_RECEIVER],[DATE_CANCEL],[SOURCE_CURRENCY_AMOUNT],[BRANCH_PAY_RECEIVER_ORIGINAL],[BRANCH_NAME_CASHIER],[ID_FLAG_RECEIVER],[ORDER_EXPIRED],[RECEIVER_DATE_AVAILABLE],[SOURCE_TAXES],[TELEX_RECEIVER],[DATE_MODIFICATION_REQ],[FX_RECEIVER],[ORIGINAL_RATE],[MOD_PAY_CURRENCY],[REF_RECEIVER],[PIN_NUMBER],[ID_RECIPIENT],[typed_date],[ID_MODIFICATION_REASON],[ORIGINAL_FEE],[FOREX_GAIN_RATE],[ORIGINATOR_BUYING_RATE],[TELEX_COMPANY],[STATUS_PAGO_AGENT],[BANK_RECEIVER],[EXCHANGE_COMPANY],[ID_CANCELATION_REQ],[NET_AMOUNT_RECEIVER],[ID_CURRENCY_SOURCE],[DATE_CANCELATION_REQ],[SOURCE_ORIGINAL_RATE],[SOURCE_FEE_AMOUNT],[TIME_RECEIVER],[ID_STATE_RECEIVER],[COMMISSION_PAYEE],[TOTAL_PAY_RECEIVER],[SOURCE],[SOURCE_TOTAL_RECEIVER],[FOREX_CALC],[COMMISSION_PAYEE_PESOS],[ID_MAIN_BRANCH_EXPIRED],[REC_PAYMENTTYPE],[TYPEID],[RECIPIENT_DATE_INSERTED],[EXPIRED_RATE],[FOREX_ESTIMATED_RATE],[FX_RATE_ORIGINATOR],[ID_FUNDS_NAME],[SOURCE_EXCHANGE_RECEIVER],[REFERAL_COMISSION_FIXED],[HANDLING_RECEIVER],[COUPON_CODE],[BRANCH_CASHIER],[FUNDING_FEE],[ID_SENDER],[MAINTENANCE_FEE],[TOTAL_RECEIVER],[acc_typeid],[ID_MAIN_BRANCH_SENT],[STATUS_PAGO_PAYEE], convert(date, [LAST_UPDATED]) as day FROM envio.dba.receiver WHERE  [LAST_UPDATED] >= '{today} 00:00:00') x"

jdbcDF = spark.read.format('jdbc')\
        .option('url', jdbc_viamericas)\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
        .option('dbtable', qryStr )\
        .option("user", secret['username'])\
        .option("password", secret['password'])\
        .option('partitionColumn', 'LAST_UPDATED')\
        .option("lowerBound", f'{today} 00:00:00')\
        .option("upperBound", f'{today} 23:59:59')\
        .option("numPartitions", 10)\
        .option("fetchsize", 1000)\
        .load()

number_of_rows = jdbcDF.count()

print(f'number of rows obtained for date higher than {today}: {number_of_rows}')

# jdbcDF = jdbcDF.withColumn('day', date_format('LAST_UPDATED', 'yyyy-MM-dd'))

if number_of_rows > 0: 
    # Definir la ruta de salida en S3
    s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/receiver/"

    # Escribir el DataFrame en formato Parquet en S3
    jdbcDF.write.partitionBy("day").parquet(s3_output_path, mode="overwrite")
    
    print(f'Data for the day: {today} written succesfully')
else:
    print(f'No data for the date: {today}')
    