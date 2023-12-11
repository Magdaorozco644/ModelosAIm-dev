import boto3, json
from awsglue.context import GlueContext
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, date_format

# Contexto
sc = SparkContext()
spark = SparkSession(sc)
glueContext = GlueContext(spark)


spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

def thread_function(args):
    query, secret, jdbc_viamericas, date = args
    print(f"INFO --- reading data for date: {date}")
    # Reading data from the database
    jdbcDF = spark.read.format('jdbc')\
        .option('url', jdbc_viamericas)\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
        .option('dbtable', query )\
        .option("user", secret['username'])\
        .option("password", secret['password'])\
        .option("numPartitions", 10)\
        .option("fetchsize", 1000)\
        .load()
    print(f"INFO --- number of rows for date: {date}: {jdbcDF.count()} ")
    jdbcDF = jdbcDF.withColumn('day', date_format('DATE_RECEIVER', 'yyyy-MM-dd'))
    print(f"INFO --- variable 'day' for date: {date} obtained")
    # Definir la ruta de salida en S3
    s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/receiver/"
    print(f"INFO --- writing into s3 bucket: {s3_output_path} data for date: {date}")
    # Escribir el DataFrame en formato Parquet en S3
    jdbcDF.write.partitionBy("day").parquet(s3_output_path, mode="overwrite")
    print(f"INFO --- data for date: {date} written successfully")

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

def main(dates):
    # creating pool threads
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for date in dates:
            qryStr = f"(SELECT [REBATE_AMOUNT] ,[ACCULINK_TRANID] ,[TOLL_FREE] ,[APPS] ,[ID_AD] ,[rec_fname] ,[rec_synch] ,[ID_MAIN_BRANCH_SENT] ,[ADDITIONAL_INFORMATION] ,[ID_MODIFICATION_REQ] ,[ID_RECEIVER] ,[acc_typeid] ,[FOREX_CALC] ,[DISCOUNT] ,[SYNC_COMPASS] ,[ORIGINAL_RATE] ,[rec_slname] ,[TYPEID] ,[STATUS_PAGO_AGENT] ,[ID_COUNTRY_RECEIVER] ,[EXPIRED_RATE] ,[FX_RECEIVER] ,[FOREX_ESTIMATED] ,[LOYALTY_RATE_COST] ,[ID_BRANCH] ,[SOURCE_TELEX_COMPANY] ,[_ID_CITY_ORI] ,[ID_MODIFICATION_REASON] ,[RATE_CHANGE_RECEIVER] ,[TIME_RECEIVER] ,[MAINTENANCE_FEE] ,[ID_FUNDS_OTHER_NAME] ,[SOURCE] ,[ID_SENDER] ,[DATE_TRANS_PAYEE] ,[FLAG_TEMP] ,[BRANCH_NAME_CASHIER] ,[ZIP_RECEIVER] ,[REC_SENACCTYPE] ,[AGENT_COMM_PROFIT] ,[FUNDING_FEE] ,[TOTAL_MODO_PAGO_COMP] ,[HANDLING_RECEIVER] ,[SOURCE_FEE_RATE] ,[BRANCH_CASHIER] ,[DEST_COUNTRY_TAX] ,[RECEIVER_DATE_AVAILABLE] ,[URGENCY_RECEIVER] ,[FX_RATE_ORIGINATOR] ,[PAYER_REFERENCENO] ,[SPECIAL_COMMENT] ,[ID_MAIN_BRANCH_EXPIRED] ,[SOURCE_FEE_AMOUNT] ,[type_basis_rec] ,[BRANCH_PAY_RECEIVER_ORIGINAL] ,[ID_CASHIER] ,[_ID_STATE_ORI] ,[RATE_AT_INSERT] ,[DATE_CANCEL] ,[ID_FLAG_RECEIVER] ,[PHONE1_RECEIVER] ,[ID_CURRENCY_SOURCE] ,[REF_RECEIVER] ,[FOREX_GAIN_RATE] ,[ID_RECIPIENT] ,[ORIGINATOR_BUYING_RATE] ,[ADDRESS_RECEIVER] ,[COMMISSION_PAYEE_PESOS] ,[MODE_PAY_RECEIVER] ,[REC_ACCBANK] ,[SOURCE_TAXES] ,[TELEX_RECEIVER] ,[FOREX_FIRST_ESTIMATED] ,[RATE_BASE_AT_INSERT] ,[STATE_TAX] ,[TRANS_RECEIVER] ,[SOURCE_CURRENCY_AMOUNT] ,[REC_PAYMENTTYPE] ,[NET_AMOUNT_RECEIVER] ,[BRANCH_PAY_RECEIVER] ,[REFERAL_COMISSION_FIXED] ,[FEE_RATE] ,[PHONE2_RECEIVER] ,[TOTAL_PAY_RECEIVER] ,[SOURCE_ORIGINAL_RATE] ,[rec_lname] ,[id_basis_rec] ,[SOURCE_EXCHANGE_RECEIVER] ,[RECIPIENT_DATE_INSERTED] ,[NUMID] ,[COUPON_CODE] ,[ID_FUNDS_NAME] ,[DATE_CANCELATION_REQ] ,[CLAVE_RECEIVER] ,[TELEX_COMPANY] ,[TOTAL_MODO_PAGO] ,[EXCHANGE_RECEIVER] ,[NOTES_RECEIVER] ,[DATE_EXPIRED] ,[ORIGINAL_FEE] ,[ORIGINATOR_TRANSACTION_ID] ,[EXCHANGE_COMPANY] ,[REFERAL_COMISSION_PERCENTAGE] ,[DEBIT_CARD_NUMBER] ,[_ID_COUNTRY_ORI] ,[BANK_RECEIVER] ,[id_receiver_unique] ,[PORC_COMISION_RECEIVER] ,[TOTAL_DIFERENCE] ,[FX_RATE_CUSTOMER] ,[NAME_RECEIVER] ,[CLOSING_PAYEE] ,[DATE_DEPOSIT] ,[STATUS_PAGO_PAYEE] ,[rec_mname] ,[SOURCE_TELEX_RECEIVER] ,[ID_STATE_RECEIVER] ,[ID_PAYMENT] ,[REC_ACCNUMBER] ,[FOREX_ESTIMATED_RATE] ,[MOD_PAY_CURRENCY] ,[SOURCE_EXCHANGE_COMPANY] ,[DATE_RECEIVER] ,[ID_CANCELATION_REQ] ,[ACC_RECEIVER] ,[FOREX_GAIN] ,[typed_date] ,[COMMISSION_PAYEE] ,[FOREX_FIRST_ESTIMATED_RATE] ,[email_receiver] ,[ORDER_EXPIRED] ,[DATE_MODIFICATION_REQ] ,[ID_FUND] ,[CLOSING_AGENT] ,[SOURCE_TOTAL_RECEIVER] ,[PAYMENT_DATE] ,[ID_CITY_RECEIVER] ,[ID_CURRENY] ,[rec_createacc] ,[RECOMEND_RECEIVER] ,[REC_ACCROUTING] ,[PIN_NUMBER] ,[TOTAL_RECEIVER] FROM envio.dba.receiver) x"
           # create arguments
            args = (qryStr, secret, jdbc_viamericas, date)
            # create threads
            future = executor.submit(thread_function, args)
            # append thread to the list of threads
            futures.append(future)
        for i in len(futures):
            print(f"INFO --- running thread number: {i + 1}")
            # execute threads
            futures[i].result()
if __name__ == "__main__":
    dates = ['2023', '2022', '2021', '2020']
# Definir la ruta de salida en S3

# Definir la ruta de salida en S3

# Escribir el DataFrame en formato Parquet en S3
    