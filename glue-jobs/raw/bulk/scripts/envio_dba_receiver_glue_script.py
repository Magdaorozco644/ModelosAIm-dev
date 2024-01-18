
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
            qryStr = f"(SELECT [DATE_DEPOSIT] ,[BRANCH_PAY_RECEIVER] ,[DATE_RECEIVER] ,[COUPON_CODE] ,[acc_typeid] ,[REC_PAYMENTTYPE] ,[DATE_EXPIRED] ,[TYPEID] ,[FOREX_GAIN_RATE] ,[RECIPIENT_DATE_INSERTED] ,[FOREX_ESTIMATED_RATE] ,[RECEIVER_DATE_AVAILABLE] ,[NET_AMOUNT_RECEIVER] ,[RATE_BASE_AT_INSERT] ,[ID_AD] ,[ORDER_EXPIRED] ,[MAINTENANCE_FEE] ,[SOURCE] ,[FOREX_FIRST_ESTIMATED_RATE] ,[SOURCE_TELEX_RECEIVER] ,[TIME_RECEIVER] ,[ID_CURRENCY_SOURCE] ,[ID_RECEIVER] ,[PAYER_REFERENCENO] ,[MODE_PAY_RECEIVER] ,[FX_RECEIVER] ,[TELEX_COMPANY] ,[PIN_NUMBER] ,[FX_RATE_CUSTOMER] ,[FOREX_GAIN] ,[SOURCE_EXCHANGE_COMPANY] ,[RATE_CHANGE_RECEIVER] ,[ACCULINK_TRANID] ,[REBATE_AMOUNT] ,[EXPIRED_RATE] ,[ID_COUNTRY_RECEIVER] ,[STATE_TAX] ,[SOURCE_TELEX_COMPANY] ,[ID_STATE_RECEIVER] ,[DATE_CANCELATION_REQ] ,[SOURCE_EXCHANGE_RECEIVER] ,[ID_CANCELATION_REQ] ,[REC_ACCROUTING] ,[DATE_CANCEL] ,[SOURCE_TAXES] ,[BRANCH_PAY_RECEIVER_ORIGINAL] ,[ID_CITY_RECEIVER] ,[SOURCE_TOTAL_RECEIVER] ,[REFERAL_COMISSION_FIXED] ,[ID_MODIFICATION_REASON] ,[DISCOUNT] ,[ID_MAIN_BRANCH_SENT] ,[id_receiver_unique] ,[HANDLING_RECEIVER] ,[RATE_AT_INSERT] ,[PORC_COMISION_RECEIVER] ,[CLAVE_RECEIVER] ,[COMMISSION_PAYEE] ,[DATE_TRANS_PAYEE] ,[REF_RECEIVER] ,[ID_RECIPIENT] ,[ORIGINAL_FEE] ,[LOYALTY_RATE_COST] ,[ZIP_RECEIVER] ,[FEE_RATE] ,[ID_FUND] ,[BRANCH_CASHIER] ,[typed_date] ,[TOTAL_RECEIVER] ,[ID_BRANCH] ,[ID_MODIFICATION_REQ] ,[SOURCE_ORIGINAL_RATE] ,[DEST_COUNTRY_TAX] ,[SOURCE_FEE_RATE] ,[RECOMEND_RECEIVER] ,[ID_FUNDS_OTHER_NAME] ,[REC_SENACCTYPE] ,[APPS] ,[STATUS_PAGO_PAYEE] ,[FUNDING_FEE] ,[REFERAL_COMISSION_PERCENTAGE] ,[TOTAL_MODO_PAGO_COMP] ,[TELEX_RECEIVER] ,[FOREX_FIRST_ESTIMATED] ,[DATE_MODIFICATION_REQ] ,[ID_CURRENY] ,[ORIGINAL_RATE] ,[MOD_PAY_CURRENCY] ,[STATUS_PAGO_AGENT] ,[COMMISSION_PAYEE_PESOS] ,[FX_RATE_ORIGINATOR] ,[EXCHANGE_COMPANY] ,[ID_FUNDS_NAME] ,[EXCHANGE_RECEIVER] ,[BANK_RECEIVER] ,[ID_PAYMENT] ,[TOTAL_PAY_RECEIVER] ,[ID_SENDER] ,[SOURCE_FEE_AMOUNT] ,[AGENT_COMM_PROFIT] ,[BRANCH_NAME_CASHIER] ,[ID_CASHIER] ,[ORIGINATOR_BUYING_RATE] ,[FOREX_ESTIMATED] ,[ID_FLAG_RECEIVER] ,[FOREX_CALC] ,[ID_MAIN_BRANCH_EXPIRED] ,[PAYMENT_DATE] ,[SOURCE_CURRENCY_AMOUNT] ,[TOTAL_MODO_PAGO] FROM envio.dba.receiver WHERE DATE_RECEIVER >= '{date}-01-01 00:00:00.000' AND DATE_RECEIVER <= '{date}-12-31 23:59:59.000') x"
            args = (qryStr, secret, jdbc_viamericas, date)
            # create threads
            future = executor.submit(thread_function, args)
            # append thread to the list of threads
            futures.append(future)
        for i in range(len(futures)):
            print(f"INFO --- running thread number: {i + 1}")
            # execute threads
            futures[i].result()
            
if __name__ == "__main__":
    dates = [
        '2023', '2022', '2021', '2020', '2019', '2018', 
        '2017', '2016', '2015', '2014', '2013', '2012', 
        '2011', '2010', '2009', '2008', '2007', '2006',
        '2005'
    ]
    
    main(dates)
    