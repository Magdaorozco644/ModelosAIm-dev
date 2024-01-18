
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
    jdbcDF = jdbcDF.withColumn('day', date_format('DATE_CREATED', 'yyyy-MM-dd'))
    print(f"INFO --- variable 'day' for date: {date} obtained")
    # Definir la ruta de salida en S3

    s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/fraud/fraud_vectors_v2_1/"
    
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
            qryStr = f"(SELECT [PAYOUT_S] ,[WEEKDAY_6.0] ,[PAYOUT_D] ,[PAYOUT_3] ,[WEEKDAY_1] ,[PAYOUT_6] ,[PAYOUT_2] ,[WEEKDAY_1.0] ,[WEEKDAY_2.0] ,[SENDER_MINUTES_SINCE_LAST_TRANSACTION] ,[PAYOUT_4] ,[ID_COUNTRY_RECEIVER] ,[PAYOUT_1] ,[WEEKDAY_4] ,[PAYOUT_7] ,[HOUR_SENDER] ,[PAYOUT_0] ,[WEEKDAY_5] ,[PAYOUT_5] ,[PAYOUT_P] ,[NET_AMOUNT_RECEIVER] ,[WEEKDAY_5.0] ,[RECEIVER_TRANSACTION_COUNT] ,[WEEKDAY_3] ,[ADDRESS_RECEIVER_TRANSACTION_COUNT] ,[DATE_CREATED] ,[SENDER_FRAUD] ,[IDPAYER_FRAUD] ,[ID] ,[PAYOUT_T] ,[ID_BRANCH] ,[ID_SALES_REPRESENTATIVE] ,[BRANCH_MINUTES_SINCE_LAST_TRANSACTION] ,[RECEIVER_FRAUD] ,[PAYOUT_X] ,[WEEKDAY_4.0] ,[WEEKDAY_3.0] ,[TRANSACTION_UNIQUE] ,[SENDER_DAYS_TO_LAST_TRANSACTION] ,[PAYOUT_C] ,[PAYOUT_M] ,[PAYOUT_O] ,[%TX_BRANCITY] ,[WEEKDAY_6] ,[PAYOUT_N] ,[BRANCH_WORKING_DAYS] ,[IDLOCATION_FRAUD] ,[WEEKDAY_2] ,[SENDER_SENDING_DAYS] ,[ID_RECEIVER] ,[IN_RANGE] ,[TX_BRANCITY] ,[MODE_PAY_RECEIVER] FROM envio.fraud.fraud_vectors_v2_1 WHERE DATE_CREATED >= '{date}-01-01 00:00:00.000' AND DATE_CREATED <= '{date}-12-31 23:59:59.000') x"
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
    