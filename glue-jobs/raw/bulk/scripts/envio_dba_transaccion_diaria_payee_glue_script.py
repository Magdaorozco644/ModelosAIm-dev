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
    jdbcDF = jdbcDF.withColumn('day', date_format('DATE_TRANS_DIARIA', 'yyyy-MM-dd'))
    print(f"INFO --- variable 'day' for date: {date} obtained")
    # Definir la ruta de salida en S3
    s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/transaccion_diaria_payee/"
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

            qryStr = f"(SELECT [BNKID] ,[DES_TRANS_DIARIA] ,[WIRE_AMT_REFERENCED] ,[CONS_TRANS_REVERSAL] ,[ID_GROUP_TRANS_DIARIA] ,[HOUR_TRANS_DIARIA] ,[TOTAL_AMOUNT] ,[ID_CONCEPTO_CONTABLE] ,[DATE_SYSTEM] ,[DATE_TRANS_DIARIA] ,[BALANCE_TRANS_DIARIA] ,[ID_CASHIER] ,[CREDIT_TRANS_DIARIA] ,[FLAG_RECONCILIATION] ,[CONS_TRANS_DIARIA] ,[DESCRIPCION_SUSPENSE] ,[LINK_REFERENCE] ,[DESC_TRANS_DIARIA1] ,[NUM_WIRETRANSFER] ,[DEBIT_TRANS_DIARIA] FROM envio.dba.transaccion_diaria_payee) x"
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
    