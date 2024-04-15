
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
df = wr.athena.read_sql_query(sql="select coalesce(cast(max(LAST_UPDATED) as varchar), cast(date_add('hour', -5, from_unixtime(cast(to_unixtime(current_timestamp) AS bigint))) as varchar)) as max_date from viamericas.vcw_sales_products", database="viamericas")

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
qryStr = f"(SELECT [LOYALTY_SUBMIT],[BARCODENUMBER],[CLOSING_AMOUNT],[DATE],[TOTALAMOUNT],[CONTROLNUMBERPIN],[CF_VIAMERICAS],[PINSALEID],[CONSECUTIVETRANSACTIONDAILYPAYEE],[FIX_VIAMERICAS],[IS_LOYALTY],[PromoAmount],[RED_PHONE],[EXCHANGERATE],[USER_IP],[isViaOne],[SECURITY_NUMBER],[ID_COST_GATEWAY],[FIX_BRANCH],[FORCE],[RelatedTransactionId],[GAIN_BRANCH],[ACCESS_NUMBER],[IMPRESO],[ID_CUSTOMER_PHONE],[DISCOUNT_VIAMERICAS],[CUSTOMER_PHONE],[TAXAMOUNT],[ID_PRODUCT],[isVerifiedAccount],[PhantomPromo],[TotalProvider],[LOYALTY_SPEND],[SALESID],[STATUSID],[DATETRANSACTION],[IDBRANCH],[TRANSACTIONID],[PIN_RECEIPT],[CF_CUSTOMER],[DiscountVia],[GAIN_VIAMERICAS],[LOYALTY_POINTS],[ID_CARRIERS_BY_PROVIDERS],[PINSTATUS],[ID_PRODUCT_TYPE],[TotalViamericas],[RESPONSEMESSAGE],[EMPLOYEEID],[ID_SENDER],[LOYALTY_OLD_BALANCE],[LAST_UPDATED],[DISCOUNT_BRANCH],[ID_BILL_BY_SENDER],[PROMO_INFORMATION],[LOYALTY_BALANCE],[TRAN_FEE],[IDCURRENCY],[PHONENUMBER],[AMOUNT],[FEE_PREPAY],[RESPONSEID],[AGENT_COMMISSION_BY_CHECK],[COUNTRYAREACODE],[LOCALAMOUNT], convert(date, [LAST_UPDATED]) as day FROM envio.dba.vcw_sales_products WHERE  [LAST_UPDATED] >= '{today} 00:00:00') x"

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
    s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/envio/dba/vcw_sales_products/"

    # Escribir el DataFrame en formato Parquet en S3
    jdbcDF.write.partitionBy("day").parquet(s3_output_path, mode="overwrite")
    
    print(f'Data for the day: {today} written succesfully')
else:
    print(f'No data for the date: {today}')
    