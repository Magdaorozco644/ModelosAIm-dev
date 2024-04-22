
import boto3, json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, date_format, lit
import awswrangler as wr
from datetime import date
from botocore.exceptions import ClientError
from Codes.check_partitions import *

# Contexto
#sc = SparkContext()
#spark = SparkSession(sc)
#glueContext = GlueContext(spark)

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

client = boto3.client('glue')

def get_partitions(input_schema, input_table ):
		response = client.get_partitions(DatabaseName= input_schema, TableName= input_table)
		results = response['Partitions']
		while "NextToken" in response:
			response = client.get_partitions(DatabaseName= input_schema, TableName= input_table, NextToken=response["NextToken"])
			results.extend(response["Partitions"])
		partitionvalues = [tuple(x['Values']) for x in results]
		partitionvalues.sort(reverse=True)
		return partitionvalues

max_date = get_partitions('viamericas','receiver')[0][0]
print("Max date in partitions is:", max_date)

# Get max date in athena

df = wr.athena.read_sql_query(sql=f"select coalesce(cast(max(LAST_UPDATED) as varchar), substring(cast(at_timezone(current_timestamp,'America/Bogota') as varchar(100)),1,23)) as max_date from viamericas.receiver where day = '{max_date}' and LAST_UPDATED <= cast(substring(cast(at_timezone(current_timestamp,'America/Bogota') as varchar(100)),1,23) as timestamp)", database="viamericas")



athena_max_date = df['max_date'].tolist()[0]
athena_max_day = athena_max_date[0:10]
print("athena_max_date is:", athena_max_date)

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

# Bucket
secret_name = "BUCKET_NAMES"
region_name = "us-east-1"
secret_bucket_names = get_secret(secret_name, region_name)

jdbc_viamericas = f"jdbc:{secret['engine']}://{secret['host']}:{secret['port']};database={secret['dbname']}"

qryStr = f"(SELECT rtrim([ID_CITY_RECEIVER]) as ID_CITY_RECEIVER,[ID_FUNDS_OTHER_NAME],rtrim([ORDER_EXPIRED]) as ORDER_EXPIRED,[DATE_CANCEL],[typed_date],[FX_RATE_ORIGINATOR],[REBATE_AMOUNT],rtrim([FOREX_CALC]) as FOREX_CALC,rtrim([PIN_NUMBER]) as PIN_NUMBER,[FOREX_ESTIMATED_RATE],rtrim([ID_MODIFICATION_REASON]) as ID_MODIFICATION_REASON,[SOURCE_ORIGINAL_RATE],[SOURCE_TAXES],rtrim([ID_CURRENY]) as ID_CURRENY,[TIME_RECEIVER],[FOREX_FIRST_ESTIMATED],rtrim([MOD_PAY_CURRENCY]) as MOD_PAY_CURRENCY,rtrim([ID_FLAG_RECEIVER]) as ID_FLAG_RECEIVER,rtrim([BRANCH_PAY_RECEIVER]) as BRANCH_PAY_RECEIVER,[DATE_CANCELATION_REQ],[ID_FUND],rtrim([ID_STATE_RECEIVER]) as ID_STATE_RECEIVER,[SOURCE_TELEX_RECEIVER],[TELEX_RECEIVER],rtrim([STATUS_PAGO_PAYEE]) as STATUS_PAGO_PAYEE,[TOTAL_MODO_PAGO_COMP],[RECIPIENT_DATE_INSERTED],rtrim([ID_MAIN_BRANCH_EXPIRED]) as ID_MAIN_BRANCH_EXPIRED,[ID_CANCELATION_REQ],[EXPIRED_RATE],[FOREX_ESTIMATED],rtrim([BRANCH_NAME_CASHIER]) as BRANCH_NAME_CASHIER,rtrim([REC_PAYMENTTYPE]) as REC_PAYMENTTYPE,[COUPON_CODE],[DATE_MODIFICATION_REQ],[TOTAL_PAY_RECEIVER],[RATE_AT_INSERT],[SOURCE_EXCHANGE_RECEIVER],[TOTAL_RECEIVER],rtrim([ID_PAYMENT]) as ID_PAYMENT,[DATE_DEPOSIT],[SOURCE_FEE_AMOUNT],[STATE_TAX],rtrim([SOURCE]) as SOURCE,[ID_RECIPIENT],[ID_AD],[RATE_BASE_AT_INSERT],[MAINTENANCE_FEE],[REF_RECEIVER],[EXCHANGE_COMPANY],[FOREX_FIRST_ESTIMATED_RATE],rtrim([MODE_PAY_RECEIVER]) as MODE_PAY_RECEIVER,[SOURCE_EXCHANGE_COMPANY],[LAST_UPDATED],[RATE_CHANGE_RECEIVER],[TOTAL_MODO_PAGO],[ACCULINK_TRANID],[COMMISSION_PAYEE],[REFERAL_COMISSION_FIXED],rtrim([BANK_RECEIVER]) as BANK_RECEIVER,[DATE_RECEIVER],[FOREX_GAIN_RATE],[APPS],[EXCHANGE_RECEIVER],[BRANCH_PAY_RECEIVER_ORIGINAL],[DATE_EXPIRED],[NET_AMOUNT_RECEIVER],[PAYMENT_DATE],[AGENT_COMM_PROFIT],[TELEX_COMPANY],[ID_RECEIVER],[DEST_COUNTRY_TAX],rtrim([ID_CASHIER]) as ID_CASHIER,rtrim([RECOMEND_RECEIVER]) as RECOMEND_RECEIVER,[HANDLING_RECEIVER],rtrim([ID_BRANCH]) as ID_BRANCH,rtrim([ID_COUNTRY_RECEIVER]) as ID_COUNTRY_RECEIVER,[DATE_TRANS_PAYEE],[COMMISSION_PAYEE_PESOS],[FX_RECEIVER],[FOREX_GAIN],[PAYER_REFERENCENO],rtrim([ZIP_RECEIVER]) as ZIP_RECEIVER,[PORC_COMISION_RECEIVER],[SOURCE_FEE_RATE],[ID_MODIFICATION_REQ],[FX_RATE_CUSTOMER],[ORIGINAL_RATE],[RECEIVER_DATE_AVAILABLE],[FUNDING_FEE],[LOYALTY_RATE_COST],rtrim([TYPEID]) as TYPEID,[DISCOUNT],[REC_ACCROUTING],[FEE_RATE],[SOURCE_TOTAL_RECEIVER],rtrim([ID_MAIN_BRANCH_SENT]) as ID_MAIN_BRANCH_SENT,rtrim([acc_typeid]) as acc_typeid,[id_receiver_unique],[BRANCH_CASHIER],[ORIGINAL_FEE],[SOURCE_CURRENCY_AMOUNT],rtrim([REC_SENACCTYPE]) as REC_SENACCTYPE,[ID_FUNDS_NAME],[SOURCE_TELEX_COMPANY],[ID_SENDER],[ID_CURRENCY_SOURCE],[ORIGINATOR_BUYING_RATE],[REFERAL_COMISSION_PERCENTAGE],[CLAVE_RECEIVER],rtrim([STATUS_PAGO_AGENT]) as STATUS_PAGO_AGENT, convert(date, [LAST_UPDATED]) as day FROM envio.dba.receiver WHERE [LAST_UPDATED] >= '{athena_max_date}') x"


jdbcDF = spark.read.format('jdbc')\
    .option('url', jdbc_viamericas)\
    .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
    .option('dbtable', qryStr )\
    .option('user', secret['username'])\
    .option('password', secret['password'])\
    .option('partitionColumn', 'LAST_UPDATED')\
    .option('lowerBound', f'{athena_max_day} 00:00:00')\
    .option('upperBound', f'{athena_max_day} 23:59:59')\
    .option('numPartitions', 10)\
    .option('fetchsize', 1000)\
    .load()


jdbcDF.createOrReplaceTempView('jdbcDF')

jdbcDF.persist()

number_of_rows = jdbcDF.count()

print(f'number of rows obtained for date(s) higher than {athena_max_date}: {number_of_rows}')
if number_of_rows > 0:
    days_count = spark.sql(f'select day, count(*) as count from jdbcDF group by day')
    days_count.createOrReplaceTempView('days_count')
    thelist=[]
    for i in days_count.collect():
        pretup = (i[0],i[1])
        thelist.append(pretup)
    for day, count in thelist:
        if count > 0:
            #Conciliar para evitar posibles duplicados en la bajada
            dfincremental = jdbcDF.filter(col("day") == lit(day))
            totaldfpre = spark.sql(f" select t2.* from viamericas.receiver t2 where day = '{day}'")
            totaldfpre.unionByName(dfincremental).dropDuplicates()
  
            # Definir la ruta de salida en S3
            
            s3_output_path = f"s3://{secret_bucket_names['BUCKET_RAW']}/envio/dba/receiver/"
            # Escribir el DataFrame en formato Parquet en S3
            totaldfpre.write.partitionBy("day").parquet(s3_output_path, mode="overwrite")    
            print(f'Data for: {day} written succesfully')
            partition_creator_v2('viamericas','receiver', {'df': None, 'PartitionValues': tuple([str({day})])})
            abcdefg
else:
    print(f'No data for dates beyond: {athena_max_date}')
    