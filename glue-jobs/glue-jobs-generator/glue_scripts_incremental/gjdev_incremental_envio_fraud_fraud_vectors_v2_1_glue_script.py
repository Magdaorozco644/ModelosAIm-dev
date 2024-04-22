
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

max_date = get_partitions('viamericas','fraud_vectors_v2_1')[0][0]
print("Max date in partitions is:", max_date)

# Get max date in athena

df = wr.athena.read_sql_query(sql=f"select coalesce(cast(max(DATE_CREATED) as varchar), substring(cast(at_timezone(current_timestamp,'America/Bogota') as varchar(100)),1,23)) as max_date from viamericas.fraud_vectors_v2_1 where day = '{max_date}' and DATE_CREATED <= cast(substring(cast(at_timezone(current_timestamp,'America/Bogota') as varchar(100)),1,23) as timestamp)", database="viamericas")



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

qryStr = f"(SELECT [ID_SALES_REPRESENTATIVE],[PAYOUT_N],[WEEKDAY_6],[IN_RANGE],[MODE_PAY_RECEIVER],[PAYOUT_5],[PAYOUT_D],[PAYOUT_M],[ID_BRANCH],[SENDER_SENDING_DAYS],[PAYOUT_0],[PAYOUT_C],[PAYOUT_2],[%TX_BRANCITY],[PAYOUT_7],[WEEKDAY_6.0],[WEEKDAY_3],[WEEKDAY_5],[BRANCH_WORKING_DAYS],[PAYOUT_X],[RECEIVER_TRANSACTION_COUNT],[HOUR_SENDER],[NET_AMOUNT_RECEIVER],[WEEKDAY_4],[SENDER_MINUTES_SINCE_LAST_TRANSACTION],[PAYOUT_4],[PAYOUT_1],[WEEKDAY_2.0],[ID_RECEIVER],[ID],[ADDRESS_RECEIVER_TRANSACTION_COUNT],[PAYOUT_3],[PAYOUT_O],[WEEKDAY_2],[WEEKDAY_1],[TRANSACTION_UNIQUE],[IDLOCATION_FRAUD],[SENDER_FRAUD],[PAYOUT_P],[WEEKDAY_5.0],[DATE_CREATED],[WEEKDAY_3.0],[TX_BRANCITY],[PAYOUT_T],[PAYOUT_S],[PAYOUT_6],[RECEIVER_FRAUD],[WEEKDAY_1.0],[WEEKDAY_4.0],[ID_COUNTRY_RECEIVER],[BRANCH_MINUTES_SINCE_LAST_TRANSACTION],[SENDER_DAYS_TO_LAST_TRANSACTION],[IDPAYER_FRAUD], convert(date, [DATE_CREATED]) as day FROM envio.fraud.fraud_vectors_v2_1 WHERE [DATE_CREATED] >= '{athena_max_date}') x"


jdbcDF = spark.read.format('jdbc')\
    .option('url', jdbc_viamericas)\
    .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
    .option('dbtable', qryStr )\
    .option('user', secret['username'])\
    .option('password', secret['password'])\
    .option('partitionColumn', 'DATE_CREATED')\
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
            totaldfpre = spark.sql(f" select t2.* from viamericas.fraud_vectors_v2_1 t2 where day = '{day}'")
            totaldfpre.unionByName(dfincremental).dropDuplicates()
  
            # Definir la ruta de salida en S3
            
            s3_output_path = f"s3://{secret_bucket_names['BUCKET_RAW']}/envio/fraud/fraud_vectors_v2_1/"
            # Escribir el DataFrame en formato Parquet en S3
            totaldfpre.write.partitionBy("day").parquet(s3_output_path, mode="overwrite")    
            print(f'Data for: {day} written succesfully')
            partition_creator_v2('viamericas','fraud_vectors_v2_1', {'df': None, 'PartitionValues': tuple([str({day})])})
            abcdefg
else:
    print(f'No data for dates beyond: {athena_max_date}')
    