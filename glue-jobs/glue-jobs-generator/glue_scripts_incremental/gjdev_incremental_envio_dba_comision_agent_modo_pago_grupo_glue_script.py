
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

max_date = get_partitions('viamericas','comision_agent_modo_pago_grupo')[0][0]
print("Max date in partitions is:", max_date)

# Get max date in athena

df = wr.athena.read_sql_query(sql=f"select coalesce(cast(max(LAST_UPDATED) as varchar), substring(cast(at_timezone(current_timestamp,'America/Bogota') as varchar(100)),1,23)) as max_date from viamericas.comision_agent_modo_pago_grupo where day = '{max_date}' and LAST_UPDATED <= cast(substring(cast(at_timezone(current_timestamp,'America/Bogota') as varchar(100)),1,23) as timestamp)", database="viamericas")



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

qryStr = f"(SELECT rtrim([ID_COUNTRY]) as ID_COUNTRY,[LAST_UPDATED],[RANGE_BEGIN_COMISION_PAY],[FEET_COMPANY_COMM_B],[PORCENT_CUSTOMER_COMM_B],[RANGE_END_COMISION_PAY],rtrim([ID_BRANCH]) as ID_BRANCH,[PORCENT_COMPANY_COMM_B],[FEET_COMISION_COMP],[FEET_COMISION_AGENTE],rtrim([ID_MAIN_BRANCH]) as ID_MAIN_BRANCH,[FEET_CUSTOMER_COMM_B],rtrim([MOD_PAY_CURRENCY]) as MOD_PAY_CURRENCY,rtrim([ID_MODO_PAGO]) as ID_MODO_PAGO,[ID_COUNTRY_SOURCE],[PORCENT_COMISION_COMP],[PORCENT_COMISION_AGENTE],[ID_CURRENCY_SOURCE], convert(date, [LAST_UPDATED]) as day FROM envio.dba.comision_agent_modo_pago_grupo WHERE [LAST_UPDATED] >= '{athena_max_date}') x"


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
            totaldfpre = spark.sql(f" select t2.* from viamericas.comision_agent_modo_pago_grupo t2 where day = '{day}'")
            totaldfpre.unionByName(dfincremental).dropDuplicates()
  
            # Definir la ruta de salida en S3
            
            s3_output_path = f"s3://{secret_bucket_names['BUCKET_RAW']}/envio/dba/comision_agent_modo_pago_grupo/"
            # Escribir el DataFrame en formato Parquet en S3
            totaldfpre.write.partitionBy("day").parquet(s3_output_path, mode="overwrite")    
            print(f'Data for: {day} written succesfully')
            partition_creator_v2('viamericas','comision_agent_modo_pago_grupo', {'df': None, 'PartitionValues': tuple([str({day})])})
            abcdefg
else:
    print(f'No data for dates beyond: {athena_max_date}')
    