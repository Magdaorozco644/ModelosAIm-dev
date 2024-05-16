
import boto3, json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from Codes.check_partitions import *
import awswrangler as wr
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.sql.parser.quotedRegexColumnNames","true")


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
      

def get_partitions(input_schema, input_table):
    client = boto3.client('glue')
    
    response = client.get_partitions(DatabaseName = input_schema, TableName = input_table)
    
    results = response['Partitions']
    while "NextToken" in response:
        response = client.get_partitions(DatabaseName= input_schema, TableName= input_table, NextToken=response["NextToken"])
        results.extend(response["Partitions"])
        
    partitionvalues = [tuple(x['Values']) for x in results]
    partitionvalues.sort(reverse=True)
    
    return partitionvalues
    
    
def thread_function(args):
    query, s3_output_path, date = args
    
    print(f"INFO --- reading data for date: {date}")
    
    # Reading data from the database
    totaldfpre = spark.sql(query)
    
    print(f'Number of records for day: {date} is : {totaldfpre.count()}')

    # Escribir el DataFrame en formato Parquet en S3
    totaldfpre.write.partitionBy("day").parquet(s3_output_path, mode="overwrite") 
    
    print(f'Data for: {date} written succesfully')
    
    partition_creator_v2('stage','intermediate_vcw_moneyorders_sales', {'df': None, 'PartitionValues': tuple([str(date)])})


def main(day):
    crawler_to_run = 'crw_stage_intermediate_vcw_moneyorders_sales'
    
    # Obtener nombre de buckets
    secret_name = "BUCKET_NAMES"
    region_name = "us-east-1"
    secret_bucket_names = get_secret(secret_name, region_name)  
    
    
    # Definir la ruta de salida en S3
    s3_output_path = f"s3://{secret_bucket_names['BUCKET_STAGE']}/intermediate_vcw_moneyorders_sales/"
    
    is_table_in_catalog = table_exists('stage', 'intermediate_vcw_moneyorders_sales')

    if is_table_in_catalog:
        # Get date n months ago
        # date_before = day - timedelta(days=days_before)
        
        # print(f'vcw_moneyorders_sales table exists. We will only update records between {date_before} and {day}')
        max_partition = get_partitions('stage','intermediate_vcw_moneyorders_sales')[0][0]
        
        print("Max date in partitions is:", max_partition)
        
        # Get max date in athena
        df = wr.athena.read_sql_query(
            sql=f"select coalesce(cast(max(LAST_UPDATED) as varchar), substring(cast(at_timezone(current_timestamp,'US/Eastern') as varchar(100)),1,23)) as max_date from stage.intermediate_vcw_moneyorders_sales where day = '{max_partition}' and LAST_UPDATED <= cast(substring(cast(at_timezone(current_timestamp,'US/Eastern') as varchar(100)),1,23) as timestamp)", 
            database="stage"
        )

        athena_max_date = df['max_date'].tolist()[0]
        athena_max_day = athena_max_date[0:10]
        print("athena_max_date is:", athena_max_date)
        
        #print(f'min date in the batch with new data is: {min_date.collect()[0][0]}')
        
        # Get just the days from the new rows added
        query_jdbcDF = f"select * from viamericas.vcw_moneyorders_sales vcw_moneyorders_sales where LAST_UPDATED >= '{athena_max_date}'"
        
        jdbcDF = spark.sql(query_jdbcDF)
    
        number_of_rows = jdbcDF.count()
        
        print(f'The number of rows for day {max_partition} is: {number_of_rows}')
        
        jdbcDF.createOrReplaceTempView(f"jdbcDF")
        
        if number_of_rows > 0:
            
            days_count = spark.sql(f"select distinct day from jdbcDF")
            days_count.createOrReplaceTempView('days_count')
            thelist=[]
            for i in days_count.collect():
                thelist.append(i[0])
            print(thelist)
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                
                for partition in thelist:
                    
                    query = f"select `(rank)?+.+` from (select vcw_moneyorders_sales.*, row_number() over(partition by ID_MONEYORDERS_SALES order by COALESCE(LAST_UPDATED, '1900-01-01 00:00:00') desc) as rank from viamericas.vcw_moneyorders_sales vcw_moneyorders_sales where day = '{partition}' ) where rank=1"
                    
                    args = (query, s3_output_path, partition)
                    
                    # create threads
                    future = executor.submit(thread_function, args)
                    # append thread to the list of threads
                    futures.append(future)
                
                for i in range(len(futures)):
                    print(f"INFO --- running thread number: {i + 1}")
                    # execute threads
                    futures[i].result()        

        print("Ended writing")
    else:
        print(f'table vcw_moneyorders_sales does not exists. We will update the whole vcw_moneyorders_sales\'s history')
        
        query_jdbcDF = f"select `(rank)?+.+` from (select vcw_moneyorders_sales.*, row_number() over(partition by ID_MONEYORDERS_SALES order by COALESCE(LAST_UPDATED, '1900-01-01 00:00:00') desc ) as rank from  viamericas.vcw_moneyorders_sales vcw_moneyorders_sales where day <= '{day}') where rank=1"
        
        jdbcDF = spark.sql(query_jdbcDF)
        
        print(f'total_rows: {jdbcDF.count()}')

        # Escribir el DataFrame en formato Parquet en S3
        jdbcDF.write.partitionBy("day").parquet(s3_output_path, mode="overwrite")
        print("Ended writing")
        
        wait_for_crawler_completion(crawler_name=crawler_to_run)
    
    
if __name__ == "__main__":
    today = datetime.today().date()
    main(today)
    