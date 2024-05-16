import pandas as pd
import os
import json
import boto3
from botocore.exceptions import NoCredentialsError


# Constantes
BUCKET = "viamericas-datalake-dev-us-east-1-283731589572-glue-jobs"
PREFIX = "stage/intermediate"
ROLE = "GlueRolFullAccess"
ENV = 'dev'


def upload_file(body, table):
    # Conectar con S3 usando boto3
    # Reemplaza 'tu-region' con tu región de S3
    s3 = boto3.client('s3', region_name='us-east-1')

    # Nombre del archivo en S3
    s3_key = f"{PREFIX}/gj{ENV}_stage_intermediate_{table}_glue_script.py"

    try:
        # Subir el script a S3
        s3.put_object(
            Body=body,
            Bucket=BUCKET,
            Key=s3_key
        )
        # Reemplaza 'tu-bucket' con el nombre de tu bucket en S3
        print(
            f"Script guardado exitosamente en S3 en la ubicación: s3://{BUCKET}//{s3_key}")
    except NoCredentialsError:
        print("No se encontraron credenciales de AWS. Por favor, configura tus credenciales de AWS.")
    except Exception as e:
        print(f"Ocurrió un error al subir el script a S3: {str(e)}")


def build_script(table, metadata):
    
    glue_script = f"""
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
    
    print(f"INFO --- reading data for date: {{date}}")
    
    # Reading data from the database
    totaldfpre = spark.sql(query)
    
    print(f'Number of records for day: {{date}} is : {{totaldfpre.count()}}')

    # Escribir el DataFrame en formato Parquet en S3
    totaldfpre.write.partitionBy("day").parquet(s3_output_path, mode="overwrite") 
    
    print(f'Data for: {{date}} written succesfully')
    
    partition_creator_v2('stage','intermediate_{table}', {{'df': None, 'PartitionValues': tuple([str(date)])}})


def main(day):
    crawler_to_run = 'crw_stage_intermediate_{table}'
    
    # Obtener nombre de buckets
    secret_name = "BUCKET_NAMES"
    region_name = "us-east-1"
    secret_bucket_names = get_secret(secret_name, region_name)  
    
    
    # Definir la ruta de salida en S3
    s3_output_path = f"s3://{{secret_bucket_names['BUCKET_STAGE']}}/intermediate_{table}/"
    
    is_table_in_catalog = table_exists('stage', 'intermediate_{table}')

    if is_table_in_catalog:
        # Get date n months ago
        # date_before = day - timedelta(days=days_before)
        
        # print(f'{table} table exists. We will only update records between {{date_before}} and {{day}}')
        max_partition = get_partitions('stage','intermediate_{table}')[0][0]
        
        print("Max date in partitions is:", max_partition)
        
        # Get max date in athena
        df = wr.athena.read_sql_query(
            sql=f"select coalesce(cast(max({metadata['order_col']}) as varchar), substring(cast(at_timezone(current_timestamp,'US/Eastern') as varchar(100)),1,23)) as max_date from stage.intermediate_{table} where day = '{{max_partition}}' and {metadata['order_col']} <= cast(substring(cast(at_timezone(current_timestamp,'US/Eastern') as varchar(100)),1,23) as timestamp)", 
            database="stage"
        )

        athena_max_date = df['max_date'].tolist()[0]
        athena_max_day = athena_max_date[0:10]
        print("athena_max_date is:", athena_max_date)
        
        #print(f'min date in the batch with new data is: {{min_date.collect()[0][0]}}')
        
        # Get just the days from the new rows added
        query_jdbcDF = f"select * from viamericas.{table} {table} where {metadata['order_col']} >= '{{athena_max_date}}'"
        
        jdbcDF = spark.sql(query_jdbcDF)
    
        number_of_rows = jdbcDF.count()
        
        print(f'The number of rows for day {{max_partition}} is: {{number_of_rows}}')
        
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
                    
                    query = f"select `(rank)?+.+` from (select {table}.*, row_number() over(partition by {metadata['partition_col']} order by COALESCE({metadata['order_col']}, '1900-01-01 00:00:00') desc) as rank from viamericas.{table} {table} where day = '{{partition}}' ) where rank=1"
                    
                    args = (query, s3_output_path, partition)
                    
                    # create threads
                    future = executor.submit(thread_function, args)
                    # append thread to the list of threads
                    futures.append(future)
                
                for i in range(len(futures)):
                    print(f"INFO --- running thread number: {{i + 1}}")
                    # execute threads
                    futures[i].result()        

        print("Ended writing")
    else:
        print(f'table {table} does not exists. We will update the whole {table}\\'s history')
        
        query_jdbcDF = f"select `(rank)?+.+` from (select {table}.*, row_number() over(partition by {metadata['partition_col']} order by COALESCE({metadata['order_col']}, '1900-01-01 00:00:00') desc ) as rank from  viamericas.{table} {table} where day <= '{{day}}') where rank=1"
        
        jdbcDF = spark.sql(query_jdbcDF)
        
        print(f'total_rows: {{jdbcDF.count()}}')

        # Escribir el DataFrame en formato Parquet en S3
        jdbcDF.write.partitionBy("day").parquet(s3_output_path, mode="overwrite")
        print("Ended writing")
        
        wait_for_crawler_completion(crawler_name=crawler_to_run)
    
    
if __name__ == "__main__":
    today = datetime.today().date()
    main(today)
    """
    
    return glue_script


def main():
    tables_metadata = {
        'receiver': {
            'order_col': 'LAST_UPDATED',
            'date_col': 'DATE_RECEIVER',
            'partition_col': 'ID_BRANCH, ID_RECEIVER'
        },
        'checktable': {
            'order_col': 'LAST_UPDATED',
            'date_col': 'CheckDate',
            'partition_col': 'checkID'
        },
        'sender': {
            'order_col': 'LAST_UPDATED',
            'date_col': 'INSERTED_DATE',
            'partition_col': 'ID_BRANCH, ID_SENDER'
        },
        'accounting_submittransaction': {
            'order_col': 'MODIFICATION_DATE',
            'date_col': 'CREATION_DATE',
            'partition_col': 'id'
        },
        'vcw_billpayment_sales': {
            'order_col': 'LAST_UPDATED',
            'date_col': 'TRANSACTION_DATE',
            'partition_col': 'ID_BILLPAYMENT_SALES'
        },
        'comision_agent_modo_pago_grupo': {
            'order_col': 'LAST_UPDATED',
            'date_col': 'LAST_UPDATED',
            'partition_col': 'ID_BRANCH, ID_MAIN_BRANCH'
        },
        'vcw_moneyorders_sales': {
            'order_col': 'LAST_UPDATED',
            'date_col': 'TRANSACTION_DATE',
            'partition_col': 'ID_MONEYORDERS_SALES'
        },
        'vcw_sales_products': {
            'order_col': 'LAST_UPDATED',
            'date_col': 'DATETRANSACTION',
            'partition_col': 'SALESID'
        },
        'branch': {
            'order_col': 'LAST_UPDATED',
            'date_col': 'DATE_INSERTED',
            'partition_col': 'ID_BRANCH'
        },
        'sf_safe_transactions': {
            'order_col': 'TRANSACTION_DATE',
            'date_col': 'TRANSACTION_DATE',
            'partition_col': 'ID_SAFE_TRANSACTIONS'
        }
    }

    for table, metadata in tables_metadata.items():
        # create script
        glue_script = build_script(table, metadata)

        # Crear un directorio para guardar los scripts si no existe
        output_directory = 'glue_scripts_stages'
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # Escribir el script en un archivo
        output_file = f"{output_directory}/gj{ENV}_stage_{table}_glue_script.py"
        with open(output_file, 'w') as f:
            f.write(glue_script)

        # Subir script a aws
        upload_file(glue_script, table)


        print("Archivo jobs.json generado exitosamente.")
        print("Scripts de Glue generados exitosamente.")


if __name__ == "__main__":
    main()
