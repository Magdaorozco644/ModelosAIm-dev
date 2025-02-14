import json
import awswrangler as wr
import os
import json
import boto3
from botocore.exceptions import NoCredentialsError


# Constantes
BUCKET = "viamericas-datalake-dev-us-east-1-283731589572-glue-jobs"
PREFIX = "incremental/scripts"
ENV = 'dev'


def upload_file(body, database, schema, table):
    # Conectar con S3 usando boto3
    # Reemplaza 'tu-region' con tu región de S3
    s3 = boto3.client('s3', region_name='us-east-1')

    # Nombre del archivo en S3
    s3_key = f"{PREFIX}/gj{ENV}_incremental_{database}_{schema}_{table}_glue_script.py"

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


def build_script(database, schema, table, columns, update_field, partition_field):
    columns = list(set(columns))
    
    glue_script = f"""
import boto3, json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
import awswrangler as wr
from datetime import date
from botocore.exceptions import ClientError
from Codes.check_partitions import *
from concurrent.futures import ThreadPoolExecutor
from timeit import default_timer as timer

spark.sparkContext.setLogLevel('ERROR')
client = boto3.client('glue')

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")


def get_partitions(input_schema, input_table ):
		response = client.get_partitions(DatabaseName= input_schema, TableName= input_table)
		results = response['Partitions']
		while "NextToken" in response:
			response = client.get_partitions(DatabaseName= input_schema, TableName= input_table, NextToken=response["NextToken"])
			results.extend(response["Partitions"])
		partitionvalues = [tuple(x['Values']) for x in results]
		partitionvalues.sort(reverse=True)
		return partitionvalues


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


def thread_function(args):
    df, s3_output_path, date = args
    
    print(f"INFO --- writing data for date: {{date}}")
    
    #Conciliar para evitar posibles duplicados en la bajada
    dfincremental = df.filter(col("day") == lit(date))
            
    print(f'Number of records for day: {{date}} is : {{dfincremental.count()}}')
        
    # Escribir el DataFrame en formato Parquet en S3
    dfincremental.write.partitionBy("day").parquet(s3_output_path, mode="append")    
    print(f'Data for: {{date}} written succesfully')
    
    partition_creator_v2('viamericas','{table}', {{'df': None, 'PartitionValues': tuple([str(date)])}})


def main():
    
    max_date = get_partitions('viamericas','{table}')[0][0]
    print("Max date in partitions is:", max_date)

    # Get max date in athena
    df = wr.athena.read_sql_query(sql=f"select coalesce(cast(max({update_field}) as varchar), substring(cast(at_timezone(current_timestamp,'US/Eastern') as varchar(100)),1,23)) as max_date from viamericas.{table} where day = '{{max_date}}' and {update_field} <= cast(substring(cast(at_timezone(current_timestamp,'US/Eastern') as varchar(100)),1,23) as timestamp)", database="viamericas")

    athena_max_date = df['max_date'].tolist()[0]
    athena_max_day = athena_max_date[0:10]
    print("athena_max_date is:", athena_max_date)
    
    # Obtener credenciales para SQL Server
    secret_name = "SQLSERVER-CREDENTIALS"
    region_name = "us-east-1"
    secret = get_secret(secret_name, region_name)

    # Bucket
    secret_name = "BUCKET_NAMES"
    region_name = "us-east-1"
    secret_bucket_names = get_secret(secret_name, region_name)

    # Definir la ruta de salida en S3
    s3_output_path = f"s3://{{secret_bucket_names['BUCKET_RAW']}}/{database}/{schema}/{table}/"
    
    # SQLServer string connection
    jdbc_viamericas = f"jdbc:{{secret['engine']}}://{{secret['host']}}:{{secret['port']}};database={{secret['dbname']}}"
    
    qryStr = f"(SELECT {','.join(columns)}, convert(date, [{partition_field}]) as day FROM {database}.{schema}.{table} with(nolock) WHERE [{update_field}] >= '{{athena_max_date}}') x"
    
    start = timer()
    
    jdbcDF = spark.read.format('jdbc')\\
        .option('url', jdbc_viamericas)\\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\\
        .option('dbtable', qryStr )\\
        .option('user', secret['username'])\\
        .option('password', secret['password'])\\
        .option('fetchsize', 1000)\\
        .load()
    
    end = timer()
    
    print("Time taken to read data from database ",end-start)
    
    jdbcDF.createOrReplaceTempView('jdbcDF')

    jdbcDF.persist()

    number_of_rows = jdbcDF.count()

    print(f'number of rows obtained for date(s) higher than {{athena_max_date}}: {{number_of_rows}}')
    if number_of_rows > 0:
        days_count = spark.sql(f'select day, count(*) as count from jdbcDF group by day')
        days_count.createOrReplaceTempView('days_count')
        print('df preview')
        print(days_count.show())
        thelist=[]
        for i in days_count.collect():
            pretup = (i[0],i[1])
            thelist.append(pretup)
            
        start = timer()    
        with ThreadPoolExecutor(max_workers=18) as executor:
                futures = []
                
                for day, _ in thelist:
            
                    args = (jdbcDF, s3_output_path, day)
                    
                    # create threads
                    future = executor.submit(thread_function, args)
                    # append thread to the list of threads
                    futures.append(future)
                
                for i in range(len(futures)):
                    print(f"INFO --- running thread number: {{i + 1}}")
                    # execute threads
                    futures[i].result()  
        end = timer()
        
        print("Time taken with parallel execution is ",end-start)
    else:
        print(f'No data for dates beyond: {{athena_max_date}}')
        

if __name__ == "__main__":
    main()
    """
    
    return glue_script


def main():
    # Leer el archivo Excel
    excel_file = 's3://viamericas-datalake-dev-us-east-1-283731589572-raw/Datadictionarytotal.csv'
    update_file = 's3://viamericas-datalake-dev-us-east-1-283731589572-raw/Frecuencia de Extraccion_v2.csv'

    # Dictionary
    df = wr.s3.read_csv(excel_file)
    df = df[df['USE?'].isnull()]
    
    # Generating files just for all database except EnvioDW.
    #df = df[df['Database'] != 'EnvioDW']
    df = df[df['Table'] != 'CHECKREADER_SCORE']
    
    # Ignore sysname, tAppName, and tSessionId because they are duplicates of other columns
    df = df[df['Data_Type'] != 'sysname']
    df = df[df['Data_Type'] != 'tAppName']
    df = df[df['Data_Type'] != 'tSessionId']
    
    # Create script just for one table
    # df = df[df['Table'] == 'SF_SAFE_TRANSACTIONS'] 
    # df= df[df["AGREGAR"]=='SI']
    
    #print(df[['Database','Schema','Table']].head())
    
    #return

    df['Column'] = df.apply(
        lambda row: f"rtrim([{row['Column']}]) as {row['Column']}"
        if
        row['Data_Type'] == 'nchar'
        or row['Data_Type'] == 'nvarchar'
        or row['Data_Type'] == 'char'
        else f"[{row['Column']}]",
        axis=1
    )  # Add trim function to columns with nchar, nvarchar y char types.

    # df = df[df['Table'] == 'SF_SAFE_TRANSACTIONS']
    dict_df = df.groupby(["Database", "Schema", "Table"]).agg(list).reset_index()

    # Update frequency
    frequency = wr.s3.read_csv(update_file,encoding = "ISO-8859-1")
    
    
    #frequency= frequency[frequency["AGREGAR"]=='SI']
    
    #print(frequency[['Database','Schema','Table']].head())
    #return
    
    # frequency = frequency[frequency['Table'] == 'SF_SAFE_TRANSACTIONS']
    frequency = frequency[(frequency['USE?'] != 'Old/Not Used/Needed') & (frequency['extraction_load_type'] == 'incremental_load')]
    
    frequency['Campo de actualización'] = frequency['Campo de actualización'].fillna('')
    
    frequency['Important Columns'] = frequency['Important Columns'].fillna('')
    #frequency= frequency[frequency["AGREGAR"]=='SI']
    
    #print(frequency[['Database','Schema','Table']].head())
    #return
    
    jobs = []
    incremental_updates = [
        'checkreader_score', 'receiver', 'checktable', 'ml_fraud_score', 'audit_rate_group_agent', 'transaccion_diaria_payee', 'sender', 'fraud_vectors_v2_1',
        'transaccion_diaria_banco_payee', 'batchtable', 'history_inventory_market', 'historicalonholdrelease', 'accounting_journal', 'accounting_customerledger',
        'accounting_submittransaction', 'vcw_billpayment_sales', 
        'comision_agent_modo_pago_grupo',
        'forex_feed_market', 'vcw_moneyorders_sales', 'vcw_billpayment_viaone_sales', 'vcw_sales_products', 'vcw_states_pricing', 'receiver_gp_components', 'checkverification',
        'customers_customer', 'viacheckfeaturemetrics', # 'rate_group_agent', 
        'branch', 'returnchecks', 'sf_safe_transactions', 'audit_branch_status', 'history_balance', 'x9transactions'
    ]
    

    # Agrupar las columnas por tabla y construir el SELECT statement
    select_statements = {}
    jobs_str = ""


    # Getting fields will be works to get incremental data
    update_field = {}
    partition_field = {}
        
    for index, row in frequency.iterrows():
        table = row['Table'].lower()
        update_field_name = row['Campo de actualización'].strip()
        partition_field_name = row['Important Columns'].strip()
        
        update_field[table] = update_field_name  
        partition_field[table] = partition_field_name
        
    for index, row in dict_df.iterrows():
        database = row['Database'].lower()
        schema = row['Schema'].lower()
        table = row['Table'].lower()
        columns = row['Column']

        # Agregar la tabla y las columnas al diccionario
        if table not in select_statements:
            select_statements[table] = columns

        # create script
        if table in incremental_updates:
            glue_script = build_script(database, schema, table, columns, update_field[table], partition_field[table])

            # Crear un directorio para guardar los scripts si no existe
            output_directory = '/tmp'

            # Escribir el script en un archivo
            output_file = f"{output_directory}/gj{ENV}_incremental_{database}_{schema}_{table}_glue_script.py"
            with open(output_file, 'w') as f:
                f.write(glue_script)

            # Subir script a aws
            upload_file(glue_script, database, schema, table)

    print("Scripts de Glue generados exitosamente.")
    

def lambda_handler(event, context):
    main()
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
