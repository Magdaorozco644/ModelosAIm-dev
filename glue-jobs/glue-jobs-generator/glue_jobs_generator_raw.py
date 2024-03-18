import pandas as pd
import os
import json
import boto3
from botocore.exceptions import NoCredentialsError


# Constantes
BUCKET = "viamericas-datalake-dev-us-east-1-283731589572-glue-jobs"
PREFIX = "bulk/scripts"
ROLE = "GlueRolFullAccess"
ALLOCATED_CAPACITY = 10

# Leer el archivo Excel
excel_file = r'Data Dictionary Total.csv'

df = pd.read_csv(excel_file)
df = df[df['USE?'].isnull()]
df = df.groupby(["Database", "Schema", "Table"]).agg(list).reset_index()

jobs = []


def upload_file(body, database, schema, table):
    # Conectar con S3 usando boto3
    # Reemplaza 'tu-region' con tu región de S3
    s3 = boto3.client('s3', region_name='us-east-1')

    # Nombre del archivo en S3
    s3_key = f"{PREFIX}/gjdev_bulk_{database}_{schema}_{table}_glue_script.py"

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


def build_script(database, schema, table, columns):
    columns = list(set(columns))
    columns = list(map(lambda x: '[' + x + ']', columns))

    update_field = {
        'checkreader_score': 'system_date', 
        'receiver': 'DATE_RECEIVER',
        'ml_fraud_score': 'DATE_CREATED', 
        'audit_rate_group_agent': 'DATE_PROCESS', 
        'transaccion_diaria_payee': 'DATE_TRANS_DIARIA', 
        'fraud_vectors_v2_1': 'DATE_CREATED', #
        'history_inventory_market': 'DATE', 
        'historicalonholdrelease': 'processeddate',
        'accounting_journal': 'CREATION_DATE',
        'accounting_customerledger': 'CREATION_DATE',
        'accounting_submittransaction': 'MODIFICATION_DATE',
        'forex_feed_market': 'FEED_DATE', 
        'receiver_gp_components': 'DATE_RECEIVER', 
        'checkverification': 'VERIFICATIONDATE', 
        'viacheckfeaturemetrics': 'MetricDate', 
        'returnchecks': 'insertedDate', 
        'transaccion_diaria_banco_payee': 'DATE_TRANS_DIARIA',
        'customers_customer': 'enrollment_date',
        'vcw_sales_products' : 'DATETRANSACTION',
        'vcw_billpayment_sales': 'TRANSACTION_DATE',
        'batchtable': 'created'
    }

    tables_update_field = [*update_field.keys()]

    if table in tables_update_field:
        glue_script = f"""
import boto3, json, sys
from awsglue.context import GlueContext
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, date_format
from datetime import datetime, date
from awsglue.utils import getResolvedOptions

# Contexto
sc = SparkContext()
spark = SparkSession(sc)


spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")


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
    query, secret, jdbc_viamericas, date = args
    
    # Obtener nombre de buckets
    secret_name = "BUCKET_NAMES"
    region_name = "us-east-1"
    secret_bucket_names = get_secret(secret_name, region_name)
    
    print(f"INFO --- reading data for date: {{date}}")
    # Reading data from the database
    jdbcDF = spark.read.format('jdbc')\\
        .option('url', jdbc_viamericas)\\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\\
        .option('dbtable', query )\\
        .option("user", secret['username'])\\
        .option("password", secret['password'])\\
        .option("numPartitions", 10)\\
        .option("fetchsize", 1000)\\
        .load()
    print(f"INFO --- number of rows for date: {{date}}: {{jdbcDF.count()}} ")
    jdbcDF = jdbcDF.withColumn('day', date_format('{update_field[table]}', 'yyyy-MM-dd'))
    print(f"INFO --- variable 'day' for date: {{date}} obtained")
    # Definir la ruta de salida en S3

    s3_output_path = f"s3://{{secret_bucket_names['BUCKET_RAW']}}/{database}/{schema}/{table}/"
    
    print(f"INFO --- writing into s3 bucket: {{s3_output_path}} data for date: {{date}}")
    
    # Escribir el DataFrame en formato Parquet en S3
    jdbcDF.write.partitionBy("day").parquet(s3_output_path, mode="overwrite")
    
    print(f"INFO --- data for date: {{date}} written successfully")


# Obtener credenciales para SQL Server
secret_name = "SQLSERVER-CREDENTIALS"
region_name = "us-east-1"
secret = get_secret(secret_name, region_name)

jdbc_viamericas = f"jdbc:{{secret['engine']}}://{{secret['host']}}:{{secret['port']}};database={{secret['dbname']}}"

def main(dates):
    # creating pool threads
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for date in dates:
            qryStr = f"(SELECT {" ,".join(columns)} FROM {database}.{schema}.{table} WHERE {update_field[table]} >= '{{date}}-01-01 00:00:00.000' AND {update_field[table]} <= '{{date}}-12-31 23:59:59.000') x"
            args = (qryStr, secret, jdbc_viamericas, date)
            # create threads
            future = executor.submit(thread_function, args)
            # append thread to the list of threads
            futures.append(future)
        for i in range(len(futures)):
            print(f"INFO --- running thread number: {{i + 1}}")
            # execute threads
            futures[i].result()
            
if __name__ == "__main__":
    # Get arguments 
    # initial_year = initial year from where we are going to extract info
    # final_year = year until which we are going to extract data
    if ('--initial_year' in sys.argv):
        args = getResolvedOptions(sys.argv, ['initial_year'])
    else:
        args = {{ 'initial_year' : '2005' }}
        
    initial_year = args['initial_year']


    if ('--final_year' in sys.argv):
        args = getResolvedOptions(sys.argv, ['final_year'])
    else:
        # If no final year is provided, we take the current year
        today = date.today()
        year = today.year
        
        args = {{ 'final_year': year }}
        
    final_year = args['final_year']
    
    
    dates = []
    for year in range(int(initial_year), int(final_year)+1):
        dates.append(str(year))

    main(dates)
    """
    else:
        glue_script = f"""
import boto3, json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date

# Contexto
sc = SparkContext()
spark = SparkSession(sc)
glueContext = GlueContext(spark)


spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

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

# Obtener nombre de buckets
secret_name = "BUCKET_NAMES"
region_name = "us-east-1"
secret_bucket_names = get_secret(secret_name, region_name)

jdbc_viamericas = "jdbc:{{secret['engine']}}://{{secret['host']}}:{{secret['port']}};database={{secret['dbname']}}"
qryStr = f"(SELECT {" ,".join(columns)} FROM {database}.{schema}.{table}) x"

jdbcDF = spark.read.format('jdbc')\\
        .option('url', jdbc_viamericas)\\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\\
        .option('dbtable', qryStr )\\
        .option("user", secret['username'])\\
        .option("password", secret['password'])\\
        .option("numPartitions", 10)\\
        .option("fetchsize", 1000)\\
        .load()

# Definir la ruta de salida en S3
s3_output_path = f"s3://{{secret_bucket_names['BUCKET_RAW']}}/{database}/{schema}/{table}/"

# Escribir el DataFrame en formato Parquet en S3
jdbcDF.write.parquet(s3_output_path, mode="overwrite")
    """
    return glue_script


# Agrupar las columnas por tabla y construir el SELECT statement
select_statements = {}
jobs_str = ""

for index, row in df.iterrows():
    database = row['Database'].lower()
    schema = row['Schema'].lower()
    table = row['Table'].lower()
    columns = row['Column']

    # Agregar la tabla y las columnas al diccionario
    if table not in select_statements:
        select_statements[table] = columns

    # create script
    glue_script = build_script(database, schema, table, columns)

    # Crear un directorio para guardar los scripts si no existe
    output_directory = 'glue_scripts'
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Escribir el script en un archivo
    output_file = f"{output_directory}/{database}_{schema}_{table}_glue_script.py"
    with open(output_file, 'w') as f:
        f.write(glue_script)

    # Subir script a aws
    upload_file(glue_script, database, schema, table)

print(jobs_str)
# Escribir el listado de trabajos en un archivo JSON
with open('jobs.json', 'w') as json_file:
    jobs_json = json.dumps(jobs, indent=4)
    json_file.write(jobs_json)

print("Archivo jobs.json generado exitosamente.")
print("Scripts de Glue generados exitosamente.")
