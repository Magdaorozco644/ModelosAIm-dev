import pandas as pd
import os
import json
import boto3
from botocore.exceptions import NoCredentialsError


# Constantes
BUCKET = "viamericas-datalake-dev-us-east-1-283731589572-glue-jobs"
PREFIX = "incremental/scripts"
ROLE = "GlueRolFullAccess"
ALLOCATED_CAPACITY = 10

# Leer el archivo Excel
excel_file = r'Data Dictionary Total.csv'
update_file = r'Frecuencia de Extraccion.csv'

# Dictionary
df = pd.read_csv(excel_file)
df = df[df['USE?'].isnull()]
df = df[df['Table'] == 'SF_SAFE_TRANSACTIONS']
dict_df = df.groupby(["Database", "Schema", "Table"]).agg(list).reset_index()

# Update frequency
frequency = pd.read_csv(update_file)
frequency = frequency[frequency['Table'] == 'SF_SAFE_TRANSACTIONS']
frequency = frequency[(frequency['USE?'] != 'Old/Not Used/Needed') & (frequency['extraction_load_type'] == 'incremental_load')]

print(frequency)

jobs = []
# incremental_updates = [
#     'checkreader_score', 'receiver', 'checktable', 'ml_fraud_score', 'audit_rate_group_agent', 'transaccion_diaria_payee', 'sender', 'fraud_vectors_v2_1', 'transaccion_diaria_banco_payee', 'batchtable', 'history_inventory_market', 'historicalonholdrelease', 'accounting_journal', 'accounting_customerledger',
#     'accounting_submittransaction', 'vcw_billpayment_sales', 
#     'comision_agent_modo_pago_grupo',
#     'forex_feed_market', 'vcw_moneyorders_sales', 'vcw_billpayment_viaone_sales', 'vcw_sales_products', 'vcw_states_pricing', 'receiver_gp_components', 'checkverification', 'customers_customer', 'viacheckfeaturemetrics', 'rate_group_agent', 'branch', 'returnchecks', 'sf_safe_transactions'
# ]
incremental_updates = [
    'sf_safe_transactions'
]

def upload_file(body, database, schema, table):
    # Conectar con S3 usando boto3
    # Reemplaza 'tu-region' con tu región de S3
    s3 = boto3.client('s3', region_name='us-east-1')

    # Nombre del archivo en S3
    s3_key = f"{PREFIX}/gjdev_incremental_{database}_{schema}_{table}_glue_script.py"

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


def build_script(database, schema, table, columns, update_field):
    columns = list(set(columns))
    columns = list(map(lambda x: '[' + x + ']', columns))

    
    glue_script = f"""
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
df = wr.athena.read_sql_query(sql="select coalesce(cast(max({update_field}) as varchar), cast(date_add('hour', -5, from_unixtime(cast(to_unixtime(current_timestamp) AS bigint))) as varchar)) as max_date from viamericas.{table}", database="viamericas")

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
qryStr = f"(SELECT {','.join(columns)}, convert(date, [{update_field}]) as day FROM {database}.{schema}.{table} WHERE convert(date, [{update_field}]) >= '{{today}}') x"

jdbcDF = spark.read.format('jdbc')\\
        .option('url', jdbc_viamericas)\\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\\
        .option('dbtable', qryStr )\\
        .option("user", secret['username'])\\
        .option("password", secret['password'])\\
        .option('partitionColumn', '{update_field}')\\
        .option("lowerBound", f'{{today}} 00:00:00')\\
        .option("upperBound", f'{{today}} 23:59:59')\\
        .option("numPartitions", 10)\\
        .option("fetchsize", 1000)\\
        .load()

number_of_rows = jdbcDF.count()

print(f'number of rows obtained for date higher than {{today}}: {{number_of_rows}}')

# jdbcDF = jdbcDF.withColumn('day', date_format('{update_field}', 'yyyy-MM-dd'))

if number_of_rows > 0: 
    # Definir la ruta de salida en S3
    s3_output_path = f"s3://viamericas-datalake-dev-us-east-1-283731589572-raw/{database}/{schema}/{table}/"

    # Escribir el DataFrame en formato Parquet en S3
    jdbcDF.write.partitionBy("day").parquet(s3_output_path, mode="overwrite")
    
    print(f'Data for the day: {{today}} written succesfully')
else:
    print(f'No data for the date: {{today}}')
    """

    return glue_script


# Agrupar las columnas por tabla y construir el SELECT statement
select_statements = {}
jobs_str = ""


# Getting fields will be works to get incremental data
update_field = {}
    
for index, row in frequency.iterrows():
    table = row['Table'].lower()
    update_field_name = row['Campo de actualización'].strip()
        
    update_field[table] = update_field_name  

print(update_field)

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
        glue_script = build_script(database, schema, table, columns, update_field[table])

        # Crear un directorio para guardar los scripts si no existe
        output_directory = 'glue_scripts'
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # Escribir el script en un archivo
        output_file = f"{output_directory}/{database}_{schema}_{table}_incremental_glue_script.py"
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
