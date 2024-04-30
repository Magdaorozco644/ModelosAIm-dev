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
from pyspark.sql.functions import col, current_date, date_format
from datetime import datetime
from botocore.exceptions import ClientError


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
       
    
def main(day):
    
    crawler_to_run = 'crw_stage_intermediate_{table}'
    
    # Obtener nombre de buckets
    secret_name = "BUCKET_NAMES"
    region_name = "us-east-1"
    secret_bucket_names = get_secret(secret_name, region_name)   

    globals()["query_"+str(day)] = f"select `(rank|day)?+.+`, '{{day}}' as day from (select {table}.*, row_number() over(partition by {metadata['partition_col']} order by COALESCE({metadata['order_col']}, '1900-01-01 00:00:00') desc ) as rank from viamericas.{table} {table} where day <= '{{day}}' ) where rank=1"
    
    globals()["df_"+str(day)] = spark.sql(globals()["query_"+str(day)])
    

    # Definir la ruta de salida en S3
    s3_output_path = f"s3://{{secret_bucket_names['BUCKET_STAGE']}}/intermediate_{table}/"

    # Escribir el DataFrame en formato Parquet en S3
    globals()["df_"+str(day)].write.partitionBy("day").parquet(s3_output_path, mode="overwrite")
    
    is_table_in_catalog = table_exists('stage', 'intermediate_{table}')

    if is_table_in_catalog:
    	partition_creator_v2('stage','intermediate_{table}', {{'df': None, 'PartitionValues': tuple([str(day)])}})
    else:
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
            'partition_col': 'ID_BRANCH, ID_RECEIVER'
        },
        'checktable': {
            'order_col': 'LAST_UPDATED',
            'partition_col': 'checkID'
        },
        'sender': {
            'order_col': 'LAST_UPDATED',
            'partition_col': 'ID_BRANCH, ID_SENDER'
        },
        'accounting_submittransaction': {
            'order_col': 'MODIFICATION_DATE',
            'partition_col': 'id'
        },
        'vcw_billpayment_sales': {
            'order_col': 'LAST_UPDATED',
            'partition_col': 'ID_BILLPAYMENT_SALES'
        },
        'comision_agent_modo_pago_grupo': {
            'order_col': 'LAST_UPDATED',
            'partition_col': 'ID_BRANCH, ID_MAIN_BRANCH'
        },
        'vcw_moneyorders_sales': {
            'order_col': 'LAST_UPDATED',
            'partition_col': 'ID_MONEYORDERS_SALES'
        },
        'vcw_sales_products': {
            'order_col': 'LAST_UPDATED',
            'partition_col': 'SALESID'
        },
        'branch': {
            'order_col': 'LAST_UPDATED',
            'partition_col': 'ID_BRANCH'
        },
        'sf_safe_transactions': {
            'order_col': 'TRANSACTION_DATE',
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
