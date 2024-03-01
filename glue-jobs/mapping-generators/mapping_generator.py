import pandas as pd
import os
import json
import boto3
from botocore.exceptions import NoCredentialsError

# Leer el archivo Excel
excel_file = r'Data Dictionary Total.csv'
BUCKETRAW = 'viamericas-datalake-dev-us-east-1-283731589572-raw'
BUCKETSCRIPTS = "viamericas-datalake-dev-us-east-1-283731589572-glue-jobs/bulk/scripts"

df = pd.read_csv(excel_file)
df = df[df['USE?'].isnull()]
df = df.groupby(["Database", "Schema", "Table"]).agg(list).reset_index()

incremental_updates = [
    'checkreader_score', 'ml_fraud_score', 
    'audit_rate_group_agent', 'transaccion_diaria_payee',
    'fraud_vectors_v2_1', 'history_inventory_market',
    'historicalonholdrelease', 'forex_feed_market',
    'receiver_gp_components', 'checkverification',
    'viacheckfeaturemetrics', 'returnchecks'
]

mappers = []
all = ""
for index, row in df.iterrows():
    database = row['Database'].lower()
    schema = row['Schema'].lower()
    table = row['Table'].lower()
    columns = row['Column']
    state_machine_name = f'sdlf_sm_bulk_{database}_{schema}_{table}' 

    # path raw files and path scripts
    path_files =  f's3://{BUCKETRAW}/{database}/{schema}/{table}'
    path_script = f"s3://{BUCKETSCRIPTS}/gjdev_bulk_{database}_{schema}_{table}_glue_script.py"
        
    # names
    job_name = f'sdlf_gj_bulk_{database}_{schema}_{table}'
    crawler_name = f'sdlf_crw_{database}_{schema}_{table}'
    state_machine_name = f'sdlf_sm_bulk_{database}_{schema}_{table}'

    database = database.capitalize()
    schema = "".join(word.capitalize() for word in schema.split("_"))
    table = "".join(word.capitalize() for word in table.split("_"))
            
    mapping = {
        'ObjectName': f'{database}{schema}{table}',
        'JobName': job_name,
        'ScriptLocation': path_script,
        'CrawlerName': crawler_name,
        'Path': path_files,
        'StateMachineName': state_machine_name
    }
        
    mappers.append(mapping)
            
    all +=  f'{database}{schema}{table}, '
    
rest = "" 
for mapping in mappers:
    rest += mapping['StateMachineName'] + ", "
    mapping  = f'{mapping["ObjectName"]}:\n  JobName: \"{mapping["JobName"]}\"\n  ScriptLocation: \"{mapping["ScriptLocation"]}\"\n  CrawlerName: \"{mapping["CrawlerName"]}\"\n  Path: \"{mapping["Path"]}\"\n  StateMachineName: \"{mapping["StateMachineName"]}\"\n'     
    
    with open('crw.txt', 'a') as crw_file:
        crw_file.write(mapping)
        
    
print(all)
# print(rest)