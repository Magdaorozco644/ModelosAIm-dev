
# Send data to Redshift V2
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

path = 's3://viamericas-datalake-dev-us-east-1-283731589572-analytics/abt_parquet/dt=2024-05-08/5830aff9a4c94cd6b88641d29d3ccd36.snappy.parquet'
schema_table = "viamericas.test_table_v2"
database = "dev"
temp_s3 = "s3://viamericas-datalake-dev-us-east-1-283731589572-analytics/glue_abt/raw/tmp_dir/"

# read from s3
source_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "path": path
    },
    format="parquet",
)

source_data_df = source_data.toDF()

print(f'Shape df: {source_data_df.count()}')

# Transform
#.....
transformed_data = source_data_df.select(col("date"), col("payer_country"), col("payer"),  col("country"))

# Write redshift
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(
        transformed_data,glueContext,"data_ready"
    ),
    catalog_connection="via-redshift-connection",
    connection_options={"dbtable":schema_table, "database": database},
    redshift_tmp_dir=temp_s3
)


job = Job(glueContext)
job.init(args["JOB_NAME"], args)
job.commit()

"""


# Create table Athena

from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Inicializar el contexto de Spark y Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.hive.metastorePartitionPruningFallbackOnException", "true")
# Definir la consulta
consulta = """
    SELECT
        trim(trans.id_branch) AS id_branch,
        TRY_CAST(trim(trans.id_receiver) AS integer) AS id_receiver,
        TRY_CAST(rc.DATE_RECEIVER AS timestamp) AS DATE_RECEIVER, 
        trim(rc.id_location) AS id_location,
        'NN' AS id_payer, 
        TRY_CAST(rc.id_sender_global AS integer) AS id_sender_global,
        TRY_CAST(trans.net_amount_receiver AS double) AS net_amount_receiver,
        rc.mode_pay_receiver AS id_payout,
        TRY_CAST(receiver_transaction_count AS integer) AS receiver_transaction_count,
        rc.id_country_receiver AS id_country_receiver_claim, 
        rc.id_state_receiver AS id_state_receiver_claim, 
        rc.id_state, 
        TRY_CAST(branch_working_days AS integer) AS branch_working_days,
        TRY_CAST(sender_sending_days AS integer) AS sender_sending_days,
        TRY_CAST(sender_days_to_last_transaction AS integer) AS sender_days_to_last_transaction,
        rc.id_country, 
        fr.fraud_classification,
        TRY_CAST(sender_minutes_since_last_transaction AS integer) AS sender_minutes_since_last_transaction,
        TRY_CAST(branch_minutes_since_last_transaction AS integer) AS branch_minutes_since_last_transaction,
        TRY_CAST('0' AS integer) AS sender_days_since_last_transaction
    FROM viamericas.vector_total trans
    LEFT JOIN analytics.receiver_fraud_large fr
    ON TRY_CAST(trim(trans.id_receiver) AS integer) = TRY_CAST(fr.id_receiver AS integer)
    AND trim(trans.id_branch) = trim(fr.id_branch),
        (SELECT updt.*, br.id_location, br.id_state, br.id_country, sd.id_sender_global
        FROM analytics.receiver_large updt, analytics.branch_large br, analytics.sender_large sd
        WHERE trim(updt.id_branch) = br.id_branch AND br.id_branch=sd.id_branch
        AND updt.id_sender = sd.id_sender
        ) rc
    WHERE TRY_CAST(trim(trans.id_receiver) AS integer) = TRY_CAST(rc.id_receiver AS integer)
    AND trim(trans.id_branch) = trim(rc.id_branch)
"""

# Ejecutar la consulta
df_resultado = spark.sql(consulta)

# Escribir el resultado en S3
ruta_s3 = "s3://viamericas-datalake-dev-us-east-1-283731589572-analytics/fraud_abt/source_fraud_2024/"
df_resultado.write.mode("overwrite").parquet(ruta_s3)

# Imprimir la ruta de salida
print("Datos guardados en:", ruta_s3)


