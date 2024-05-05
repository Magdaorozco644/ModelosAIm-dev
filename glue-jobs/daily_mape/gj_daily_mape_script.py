################################################
################################################
################################################
#               IMPORTS & CONFIG               #
################################################
################################################
################################################

import awswrangler as wr
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Spark uses .items
pd.DataFrame.iteritems = pd.DataFrame.items
from datetime import datetime
import logging
import sys
import boto3
from io import BytesIO # Para el WA

from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


# check input variables
class InputVaribleRequired(Exception):
    pass
# File not found
class FileNotFound(Exception):
    pass

# Logging Class
class LoggerInit:
    """
    LoggerInit class
    Initialize logger level with defaults configs
    level=class logging level, default= 'DEBUG'
                                        'CRITICAL',
                                        'FATAL',
                                        'ERROR',
                                        'WARN',
                                        'WARNING',
                                        'INFO',
                                        'DEBUG',
                                        'NOTSET'
    msg_format= str format, default= '%(asctime)s %(levelname)s %(name)s: %(message)s'
    date_format= date format, default = '%Y-%m-%d %H:%M:%S'
    """

    def __init__(
        self,
        level="DEBUG",
        msg_format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        date_format="%Y-%m-%d %H:%M:%S",
    ) -> None:
        self.msg_format = msg_format
        self.date_format = date_format
        self.level = level
        self._logger = self.set_logging()

        pass

    @property
    def logger(self):
        return self._logger

    def set_logging(self):
        logging.basicConfig(
            format=self.msg_format,
            datefmt=self.date_format,
            stream=sys.stdout,
        )
        logger = logging.getLogger()
        logger.setLevel(self.level)
        logger.info("Logger Initializated")
        return logger


# Args class
class ArgsGet:
    """
    Parse input runtime arguments from glue job init params
    ```
    my_args = [ 'JOB_NAME',
                'LOG_LEVEL',
                'bucket_name',
                'date_from',
                'date_to']
    parser = ArgsGet(my_args)
    bucket_name = parser['bucket_name']
    ```
    TO DO:
        python data class
    """

    @property
    def loaded_args(self):
        return self._loaded_args

    def __init__(self, arg_vars=dict) -> None:
        self.args = arg_vars
        self._loaded_args = self.load_args()

    def load_args(self) -> dict:
        """get args from awsglue.utils,
        set default value from provided dict

        Returns:
            dict: with loaded arguments from init or default
        """
        args_list = list(self.args.keys())
        glue_args = getResolvedOptions(sys.argv, args_list)
        # put default values if not set, or breack if required and not setted
        for k, value in self.args.items():
            if not glue_args.get(k) and value == "__required__":
                raise InputVaribleRequired(
                    f"The variable {k} is required and was no setted, please pass it as argument '--{k}' {value}"
                )
            else:  # set default"
                glue_args[k] = glue_args[k] or value
        return glue_args


##############################################
##############################################
##############################################
#               INFERENCE                    #
##############################################
##############################################
##############################################


class Mape:

    def __init__(self, args) -> None:
        self.args = args
        self.logger = LoggerInit(args["LOG_LEVEL"]).logger
        self.logger.info("Init Daily Mape Calculation")
        self.process_date = self.check_date()
        self.logger.info(f"Process date: {self.process_date}")
        self.s3_client = boto3.client("s3")

    def check_date(self):
        """Validate input process date.

        Raises:
            InputVaribleRequired: process date must be None or in format YYYY-MM-DD
        """
        if self.args["process_date"].upper() == "NONE":
            partition_dt = datetime.now().strftime("%Y-%m-%d")
        else:
            try:
                partition_dt = datetime.strptime(self.args["process_date"], "%Y-%m-%d")
                partition_dt = partition_dt.strftime("%Y-%m-%d")
            except ValueError:
                self.logger.info("Invalid format date.")
                raise InputVaribleRequired(
                    f"The variable 'process_date' must be in the format YYYY-MM-DD or 'None', please correct it."
                )
        return partition_dt

    def calculate_mape(self, merged_df):
        # Calculate mean absolute error
        merged_df['pred'] = merged_df['pred'].astype(float)
        merged_df['amount'] = merged_df['amount'].astype(float)
        merged_df['abs_error'] = np.abs(merged_df['pred'] - merged_df['amount'])

        # ABS error
        merged_df['MAPE'] = (merged_df['abs_error'] / merged_df['amount']) * 100
        merged_df['MAPE'] = merged_df['MAPE'].fillna(0)

        merged_df[merged_df['amount'] == 0]
        # Select columns
        mape_df = merged_df[['processing_date', 'pred_date', 'pred', 'payer_country',
                'id_main_branch', 'id_country', 'amount', 'abs_error', 'MAPE']]

        return mape_df

    def create_df_mape(self):
        s3_bucket = self.args['bucket_name'] # 'viamericas-datalake-dev-us-east-1-283731589572-analytics'
        inference_prefix = self.args['prefix'] # 'inference_prediction'
        save_prefix = self.args['save_prefix']

        # Setting processing_date
        processing_date = self.process_date
        previous_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

        self.logger.info(f'Processing date: {processing_date}')
        self.logger.info(f'Previous date: {previous_date}')

        # Read Daily Check
        database_name = self.args['database_athena'] # "analytics"
        table_name = self.args['table_name_athena'] # "daily_check_gp"
        df_daily = wr.athena.read_sql_table(
            table=table_name,
            database=database_name,
        )

        # Cast to date
        df_daily['date'] = pd.to_datetime(df_daily['date'])
        self.logger.info(f'Shape daily check: {df_daily.shape}')

        # Read predictions
        predict_path = f's3://{s3_bucket}/{inference_prefix}/day={previous_date}/predictions_2d/'
        self.logger.info(f'Read from: {predict_path}')
        try:
            df_predict = wr.s3.read_parquet(predict_path)
        except Exception as e:
            self.logger.info('File not found.')
            raise FileNotFound(f'Warning! File not found. Please check in s3 if file exist: {predict_path}')
        self.logger.info(f'Shape predict: {df_predict.shape}')
        self.logger.info(f'Types predict: {df_predict.dtypes}')
        self.logger.info(f'Types daily: {df_daily.dtypes}')

        df_predict['pred_date'] = pd.to_datetime(df_predict['pred_date'])

        # Merge data frames
        merged_df = pd.merge(df_predict, df_daily, left_on=['pred_date','id_main_branch','id_country', 'payer', 'country'],
                            right_on=['date','id_main_branch','id_country', 'payer', 'country'])
        self.logger.info(f'Shape merge: {merged_df.shape}')

        mape_df = self.calculate_mape(merged_df=merged_df)
        self.logger.info(f'Shape mape: {mape_df.shape}')
        self.logger.info(f'Shape mape: {mape_df.dtypes}')

        # Save as parquet
        to_save = f's3://{s3_bucket}/{save_prefix}/day={previous_date}/mape_2d/'
        self.logger.info(f'To save: {to_save}')
        wr.s3.to_parquet(df=mape_df,path=to_save, dataset=True, index=False,mode="overwrite_partitions",compression="snappy")

        return mape_df

    def get_all_files(self, bucket_name: str, prefix: str) -> list:
        """Get all objects key

        Args:
            bucket_name (str): bucket name
            prefix (str): folder to read

        Returns:
            list: all the objects keys.
        """
        files = []
        s3_client = boto3.client("s3")

        # Configure paginator to list all objects
        paginator = s3_client.get_paginator("list_objects_v2")

        # Iterate
        for page in paginator.paginate(
            Bucket=bucket_name, Prefix=prefix, Delimiter="/"
        ):
            if "CommonPrefixes" in page:
                for prefix in page["CommonPrefixes"]:
                    files.append(prefix["Prefix"])

        return files

    def update_historical_data(self, df_preds):
        # Iterarar sobre la carpeta de mapes historicos (15 top payers)
        self.logger.info(f"Reading xlsx files from: s3://{self.args['bucket_name']}/{self.args['top_payers_abt_key']} ")
        folder_list = self.get_all_files(bucket_name=self.args['bucket_name'], prefix=self.args['top_payers_abt_key'])
        self.logger.info(folder_list)
        for folder_name in folder_list:
            # Obtener la lista de objetos en la carpeta
            objects = self.s3_client.list_objects_v2(Bucket=self.args['bucket_name'], Prefix=folder_name)['Contents']
            # Buscar archivos xlsx en la carpeta
            excel_objects = [obj for obj in objects if obj['Key'].endswith('.xlsx') and '7d' not in obj['Key']]
            # iterate through excel files and append prediction
            for object in excel_objects:
                self.logger.info(f'File: {object}')
                payer_country = object['Key'].split('/')[-2]
                file_name = object['Key'].split('/')[-1]
                self.logger.info(f'Payer country: {payer_country}')
                self.logger.info(f'File Name: {file_name}')
                obj = self.s3_client.get_object(Bucket=self.args['bucket_name'], Key=object['Key'])
                excel_data = obj['Body'].read()
                df = pd.read_excel(BytesIO(excel_data))
                self.logger.info(df_preds.head())
                self.logger.info(df_preds['payer_country'].unique())

                # Filter predictions
                df_payer_country = df_preds[df_preds['payer_country'] == payer_country]
                df_payer_country = df_payer_country.rename(columns={"pred_date": "date", "pred": "valor_predicho", "MAPE": "mape", "abs_error": "error_abs", "amount": "valor_real"})
                self.logger.info(df_payer_country.head())
                # Merge with predictions
                df_merged = pd.merge(df, df_payer_country, how='outer', on='date', suffixes=('_df1', '_df2'))
                df_merged['valor_real'] = df_merged['valor_real_df2'].fillna(df_merged['valor_real_df1'])
                df_merged['valor_predicho'] = df_merged['valor_predicho_df2'].fillna(df_merged['valor_predicho_df1'])
                df_merged['mape'] = df_merged['mape_df2'].fillna(df_merged['mape_df1'])
                df_merged['error_abs'] = df_merged['error_abs_df2'].fillna(df_merged['error_abs_df1'])

                # Get columns
                df_merged.drop(['valor_real_df1', 'valor_real_df2', 'valor_predicho_df1', 'valor_predicho_df2',
                'mape_df1', 'mape_df2', 'error_abs_df1', 'error_abs_df2', 'processing_date', 'payer_country',
                'id_main_branch','id_country'], axis=1, inplace=True)


                # Save s3
                excel_buffer = BytesIO()
                df_merged.to_excel(excel_buffer, index=False)
                excel_buffer.seek(0)
                # Name S3
                file_name = f'{self.args['top_payers_abt_key']}{file_name}_archivo_final.xlsx'
                self.s3_client.upload_fileobj(excel_buffer, self.args['bucket_name'], file_name)

##########################################
##########################################
##########################################
#               RUN SCRIPT               #
##########################################
##########################################
##########################################


if __name__ == "__main__":
    my_default_args = {
        "JOB_NAME": "__required__",
        "LOG_LEVEL": "INFO",
        "process_date": "None",
        "bucket_name": "__required__",
        "prefix": "__required__",
        "save_prefix": "__required__",
        "database_redshift": "__required__",
        "database_athena": "__required__",
        "table_name_athena": "__required__",
        "table_name_redshift": "__required__",
        "temp_s3_dir": "__required__",
        "top_payers_abt_key": "__required__",
        "schema": "__required__"
    }
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    parser = ArgsGet(my_default_args)
    args = parser.loaded_args
    job.init(args["JOB_NAME"], args)

    daily_mape = Mape(args=args)

    df_final_2d = daily_mape.create_df_mape()
    print(df_final_2d.shape)
    print(df_final_2d.info())

    #Update historics top 15 payers
    daily_mape.update_historical_data(df_preds=df_final_2d)

    if df_final_2d is None or df_final_2d.shape[0] == 0:
        print('Dataframe Empty.')
        # Schema
        data =  {
            'processing_date': [datetime.today().date()]*2,
            'pred_date': [datetime.today().date(), (datetime.today() + timedelta(days=1)).date()],
            'pred': [0, 0],
            'payer_country': ['', ''],
            'id_main_branch': ['', ''],
            'id_country': ['', ''],
            'amount': [0, 0],
            'abs_error': [0, 0],
            'MAPE': [0, 0]
        }

        # Create empty dataframe
        df_final_2d = pd.DataFrame(data=data)

    # Pandas DataFrame to Spark DataFrame
    df_final_2d_spark = spark.createDataFrame(df_final_2d)

    print(df_final_2d_spark.printSchema())
    print(df_final_2d_spark.show())

    # Converto to Frame to upload to Redshift
    df_final_2d_frame = DynamicFrame.fromDF(
        df_final_2d_spark, glueContext, "df_final_2d"
    )
    # Redshift connection
    rds_conn = "via-redshift-connection"
    # Create stage temp table with schema.
    pre_query = """
    CREATE TABLE if not exists {database}.{schema}.{table_name} (
        processing_date date ENCODE az64,
        pred_date date ENCODE az64,
        pred double precision ENCODE raw,
        payer_country character varying(100) ENCODE lzo,
        id_main_branch character varying(100) ENCODE lzo,
        id_country character varying(100) ENCODE lzo,
        amount double precision ENCODE raw,
        abs_error double precision ENCODE raw,
        MAPE double precision ENCODE raw
        );
    """
    post_query = """
    begin;
    delete from {database}.{schema}.{table_name} using public.stage_table_temporary_mape where public.stage_table_temporary_mape.processing_date = {database}.{schema}.{table_name}.processing_date;
    insert into {database}.{schema}.{table_name} select * from public.stage_table_temporary_mape;
    drop table public.stage_table_temporary_mape;
    end;
    """

    pre_query_2d = pre_query.format(
        database=args["database_redshift"],
        schema=args["schema"],
        table_name=args["table_name_redshift"],
    )
    post_query_2d = post_query.format(
        database=args["database_redshift"],
        schema=args["schema"],
        table_name=args["table_name_redshift"],
    )
    print(f"Pre query 2d: {pre_query_2d}")
    print(f"Post query 2d: {post_query_2d}")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=df_final_2d_frame,
        catalog_connection=rds_conn,
        connection_options={
            "database": args["database_redshift"],
            "dbtable": f"public.stage_table_temporary_mape",
            "preactions": pre_query_2d,
            "postactions": post_query_2d,
        },
        redshift_tmp_dir=args["temp_s3_dir"],
        transformation_ctx="upsert_to_redshift_2d",
    )

    job.commit()
