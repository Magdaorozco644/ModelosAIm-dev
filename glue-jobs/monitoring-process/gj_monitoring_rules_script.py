################################################
################################################
################################################
#               IMPORTS & CONFIG               #
################################################
################################################
################################################

import awswrangler as wr
import pandas as pd

# Spark uses .items
pd.DataFrame.iteritems = pd.DataFrame.items
from datetime import datetime
import logging
import sys
import boto3
from io import BytesIO
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


# check input variables
class InputVaribleRequired(Exception):
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


class MonitoringRules:

    def __init__(self, args) -> None:
        self.args = args
        self.logger = LoggerInit(args["LOG_LEVEL"]).logger
        self.logger.info("Init Monitoring Rules")
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
            except ValueError:
                self.logger.info("Invalid format date.")
                raise InputVaribleRequired(
                    f"The variable 'process_date' must be in the format YYYY-MM-DD or 'None', please correct it."
                )
        return partition_dt

    def recommended_actions(self):
        self.logger.info(f'Recommended actions process...')
        df_monitoring = self.generate_conditions()
        df_monitoring['RECOMMENDED_ACTIONS'] = df_monitoring.apply(self.actions_recommended, axis=1)
        # Define date
        df_monitoring['last_check_date'] = df_monitoring['last_check_date'].dt.strftime('%Y%m%d')
        # Define file_name
        file_name = f"day={df_monitoring['last_check_date'].iloc[0]}/"
        # Save dataframe
        self.save_df(df=df_monitoring, name=file_name)

        return df_monitoring

    def actions_recommended(self, row):
        if  row['CONDITION1'] == True and row['CONDITION2']==True and row['CONDITION3']==False:
            return 'REQUIRES CALIBRATION'
        elif row['CONDITION1'] == True and row['CONDITION3']==True:
            return 'REQUIRES RE-TRAINING'
        else:
            return 'KEEP CURRENT MODEL'

    def generate_conditions(self):
        self.logger.info('Consolidate dataframe using xlsx files from the top15 payers.')
        # Create Df
        df = self.consolidate_df()
        self.logger.info(f'Shape df: {df.shape}')
        # Condition 1
        self.logger.info('Executing condition 1...')
        result_one = self.condition_one(df=df)
        self.logger.info(f'Result 1 shape: {result_one.shape}')
        # Condition 2
        self.logger.info('Executing condition 2...')
        result_two = self.condition_two(df=df)
        self.logger.info(f'Result 2 shape: {result_two.shape}')
        # Combine
        ###WE ADD THE MAXIMUM OF THE 7-DAY MOVING AVERAGE OF THE MAPEs IN THE TEST PERIOD
        df_monitoring=pd.merge(result_one, result_two, on='folder_name', how='outer')
        # Rename columns
        df_monitoring.rename(columns={'mape_7_days_avg': 'mape_7_days_avg_mobile_max'}, inplace=True)
        # Generate condition 2 and 3.
        df_monitoring['CONDITION2'] = df_monitoring.apply(self.generate_condition2, axis=1)
        df_monitoring['CONDITION3'] = df_monitoring.apply(self.generate_condition3, axis=1)

        self.logger.info(f'Monitoring shape final: {df_monitoring.shape}')

        return df_monitoring

    ###GENERATE CONDITION 2
    def generate_condition2(self, row):
        if  row['mape_last_monday']  > row['mape_7_days_avg_mobile_max'] * 1.05:
            return True
        else:
            return False

    ###GENERATE CONDITION 3
    def generate_condition3(self, row):
        if  row['mape_last_monday']  >  row['mape_7_days_avg_mobile_max'] * 1.10:
            return True
        else:
            return False

    # TODO: Change DATES
    ### CONDITION2: THE MAPE OF THE LAST WEEK IS NOT GREATER THAN THE MAXIMUM 7-DAY MOVING AVERAGE HISTORICAL MAXIMUM OF THE TEST PERIOD
    def condition_two(self, df):
        mape_max_period=df[(df['date'] >= '2023-06-22') & (df['date'] <= '2023-12-18')].groupby('folder_name')['mape_7_days_avg'].max()
        return mape_max_period

    # TODO: Change DATES
    ### CONDITION 1: THAT THE 7-DAY MOVING AVERAGE OF THE LAST TWO MONDAYS GROWS:
    def condition_one(self, df):
        #MAPE AVERAGE IN TEST
        ##REFERENCE HISTORICAL MAPEs ARE CALCULATED BETWEEN 01/01/2021 AND 12/18/23
        self.mape_mean_period = df[(df['date'] >= '2023-06-22') & (df['date'] <= '2023-12-18')].groupby('folder_name')['mape'].mean()
        # Apply the mape_7_days_avg function to each group
        data_processed = df.groupby('folder_name').apply(self.mape_7_days_avg)
        # Reset index
        data_processed.reset_index(drop=True, inplace=True)
        # APPLY THE CHECK_GROWTH FUNCTION AND CONCATENATE RESULTS
        result = pd.concat([self.check_growth(group) for _, group in data_processed.groupby('folder_name')], ignore_index=True)
        return result

    # FUNCTION TO CALCULATE THE MOVING AVERAGE OF THE LAST 7 DAYS EXCLUDING VALUES 10 TIMES OR MORE HIGHER TAHN THE AVERAGE MAPE.
    def mape_7_days_avg(self, group):
        self.logger.info('Moving average of the last 7 days.')
        high_mape_values = group[group['mape'] >= 10 * self.mape_mean_period[group.name]]
        group.loc[high_mape_values.index, 'mape'] = None  # Replace high values with None
        #Calculate the moving average of the last 7 days for the reference period (06/2023 - 12-2023)
        group['mape_7_days_avg'] = group.groupby('folder_name')['mape'].rolling(window=7, min_periods=5).mean().reset_index(level=0, drop=True).shift(1)
        return group

    #Apply the grouping function and obtain the variable CONDITION 1 for each 'folder_name'.
    def check_growth(self, group):
        ###DETECT THE LAST MONDAY IN THE SERIES
        last_monday = group[group['date'].dt.dayofweek == 0].max()['date']
        ### EXTRAMING THE MAPEs MOBILE AVERAGE FOR THE 7 DAYS PRIOR TO THAT MONDAY
        mape_last_monday = group[group['date'] == last_monday]['mape_7_days_avg'].iloc[0]
        ###DETECT THE PREVIOUS MONDAY
        previous_monday = last_monday - pd.DateOffset(7)
        ###OBTAINING THE MOVING AVERAGE OF MAPS FOR THE PREVIOUS SEVEN DAYS
        mape_previous_monday = group[group['date'] == previous_monday]['mape_7_days_avg'].iloc[0]
        ###DETECT THE MONDAY BEFORE THE SECOND MONDAY
        previous_previous_monday = previous_monday - pd.DateOffset(7)
        ###OBTAINING THE MOVING AVERAGE OF MAPES FOR THE PREVIOUS SEVEN DAYS
        mape_previous_previous_monday = group[group['date'] == previous_previous_monday]['mape_7_days_avg'].iloc[0]

        # CREATE A DATAFRAME WITH ADDITIONAL COLUMNS
        result = pd.DataFrame({
            'last_check_date': [last_monday],
            'folder_name': [group['folder_name'].iloc[0]], # El nombre del folder_name es el mismo para todos los registros del grupo
            'mape_last_monday': [mape_last_monday],
            'mape_previous_monday': [mape_previous_monday],
            'mape_previous_previous_monday': [mape_previous_previous_monday]
        })

        # ADD CONDITION 1 COLUMN
        if mape_last_monday > mape_previous_monday and mape_previous_monday > mape_previous_previous_monday:
            result['CONDITION1'] = True
        else:
            result['CONDITION1'] = False

        return result

    def save_df(self, df, name):
        path_s3 = (
            "s3://"
            + self.args["bucket_name"]
            + "/"
            + self.args["prefix_name_save"]
            + "/"
            + name
        )
        self.logger.info(f"Save inference in: {path_s3}")
        # Guarda el DataFrame en formato Parquet en S3
        response = wr.s3.to_parquet(df, path=path_s3, dataset=True, index=False,mode="overwrite_partitions",compression="snappy",)
        self.logger.info(f"Response: {response}")

    def consolidate_df(self):
        # I call function to read xlsx and consolidate into one DF
        df_top15 = self.read_files(bucket_name=self.args['bucket_name'], prefix=self.args['prefix_name_xlsx'])
        ##DATE OF ANALYSIS #TODO: Change DATE.
        df_top15=df_top15.loc[df_top15.date<'2024-02-01']
        # Cast to date
        df_top15['date'] = pd.to_datetime(df_top15['date'])
        ##WE REPLACE THE NEGATIVE PREDICTED VALUES
        valor_a_reemplazar = 0
        df_top15['valor_predicho'] = df_top15['valor_predicho'].apply(lambda x: valor_a_reemplazar if x < 0 else x)
        df_top15 = df_top15.sort_values(by=['folder_name', 'date'])
        ##WE OBTAIN THE MOVING AVERAGE OF THE MAPE OF THE PREVIOUS 7 DAYS.
        df_top15['mape_7_days_avg'] = df_top15.groupby('folder_name')['mape'].rolling(window=7, min_periods=5).mean().reset_index(level=0, drop=True).shift(1)
        return df_top15

    def read_files(self, bucket_name, prefix):
        dfs = []
        folders_v3 = self.get_all_files(bucket_name=bucket_name,prefix=prefix)
        # Iterar sobre cada carpeta
        for folder_name in folders_v3:
            # Obtener la lista de objetos en la carpeta
            objects = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f'{prefix}{folder_name}/')['Contents']

            # Buscar archivos xlsx en la carpeta
            excel_objects = [obj for obj in objects if obj['Key'].endswith('.xlsx') and '7d' not in obj['Key']]

            if excel_objects:
                self.logger.info(excel_objects)
                # Leer el primer archivo xlsx encontrado
                obj = self.s3_client.get_object(Bucket=bucket_name, Key=excel_objects[0]['Key'])
                excel_data = obj['Body'].read() # No se puede leer con pandas directo de S3

                df = pd.read_excel(BytesIO(excel_data))
                # Agrego columna con el nombre de la carpeta
                df['folder_name'] = folder_name
                # Agregar el df a la lista
                dfs.append(df)
            else:
                self.logger.info(f"No se encontraron archivos xlsx en la carpeta {folder_name}.")

        if dfs:
            # Concateno todo
            consolidated_df = pd.concat(dfs, ignore_index=True)
            return consolidated_df
        else:
            self.logger.info("No se encontraron archivos xlsx en ninguna de las carpetas especificadas.")
            return None

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
            files.append([prefix['Prefix'].split('/')[-2] for prefix in page.get('CommonPrefixes', [])])

        return files


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
        "bucket_name": "__required__",
        "prefix_name_xlsx": "__required__",
        "prefix_name_save": "__required__",
        "process_date": "None",
    }
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    parser = ArgsGet(my_default_args)
    args = parser.loaded_args
    job.init(args["JOB_NAME"], args)
    # Object Monitoring
    monitoring = MonitoringRules(args=args)

    df = monitoring.recommended_actions()
    # TODO: Implement this to send to Redshift (check if needed).
    if df is not None and df.shape[0] != 0:

        # Pandas DataFrame to Spark DataFrame
        df_final_2d_spark = spark.createDataFrame(df)

        print(df_final_2d_spark.printSchema())
        print(df_final_2d_spark.show())

        # Converto to Frame to upload to Redshift
        df_final_2d_frame = DynamicFrame.fromDF(
            df_final_2d_spark, glueContext, "df_final"
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

