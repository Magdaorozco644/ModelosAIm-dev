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


DAILY_CHECK_GP = 'daily_check_gp' #20240513_daily_check_gp

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
                # return as string
                partition_dt = partition_dt.strftime("%Y-%m-%d")
            except ValueError:
                self.logger.info("Invalid format date.")
                raise InputVaribleRequired(
                    f"The variable 'process_date' must be in the format YYYY-MM-DD or 'None', please correct it."
                )
        return partition_dt

    def read_daily_check_gp(self):
        self.logger.info("Reading from daily_check_gp...")
        # DB Setting
        database_name = "analytics"
        table_name = DAILY_CHECK_GP
        df = wr.athena.read_sql_table(
            table=table_name,
            database=database_name,
        )
        aux=df.loc[:,['payer','country','id_main_branch', 'id_country']]
        aux = aux.drop_duplicates(subset=['id_country', 'id_main_branch'], keep='last')
        aux['payer_country'] = aux['payer'] +'_'+ aux['country']
        aux=aux.loc[:,['payer_country','id_main_branch', 'id_country']]
        self.logger.info(f"Aux check: {aux.shape}")
        return aux

    def recommended_actions(self):
        self.logger.info(f'Recommended actions process...')
        # Generate monitoring df
        df_monitoring = self.generate_conditions()
        df_monitoring['desc_recommended_actions'] = df_monitoring.apply(self.actions_recommended, axis=1)
        # Generate aux df
        aux = self.read_daily_check_gp()
        # Merge with aux
        df_monitoring = pd.merge(df_monitoring, aux, left_on=['folder_name'],right_on=['payer_country'], how='inner')
        df_monitoring[['payer', 'country']] = df_monitoring['payer_country'].str.split('_', expand=True)
        # Normalize
        df_monitoring = df_monitoring.rename(columns={'payer': 'des_payer', 'country': 'des_country', 'id_main_branch': 'id_payer'})
        cols_to_eliminate = ['payer_country', 'folder_name']
        df_monitoring = df_monitoring.drop(columns=cols_to_eliminate)
        # Define date
        df_monitoring['date_last_check'] = df_monitoring['date_last_check'].dt.strftime('%Y%m%d')
        # Date
        date = df_monitoring['date_last_check'].iloc[0]
        # Define file_name
        file_name = f"day={date[:4]}-{date[4:6]}-{date[6:]}"
        # Save dataframe
        self.save_df(df=df_monitoring, name=file_name)

        return df_monitoring

    def actions_recommended(self, row):
        if  row['flag_increasing_moving_averages'] == True and row['flag_activate_calibration']==True and row['flag_activate_retraining']==False:
            return 'requires calibration'
        elif row['flag_increasing_moving_averages'] == True and row['flag_activate_retraining']==True:
            return 'requires re-training'
        else:
            return 'keep current model'

    def generate_conditions(self):
        self.logger.info('Consolidate dataframe using xlsx files from the top15 payers.')
        # Create Df
        df = self.consolidate_df()
        self.logger.info(f'Shape df: {df.shape}')
        # Condition 1
        self.logger.info('Executing condition 1...')
        result_one = self.condition_one(df=df)
        self.logger.info(f'Result 1 shape: {result_one.shape}')
        self.logger.info(f'DF one: \n {result_one}')
        # Condition 2
        self.logger.info('Executing condition 2...')
        result_two = self.condition_two(df=df)
        self.logger.info(f'Result 2 shape: {result_two.shape}')
        self.logger.info(f'DF two: \n {result_two}')
        # Combine
        ###WE ADD THE MAXIMUM OF THE 7-DAY MOVING AVERAGE OF THE MAPEs IN THE TEST PERIOD
        df_monitoring=pd.merge(result_one, result_two, on='folder_name', how='outer')
        self.logger.info(f'DF merge: \n {df_monitoring}')
        # Rename columns
        df_monitoring.rename(columns={'mape_7_days_avg': 'val_mape_max_mobile_7_days'}, inplace=True)
        # Generate condition 2 and 3.
        # Aplicar la función a lo largo de las filas del DataFrame
        df_monitoring['flag_activate_calibration'] = df_monitoring.apply(self.generate_condition2, axis=1)
        df_monitoring['flag_activate_retraining'] = df_monitoring.apply(self.generate_condition3, axis=1)

        self.logger.info(f'Monitoring shape final: {df_monitoring.shape}')
        self.logger.info(f'Monitoring final: {df_monitoring}')

        return df_monitoring

    ###GENERATE CONDITION 2
    def generate_condition2(self, row):
        if  row['val_mape_1_week']  > row['val_mape_max_mobile_7_days'] * 1.05:
            return True
        else:
            return False

    ###GENERATE CONDITION 3
    def generate_condition3(self, row):
        if  row['val_mape_1_week']  >  row['val_mape_max_mobile_7_days'] * 1.10:
            return True
        else:
            return False

    ### CONDITION2: THE MAPE OF THE LAST WEEK IS NOT GREATER THAN THE MAXIMUM 7-DAY MOVING AVERAGE HISTORICAL MAXIMUM OF THE TEST PERIOD
    def condition_two(self, df):
        mape_max_period=df[(df['date'] >= '2023-06-22') & (df['date'] <= '2023-12-18')].groupby('folder_name')['mape_7_days_avg'].max()
        return mape_max_period

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
        val_mape_1_week = group[group['date'] == last_monday]['mape_7_days_avg'].iloc[0]
        ###DETECT THE PREVIOUS MONDAY
        previous_monday = last_monday - pd.DateOffset(7)
        ###OBTAINING THE MOVING AVERAGE OF MAPS FOR THE PREVIOUS SEVEN DAYS
        val_mape_2_week = group[group['date'] == previous_monday]['mape_7_days_avg'].iloc[0]
        ###DETECT THE MONDAY BEFORE THE SECOND MONDAY
        previous_previous_monday = previous_monday - pd.DateOffset(7)
        ###OBTAINING THE MOVING AVERAGE OF MAPES FOR THE PREVIOUS SEVEN DAYS
        val_mape_3_week = group[group['date'] == previous_previous_monday]['mape_7_days_avg'].iloc[0]

        # CREATE A DATAFRAME WITH ADDITIONAL COLUMNS
        result = pd.DataFrame({
            'date_last_check': [last_monday],
            'folder_name': [group['folder_name'].iloc[0]], # El nombre del folder_name es el mismo para todos los registros del grupo
            'val_mape_1_week': [val_mape_1_week],
            'val_mape_2_week': [val_mape_2_week],
            'val_mape_3_week': [val_mape_3_week]
        })

        # ADD CONDITION 1 COLUMN
        if val_mape_1_week > val_mape_2_week and val_mape_2_week > val_mape_3_week:
            result['flag_increasing_moving_averages'] = True
        else:
            result['flag_increasing_moving_averages'] = False

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
        # date_last_check to datetime
        df["date_last_check"] = pd.to_datetime(df["date_last_check"])
        self.logger.info(f"Save inference in: {path_s3}")
        # Guarda el DataFrame en formato Parquet en S3
        response = wr.s3.to_parquet(df, path=path_s3, dataset=True, index=False,mode="overwrite_partitions",compression="snappy",)
        self.logger.info(f"Response: {response}")

    def consolidate_df(self):
        # I call function to read xlsx and consolidate into one DF
        df_top15 = self.read_files(bucket_name=self.args['bucket_name'], prefix=self.args['prefix_name_xlsx'])
        ##DATE OF ANALYSIS
        date = self.process_date # datetime.today().strftime('%Y-%m-%d')
        df_top15=df_top15.loc[df_top15.date<date]
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
        self.logger.info(f'Bucket: {bucket_name}')
        self.logger.info(f'Prefix: {prefix}')
        # Get all payers folders top-15
        folders_v3 = self.get_all_files(bucket_name=bucket_name,prefix=prefix)
        self.logger.info(f'Folder: {folders_v3}')
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
                excel_data = obj['Body'].read()

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
            files.extend([prefix['Prefix'].split('/')[-2] for prefix in page.get('CommonPrefixes', [])])

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
        "database_redshift": "__required__",
        "table_name_redshift": "__required__",
        "schema": "__required__",
        "temp_s3_dir" : "__required__",
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
    # Send to Redshift
    if df is not None and df.shape[0] != 0:

        # Pandas DataFrame to Spark DataFrame
        df_final_2d_spark = spark.createDataFrame(df)

        # Detect numeric columns
        numeric_cols = [
            c[0] for c in df_final_2d_spark.dtypes if c[1] in ["bigint", "double", "float"]
        ]
        # Fill numeric values in spark dataframe
        df_filled = df_final_2d_spark.fillna(0, subset=numeric_cols)

        print(df_filled.printSchema())
        print(df_filled.show())

        # Converto to Frame to upload to Redshift
        df_final_2d_frame = DynamicFrame.fromDF(
            df_filled, glueContext, "df_final"
        )
        # Redshift connection
        rds_conn = "via-redshift-connection"
        # Create stage temp table with schema.
        pre_query = """
        begin;
        CREATE TABLE if not exists {database}.{schema}.{table_name} (
            date_last_check date ENCODE az64,
            val_mape_1_week double precision ENCODE raw,
            val_mape_2_week double precision ENCODE raw,
            val_mape_3_week double precision ENCODE raw,
            flag_increasing_moving_averages boolean ENCODE raw,
            val_mape_max_mobile_7_days double precision ENCODE raw,
            flag_activate_calibration boolean ENCODE raw,
            flag_activate_retraining boolean ENCODE raw,
            desc_recommended_actions character varying(100) ENCODE lzo,
            id_payer character varying(100) ENCODE lzo,
            id_country character varying(100) ENCODE lzo,
            des_payer character varying(100) ENCODE lzo,
            des_country character varying(100) ENCODE lzo
            );
        end;
        """

        post_query = """
        begin;
        delete from {database}.{schema}.{table_name} using public.stage_table_monitoring_rules_temp where public.stage_table_monitoring_rules_temp.date_last_check = {database}.{schema}.{table_name}.date_last_check;
        insert into {database}.{schema}.{table_name} select * from public.stage_table_monitoring_rules_temp;
        drop table public.stage_table_monitoring_rules_temp;
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
                "dbtable": f"public.stage_table_monitoring_rules_temp",
                "preactions": pre_query_2d,
                "postactions": post_query_2d,
            },
            redshift_tmp_dir=args["temp_s3_dir"],
            transformation_ctx="upsert_to_redshift_2d",
        )

    job.commit()

