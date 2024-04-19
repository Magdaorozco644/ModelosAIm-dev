################################################
################################################
################################################
#               IMPORTS & CONFIG               #
################################################
################################################
################################################

import awswrangler as wr
import warnings
import pandas as pd

# Spark uses .items
pd.DataFrame.iteritems = pd.DataFrame.items
from datetime import datetime
import logging
import sys
import boto3
from io import BytesIO
import joblib
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


class Inference:

    def __init__(self, args) -> None:
        self.args = args
        self.logger = LoggerInit(args["LOG_LEVEL"]).logger
        self.logger.info("Init Inference Prediction")
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

    def process_payer(self):
        # Abt partition
        data = self.load_abt_df()
        self.logger.info("Data loaded.")
        # Failed Predicts
        failed_payers = []
        # Process 2d
        self.logger.info("Processing 2d model.")
        df_2d, failed_list = self.process_model_ndays(
            days=2, prefix=self.args["model_2d_prefix"], data=data
        )
        failed_payers.extend(failed_list)
        # Process 8d
        self.logger.info("Processing 8d model.")
        # Marcar con 1 en 'day_of_the_dead' cuando 'date' sea 2 de noviembre
        data["date"] = pd.to_datetime(data["date"])
        data.loc[
            data["date"].dt.month.eq(11) & data["date"].dt.day.eq(2), "day_of_the_dead"
        ] = 1
        df_8d, failed_list = self.process_model_ndays(
            days=8, prefix=self.args["model_8d_prefix"], data=data
        )
        failed_payers.extend(failed_list)
        self.logger.info(f"Failed predictions: {failed_payers}")
        # Replace first 2 days from df_8d with df_2d
        # Exclude [index - payer_country] from 8d that exists in 2d
        df_8d_filtered = df_8d[
            ~df_8d.set_index(["pred_date", "payer_country"]).index.isin(
                df_2d.set_index(["pred_date", "payer_country"]).index
            )
        ]

        # Create df8_d
        df_8d = pd.concat([df_8d_filtered, df_2d])

        # Daily Check
        df = self.read_daily_check_gp()
        # Ids
        df_id = (
            df[["payer_country", "id_main_branch", "id_country"]]
            .drop_duplicates()
            .dropna(subset="id_main_branch")
        )
        self.logger.info(f"Total ids: {len(df_id)}")

        df_final_2d = pd.merge(df_2d, df_id, on="payer_country", how="left")
        df_final_8d = pd.merge(df_8d, df_id, on="payer_country", how="left")

        # Insertar la columna 'processing_date' al principio del DataFrame
        df_final_2d.insert(0, "processing_date", self.process_date)
        df_final_8d.insert(0, "processing_date", self.process_date)

        df_final_2d["processing_date"] = pd.to_datetime(df_final_2d["processing_date"])
        df_final_8d["processing_date"] = pd.to_datetime(df_final_8d["processing_date"])

        # Splitting 'payer' & 'country'
        df_final_2d[["payer", "country"]] = df_final_2d["payer_country"].str.split(
            "_", expand=True
        )
        df_final_8d[["payer", "country"]] = df_final_8d["payer_country"].str.split(
            "_", expand=True
        )

        self.logger.info(f"DF 8d: {df_final_8d.shape}")
        self.logger.info(f"DF 2d: {df_final_2d.shape}")

        self.save_df(df=df_final_2d, name=f"day={self.process_date}/predictions_2d/")
        self.save_df(df=df_final_8d, name=f"day={self.process_date}/predictions_8d/")

        return failed_payers, df_final_2d, df_final_8d

    def save_df(self, df, name):
        path_s3 = (
            "s3://"
            + self.args["bucket_name"]
            + "/"
            + self.args["prefix_inference_name"]
            + name
        )
        self.logger.info(f"Save inference in: {path_s3}")
        # Guarda el DataFrame en formato Parquet en S3
        response = wr.s3.to_parquet(df, path=path_s3, dataset=True, index=False,mode="overwrite_partitions",compression="snappy",)
        self.logger.info(f"Response: {response}")

    def read_daily_check_gp(self):
        self.logger.info("Reading from daily_check_gp...")
        # DB Setting
        database_name = "analytics"
        table_name = "daily_check_gp"
        df = wr.athena.read_sql_table(
            table=table_name,
            database=database_name,
        )
        # Payer country
        df["payer_country"] = df["payer"] + "_" + df["country"]
        self.logger.info(f"Daily check gp: {df.shape}")
        return df

    def process_model_ndays(self, days: int, prefix: str, data):
        # List payer country fail
        payer_countries_pinched = []
        # Initialize an empty DataFrame to store the results
        df_temp = pd.DataFrame(columns=["date", "pred", "payer_country", "model"])
        # Pckl Models
        if days == 2:
            prefix_s3 = self.args["prefix_model_name_2d"]
        elif days == 8:
            prefix_s3 = self.args["prefix_model_name_8d"]
        else:
            raise InputVaribleRequired("Days must be 2 or 8.")
        pkl_files = self.list_pkl_objects(prefix_name=prefix, prefix_s3=prefix_s3)
        if len(pkl_files) == 0:
            raise InputVaribleRequired("Total pkl objects equals = 0")
        # flag
        i = 1
        # Iterate over pkl files
        for file_key in pkl_files:
            # Extract payer_country from file_key
            payer_country = file_key.split("/")[2]
            self.logger.info(
                f"Payer country: {payer_country}, Bucket Name: {self.args['bucket_name']}, Prefix: {file_key}"
            )

            # Download pkl file from S3 and load it into memory
            response = self.s3_client.get_object(
                Bucket=self.args["bucket_name"], Key=file_key
            )
            buffer = BytesIO(response["Body"].read())
            forecaster = joblib.load(buffer)

            #### PAYER SETTING ####
            # Filter data for the specific payer_country
            datos = data[data["payer_country"] == payer_country].copy()
            # datos = data.loc[data.payer_country == payer_country]
            datos["date"] = pd.to_datetime(datos["date"])
            datos.set_index("date", inplace=True)
            datos = datos.asfreq("D")

            # Predictions settings
            last_window_date = forecaster.last_window.index[-1] + pd.Timedelta(days=1)
            # Today or Reprocess date
            test_date = pd.Timestamp(
                self.process_date
            )  # The first test day would be the day to predict

            # Extract data for last window and test period. LastDayModel_Today()-1
            data_last_window = datos.loc[
                last_window_date : test_date - pd.Timedelta(days=1)
            ].copy()
            data_last_window[forecaster.exog_col_names] = data_last_window[
                forecaster.exog_col_names
            ].fillna(0)
            data_test = datos.loc[
                test_date : test_date + pd.Timedelta(days=days - 1)
            ].copy()
            data_test[forecaster.exog_col_names] = data_test[
                forecaster.exog_col_names
            ].fillna(0)
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore")
                # Make predictions
                predictions = forecaster.predict(
                    steps=days - 1,
                    exog=data_test[forecaster.exog_col_names],
                    last_window=data_last_window["amount"],
                    last_window_exog=data_last_window[forecaster.exog_col_names],
                )
                # Store predictions in a temporary DataFrame
                df_temp = pd.DataFrame(predictions, columns=["pred"]).reset_index()

            except:
                # If an exception occurs, set predictions to zero
                self.logger.info(f"Error processing {payer_country}")
                predictions = [0] * days
                date_range = pd.date_range(start=test_date, periods=days)
                df_temp = pd.DataFrame(
                    {
                        "index": date_range,
                        "pred": predictions,
                    }
                )
                payer_countries_pinched.append(f"{payer_country} | {days}d Model")

            # Add additional columns
            df_temp["payer_country"] = payer_country
            df_temp["model"] = file_key.split("/")[-1]

            # Concatenate df_temp with the main DataFrame
            if i:
                temp_df = df_temp.copy()
                i = 0
            else:
                temp_df = pd.concat([temp_df, df_temp], ignore_index=True)
        # Replace values in 'pred' column with 0 where 'pred' is less than 0
        temp_df.loc[temp_df["pred"] < 0, "pred"] = 0
        # Convert 'index' column to date format
        temp_df["index"] = pd.to_datetime(temp_df["index"]).dt.date
        # Rename column
        temp_df.rename(columns={"index": "pred_date"}, inplace=True)
        self.logger.info(f"Result df: {temp_df.shape}")
        return temp_df, payer_countries_pinched

    def list_pkl_objects(self, prefix_name: str, prefix_s3: str):
        """_summary_

        Args:
            prefix (str): _description_. 'MODEL_2d_'
        """
        self.logger.info(f'Bucket: {self.args["bucket_name"]} , Prefix: {prefix_s3}')
        elements = self.s3_client.list_objects(
            Bucket=self.args["bucket_name"], Prefix=prefix_s3
        )
        print(elements)

        # Listing pkl files
        pkl_files = [
            obj["Key"]
            for obj in elements.get("Contents", [])
            if obj["Key"].endswith(".pkl")
            and (obj["Key"].split("/")[-1].startswith(prefix_name))
        ]

        self.logger.info(f"Total pckl models: {len(pkl_files)}")

        return pkl_files

    def load_abt_df(self):
        self.logger.info("Reading last abt partition.")
        prefix = self.last_abt_partition()
        bucket = self.args["bucket_name"]
        df = wr.s3.read_parquet(path=f"s3://{bucket}/{prefix}")
        df["date"] = pd.to_datetime(df["date"]).dt.date
        self.logger.info(f"Df shape: {df.shape}")
        return df

    def last_abt_partition(self):
        prefix = f'{self.args["prefix_abt_name"]}/dt='
        # List objects in the S3 path
        files = self.get_all_files(bucket_name=self.args["bucket_name"], prefix=prefix)
        max_date = None
        # Iterate prefixs
        for prefix in files:
            # Get date
            date_str = prefix.split("=")[-1].rstrip("/")
            date = datetime.strptime(date_str, "%Y-%m-%d")
            # Get last date
            if max_date is None or date > max_date:
                max_date = date
                prefix_abt = prefix
        self.logger.info(f"Partition abt to use: {max_date}, Prefix: {prefix_abt}")
        return prefix_abt

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
        "prefix_abt_name": "__required__",
        "prefix_inference_name": "__required__",
        "prefix_model_name_8d": "__required__",
        "prefix_model_name_2d": "__required__",
        "model_2d_prefix": "__required__",
        "model_8d_prefix": "__required__",
        "arn_report": "__required__",
        "temp_s3_dir": "__required__",
        "database": "__required__",
        "schema": "__required__",
        "table_name_2d": "__required__",
        "table_name_8d": "__required__",
        "process_date": "None",
    }
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    parser = ArgsGet(my_default_args)
    args = parser.loaded_args
    job.init(args["JOB_NAME"], args)
    # Object Inference
    prediction = Inference(args=args)
    # Read from athena
    #df = wr.athena.read_sql_query(sql='select * from analytics.daily_check_gp limit 10;', database='analytics')
    #print(df.head())

    failure_predictions, df_final_2d, df_final_8d = prediction.process_payer()
    #failure_predictions = []
    #df_final_2d = wr.s3.read_parquet(
    #    path="s3://viamericas-datalake-dev-us-east-1-283731589572-analytics/inference_prediction/2024-04-09/predictions_2d/modified.snappy.parquet"
    #)
    #df_final_8d = wr.s3.read_parquet(
    #    path="s3://viamericas-datalake-dev-us-east-1-283731589572-analytics/inference_prediction/2024-04-09/predictions_8d/modified.snappy.parquet"
    #)
    # Send sns message with failures
    if len(failure_predictions) > 0:
        formatted_message = "List of payers that failed the prediction: \n"
        formatted_message += "\n".join(str(item) for item in failure_predictions)
        # Create SNS client
        sns_client = boto3.client("sns")

        # Send email
        response = sns_client.publish(
            TopicArn=args["arn_report"],
            Message=formatted_message,
            Subject="Report Failed Predictions",
        )

    # Pandas DataFrame to Spark DataFrame
    df_final_2d_spark = spark.createDataFrame(df_final_2d)
    df_final_8d_spark = spark.createDataFrame(df_final_8d)

    # Converto to Frame to upload to Redshift
    df_final_2d_frame = DynamicFrame.fromDF(
        df_final_2d_spark, glueContext, "df_final_2d"
    )
    df_final_8d_frame = DynamicFrame.fromDF(
        df_final_8d_spark, glueContext, "df_final_8d"
    )

    # Redshift connection
    rds_conn = "via-redshift-connection"
    # Create stage temp table with schema.
    pre_query = """
    CREATE TABLE if not exists {database}.{schema}.{table_name} (
        processing_date date ENCODE az64,
        pred_date date ENCODE az64,
        pred double precision ENCODE raw,
        payer_country character varying(200) ENCODE lzo,
        model character varying(200) ENCODE lzo,
        id_main_branch character varying(200) ENCODE lzo,
        id_country character varying(10) ENCODE lzo,
        payer character varying(200) ENCODE lzo,
        country character varying(200) ENCODE lzo
        );
    """
    post_query = """
    begin;
    delete from {database}.{schema}.{table_name} using public.stage_table_temporary where public.stage_table_temporary.processing_date = {database}.{schema}.{table_name}.processing_date;
    insert into {database}.{schema}.{table_name} select * from public.stage_table_temporary;
    drop table public.stage_table_temporary;
    end;
    """

    pre_query_2d = pre_query.format(
        database=args["database"],
        schema=args["schema"],
        table_name=args["table_name_2d"],
    )
    post_query_2d = post_query.format(
        database=args["database"],
        schema=args["schema"],
        table_name=args["table_name_2d"],
    )
    print(f"Pre query 2d: {pre_query_2d}")
    print(f"Post query 2d: {post_query_2d}")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=df_final_2d_frame,
        catalog_connection=rds_conn,
        connection_options={
            "database": args["database"],
            "dbtable": f"public.stage_table_temporary",
            "preactions": pre_query_2d,
            "postactions": post_query_2d,
        },
        redshift_tmp_dir=args["temp_s3_dir"],
        transformation_ctx="upsert_to_redshift_2d",
    )

    pre_query_8d = pre_query.format(
        database=args["database"],
        schema=args["schema"],
        table_name=args["table_name_8d"],
    )
    post_query_8d = post_query.format(
        database=args["database"],
        schema=args["schema"],
        table_name=args["table_name_8d"],
    )
    print(f"Pre query 8d: {pre_query_8d}")
    print(f"Post query 8d: {post_query_8d}")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=df_final_8d_frame,
        catalog_connection=rds_conn,
        connection_options={
            "database": args["database"],
            "dbtable": f"public.stage_table_temporary",
            "preactions": pre_query_8d,
            "postactions": post_query_8d,
        },
        redshift_tmp_dir=args["temp_s3_dir"],
        transformation_ctx="upsert_to_redshift_8d",
    )

    job.commit()
