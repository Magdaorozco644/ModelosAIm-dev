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
from datetime import datetime
import logging
import sys
import boto3
from io import BytesIO
import joblib
from awsglue.utils import getResolvedOptions


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
                    f"The variable {k} is required and was no setted, please pass it as argument '--{k} value'"
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
        self.logger.info(f"Process date: {self.partition_dt}")
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
        # Process 2d
        self.logger.info("Processing 2d model.")
        df_2d = self.process_model_ndays(
            days=2, prefix=self.args["model_2d_prefix"], data=data
        )
        # Process 8d
        self.logger.info("Processing 8d model.")
        df_8d = self.process_model_ndays(
            days=8, prefix=self.args["model_2d_prefix"], data=data
        )
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

        self.save_df(df=df_final_2d, name="predictions_2d.parquet")
        self.save_df(df=df_final_8d, name="predictions_8d.parquet")

    def save_df(self, df, name):
        path_s3 = (
            "s3://" + self.args["bucket_name"] + self.args["prefix_inference_name"] + name
        )
        self.logger.info(f"Save inference in: {path_s3}")
        # Guarda el DataFrame en formato Parquet en S3
        response = wr.s3.to_parquet(df, path=path_s3, dataset=True, index=False)
        self.logger.info(f"Response: {response}")

    # TODO: check si tomo la ultima particion o que.
    def read_daily_check_gp(self):
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
        pkl_files = self.list_pkl_objects(prefix=prefix)
        # flag
        i = 1
        # Iterate over pkl files
        for file_key in pkl_files:
            # Extract payer_country from file_key
            payer_country = file_key.split("/")[2]
            self.logger.info(f"Payer country: {payer_country}")

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
                self.logger.info(
                    "\033[1;31m" + f"Error processing {payer_country}" + "\033[0m"
                )
                predictions = [0, 0]
                df_temp = pd.DataFrame(
                    {
                        "index": [test_date, test_date + pd.Timedelta(days=1)],
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
        return temp_df

    def list_pkl_objects(self, prefix: str):
        """_summary_

        Args:
            prefix (str): _description_. 'MODEL_2d_'
        """
        elements = self.s3_client.list_objects(
            Bucket=self.args["bucket_name"], Prefix=self.args["prefix_models"]
        )

        # Listing pkl files
        pkl_files = [
            obj["Key"]
            for obj in elements.get("Contents", [])
            if obj["Key"].endswith(".pkl") and (obj["Key"].startswith(prefix))
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
        "prefix_models": "__required__",
        "model_2d_prefix": "__required__",
        "model_8d_prefix": "__required__",
        "process_date": "None",
    }

    parser = ArgsGet(my_default_args)
    args = parser.loaded_args
    # Object Inference
    prediction = Inference(args=args)
    prediction.process_payer()
