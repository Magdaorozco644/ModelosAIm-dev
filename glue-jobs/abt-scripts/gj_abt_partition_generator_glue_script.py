################################################
################################################
################################################
#               IMPORTS & CONFIG               #
################################################
################################################
################################################

import awswrangler as wr
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
import json
import holidays
import boto3
import time
from io import StringIO
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# CREDENTIALS
SECRET_NAME = "REDSHIFT-CREDENTIALS"
REGION_NAME = "us-east-1"

# UNIVERSE ARGS
MIN_AGE_PAYER = 3
MAX_INACTIVE_TIME = 3
MIN_TOTAL_AMOUNT = 10000
MIN_TOTAL_TRANSACTIONS = 50

# LAG ARGS
RATES_MUMBER = 30
TX_CANCELLED_LAGS = 30
TX_RATIO_COUPON_TX_LAGS = 30
TX_LAGS = 30
MARGIN_LAGS = 10

# Date ARGS
DATE_LAG = 1
MAX_DAYS_PREDICT = 8
WINDOW = 30
HOLIDAYS_TO_EXCLUDE = [
    "is_fourth_of_july",
    "christmas_day",
    "new_year_day",
    "thanksgiving_day",
]

# TABLE NAMES
# TODO: change table name to prod.
DAILY_CHECK_GP = "daily_check_gp"  # "20240513_daily_check_gp"  # daily_check_gp
LAST_DAILY_FOREX = "last_daily_forex_country"  # "20240513_last_daily_forex_country"  # last_daily_forex_country
DAILY_SALES_CANCELLED = "daily_sales_count_cancelled_v2"  # "20240513daily_sales_count_cancelled"  # daily_sales_count_cancelled_v2

# Redshift DATA
REDSHIFT_FIRST_DATE = "2022-01-01"
REDSHIFT_UPDATE_DAYS = 90
REDSHIFT_COLUMNS = [
    "date",
    "payer_country",
    "payer",
    "country",
    "tx",
    "amount",
    "coupon_count",
    "gp",
    "margin",
    "max_feed_price",
    "ratio_coupon_tx",
    "tx_cancelled",
    "id_main_branch",
    "id_country",
]


# check input variables
class InputVaribleRequired(Exception):
    pass


# empty universe
class EmptyUniverse(Exception):
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
#               ABT GENERATION               #
##############################################
##############################################
##############################################


class ABT:

    def __init__(self, args) -> None:
        self.args = args
        self.logger = LoggerInit(args["LOG_LEVEL"]).logger
        self.logger.info("Init ABT Generation")
        self.partition_dt = self.check_date(key="process_date")
        self.args["end_date"] = self.check_date(key="end_date")
        self.logger.info(f"Process date: {self.partition_dt}")
        self.logger.info(
            f"Window time: {self.args['start_date']} - {self.args['end_date']}"
        )

    def check_date(self, key):
        """Validate input process date.

        Raises:
            InputVaribleRequired: process date must be None or in format YYYY-MM-DD
        """
        if self.args[key].upper() == "NONE":
            if key == "process_date":
                date = datetime.now().strftime("%Y-%m-%d")
            elif key == "end_date":
                date = (
                    datetime.now()
                    - timedelta(days=DATE_LAG)
                    + timedelta(days=MAX_DAYS_PREDICT)
                ).strftime("%Y-%m-%d")
        else:
            try:
                # CHeck format
                date = datetime.strptime(self.args[key], "%Y-%m-%d")
                # return as string
                date = date.strftime("%Y-%m-%d")
            except ValueError:
                self.logger.info("Invalid format date.")
                raise InputVaribleRequired(
                    f"The variable '{key}' must be in the format YYYY-MM-DD or 'None', please correct it."
                )

        return date

    def create_partition(self):
        # Create daily check dataframe
        self.logger.info("Create daily check")
        df, data_table_eqs, aux = self.create_daily_check_and_equivalence()
        self.logger.info(f"Daily check shape: {df.shape}")

        # Read last daily forex
        self.logger.info("Create last daily forex")
        rates = self.create_last_daily_forex()
        self.logger.info(f"Rates shape: {rates.shape}")
        # Filtering 'payer_country' based on Aging notebook
        self.logger.info("Filtering aging")
        df_aging = self.aging_filter(df)
        self.logger.info(f"DFF AGING shape: {df_aging.shape}")
        if df_aging.shape[0] == 0:
            self.logger.info("WARNING!!! EMPTY UNIVERSE!.")
            raise EmptyUniverse(
                "Warning! After apply aging_filter, the universe is empty."
            )
        # Applying aging filters
        df_filtered = df[df["payer_country"].isin(df_aging["payer_country"])]
        df_filtered["day"] = pd.to_datetime(df_filtered["day"])

        # Generate Lag rates
        self.logger.info("Generate lag and variation")
        rates = self.generate_lag_and_variation(rates, RATES_MUMBER)
        # No es necesario, ya esta creado en last_daily_forex
        # rates['country'] = rates['symbol'].map(rates_dict)

        self.logger.info(rates.columns)
        self.logger.info("Other dict \n")
        self.logger.info(df_filtered.columns)
        self.logger.info(f"Df Filtered shape: {df_filtered.shape}")

        # First df: rates df with universe filtered.
        self.logger.info("Merge")
        df1 = pd.merge(df_filtered, rates, on=["day", "country"], how="left")
        self.logger.info(f"Df merge1 shape: {df1.shape}")
        df1["date"] = pd.to_datetime(df1["date"])
        # Coupon ratio
        df1["ratio_coupon_tx"] = df1.coupon_count / df1.tx
        # Call the function and assign the result back to df1
        self.logger.info("df1 merge")
        self.logger.info(df1.info())
        self.logger.info("Generate coupon tx lags")
        df1 = self.generate_coupon_tx_lags(df1, TX_RATIO_COUPON_TX_LAGS)
        # Call the function and assign the result back to df1
        self.logger.info("Generate tx lags")
        df1 = self.generate_tx_lags(df1, TX_LAGS)
        # Call the function and assign the result back to df1
        self.logger.info("Generate margin lags")
        df1 = self.generate_margin_lags(df1, MARGIN_LAGS)

        self.logger.info(f"Df with lags shape: {df1.shape}")

        # Second df
        self.logger.info("Create canceled transactions")
        df2 = self.create_canceled_transactions(aux=aux, data_table_eqs=data_table_eqs)
        self.logger.info(f"Df canceled transactions shape: {df2.shape}")

        # Merge df1 with df2
        self.logger.info("Final merge")
        df_final = pd.merge(
            df1,
            df2,
            on=["date", "payer", "country", "payer_country", "amount"],
            how="inner",
        )
        df_final["date"] = pd.to_datetime(df_final["date"])
        self.logger.info(f"Df final shape: {df_final.shape}")

        # Applying holiday function
        df_final = self.mark_us_holidays(df_final)
        # Mark 4 July
        df_final = self.mark_fourth_july(df_final)
        # Aplicar la función calculate_var_30ds a cada fila del DataFrame
        df_final["var_30ds"] = df_final.apply(
            lambda row: self.calculate_var_30ds(WINDOW, row, df_final), axis=1
        )
        df_final["var_30ds"] = df_final["var_30ds"].fillna(0)
        # Crhistmas day
        df_final = self.mark_special_day(df_final, 12, 25, "christmas_day")
        # New Year day
        df_final = self.mark_special_day(df_final, 1, 1, "new_year_day")
        # Post Holidays
        df_final = self.mark_post_holiday(df_final)
        # Thanksgiving day
        df_final = self.mark_thanksgiving_day(df_final)

        # Mark Day of the Dead MEXICO
        # Create a boolean mask to filter by country and date
        mask = (
            (df_final["country"] == "MEXICO")
            & (df_final["date"].dt.month == 11)
            & (df_final["date"].dt.day == 2)
        )
        # Mark the Day of the Dead according to the mask
        df_final["day_of_the_dead"] = 0  # Initialize with 0
        df_final.loc[mask, "day_of_the_dead"] = 1  # Mark as 1 where the mask is True

        # Aplicar la función
        df_final = self.correcting_holidays(df_final, HOLIDAYS_TO_EXCLUDE)

        self.logger.info(f"Df final shape: {df_final.shape}")

        return df_final

    def create_equivalence_df(self):
        client = boto3.client("s3")
        # Specify the CSV file key
        csv_key = None
        # List objects in the S3 path
        response = client.list_objects(
            Bucket=self.args["bucket_name"],
            Prefix=self.args["prefix_equivalence_table"],
        )
        # Find the CSV file in the S3 path
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(self.args["endswith_equivalence_table"]):
                csv_key = obj["Key"]
                break
        # Check if CSV file is found
        if csv_key is not None:
            # Read CSV content from S3
            csv_response = client.get_object(
                Bucket=self.args["bucket_name"], Key=csv_key
            )
            csv_content = csv_response["Body"].read().decode("utf-8")

            # Transform CSV content to DataFrame
            data_table_eqs = pd.read_csv(StringIO(csv_content), sep=";")
            data_table_eqs["DATE_FROM"] = pd.to_datetime(
                data_table_eqs["DATE_FROM"]
            ).dt.date
            data_table_eqs["DATE_TO"] = pd.to_datetime(
                data_table_eqs["DATE_TO"]
            ).dt.date
            self.logger.info("CSV file loaded")
            return data_table_eqs
        else:
            self.logger.info("No CSV file found in the specified S3 path.")
            return None

    def apply_eq(self, row):
        if (
            row["_merge"] == "both"
            and row["date"] >= row["DATE_FROM"]
            and row["date"] <= row["DATE_TO"]
            and row["FLAG_ACTIVE"] == 1
        ):
            row["id_main_branch"] = row["ID_MAIN_BRANCH_PARENT"]
            row["id_country"] = row["ID_COUNTRY"]
            row["payer"] = row["PARENT_PAYER"]
            row["country"] = row["PARENT_COUNTRY"]
        return row

    def create_daily_check_and_equivalence(self):
        # Create Daily check dataframe
        database_name = "analytics"
        table_name = DAILY_CHECK_GP
        df = wr.athena.read_sql_table(table=table_name, database=database_name)
        # Create aux for id_main_branch and id_country
        aux = df.loc[:, ["payer", "country", "id_main_branch", "id_country"]]
        aux = aux.drop_duplicates(subset=["id_country", "id_main_branch"], keep="last")
        aux["payer_country"] = aux["payer"] + "_" + aux["country"]
        aux = aux.loc[:, ["payer_country", "id_main_branch", "id_country"]]

        # Create main id
        df["ID_FULL"] = df["id_country"].str.strip() + df["id_main_branch"].str.strip()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["day"] = pd.to_datetime(df["day"]).dt.date

        # Create equivalence table
        data_table_eqs = self.create_equivalence_df()
        self.logger.info(f"Equivalence shape: {data_table_eqs.shape}")
        # Create main id
        data_table_eqs["ID_FULL"] = (
            data_table_eqs["ID_COUNTRY"].str.strip()
            + data_table_eqs["ID_MAIN_BRANCH_CHILD"].str.strip()
        )
        # Adding Parent data (Name & Country)
        data_table_eqs = pd.merge(
            data_table_eqs,
            df[["payer", "country", "id_main_branch"]],
            left_on="ID_MAIN_BRANCH_PARENT",
            right_on="id_main_branch",
            how="left",
        ).drop_duplicates()
        # Renaming
        data_table_eqs = data_table_eqs.rename(
            columns={"payer": "PARENT_PAYER", "country": "PARENT_COUNTRY"}
        )
        data_table_eqs.drop("id_main_branch", axis=1, inplace=True)
        # Merge both
        df = pd.merge(df, data_table_eqs, on="ID_FULL", how="left", indicator=True)

        # Apply transform
        df = df.apply(self.apply_eq, axis=1)

        other_cols = ["payer", "country", "day"]
        variables_to_sum = ["tx", "amount", "coupon_count", "gp"]

        # Numeric columns
        df["tx"] = df["tx"].astype(int)
        df["coupon_count"] = df["coupon_count"].astype(int)
        df["amount"] = df["amount"].astype(float)
        df["gp"] = df["gp"].astype(float)

        # Grouping columns
        df_grouped = (
            df.groupby(["date", "id_country", "id_main_branch"])[variables_to_sum]
            .agg("sum")
            .reset_index()
        )

        df_non_numeric = (
            df.groupby(["date", "id_country", "id_main_branch"])[other_cols]
            .first()
            .reset_index()
        )
        eq_df = pd.merge(
            df_grouped, df_non_numeric, on=["date", "id_main_branch", "id_country"]
        )

        df = eq_df.copy()
        # Convert the 'date' column to datetime format
        df["date"] = pd.to_datetime(df["date"])
        df["day"] = pd.to_datetime(df["day"])
        # Grouping by 'payer' and 'country' concatenated for this level of granularity
        df["payer_country"] = df["payer"] + "_" + df["country"]
        print(df.columns)
        # Margin (when tx !=0)
        df["margin"] = df.apply(
            lambda row: row["gp"] / row["tx"] if row["tx"] != 0 else 0, axis=1
        )
        df["margin"] = df["margin"].apply(lambda x: float(x)).round(4)
        # Specify date range
        df = df[
            (df["date"] >= self.args["start_date"])
            & (df["date"] <= self.args["end_date"])
        ]
        # Defining Universe
        df = df[df["payer"] != "EXPIRED ORDERS"]
        df = df[df["amount"] != 0]  # Excluding 0 (flag A & Flag C), defined in EDA
        # Exclude special date
        # df = df[df["date"] != "2020-12-31"]  # Excluyo el 31-12-2020
        # Fill missing dates in df_filtered
        df_filled = self.fill_missing_dates_daily_check(
            df, self.args["start_date"], self.args["end_date"]
        )
        return df_filled, data_table_eqs, aux

    def fill_missing_dates_daily_check(self, df, start_date, end_date):
        """
        Fill missing dates in the DataFrame with zero values and ensure all date ranges are covered.

        Args:
            df (pandas.DataFrame): Input DataFrame with columns 'date', 'amount', 'tx_cancelled', 'payer_country', etc.
            start_date (str or datetime.date): Start date of the desired date range.
            end_date (str or datetime.date): End date of the desired date range.

        Returns:
            pandas.DataFrame: DataFrame with missing dates filled and all date ranges covered.
        """
        # Convertir la columna 'date' a tipo datetime si aún no lo está
        df["date"] = pd.to_datetime(df["date"])

        # Definir el rango de fechas deseado
        date_range = pd.date_range(start=start_date, end=end_date)

        # Obtener el rango de fechas mínimo y máximo para cada 'payer_country'
        payer_country_ranges = (
            df.groupby("payer_country")["date"].agg(["min", "max"]).reset_index()
        )
        payer_country_ranges["min"] = payer_country_ranges["min"].fillna(
            pd.to_datetime(start_date)
        )
        payer_country_ranges["max"] = payer_country_ranges["max"].fillna(
            pd.to_datetime(end_date)
        )

        # Combinar el DataFrame original con el DataFrame de todas las combinaciones de fechas
        df_filled = pd.DataFrame()
        for index, row in payer_country_ranges.iterrows():
            payer_country = row["payer_country"]
            start_payer = row["min"]
            ##APLICAR CAMBIO AL PIPELINE!!
            # end_payer = row['max']
            end_payer = end_date

            # Filtrar el DataFrame original por 'payer_country'
            df_payer = df[df["payer_country"] == payer_country]

            # Rellenar valores faltantes en el rango de fechas del 'payer_country'
            date_range_payer = pd.date_range(start=start_payer, end=end_payer)
            date_combinations = pd.DataFrame(
                {"date": date_range_payer, "payer_country": payer_country}
            )
            df_combined = pd.merge(
                date_combinations, df_payer, on=["date", "payer_country"], how="left"
            )

            # Rellenar valores faltantes con cero
            numeric_columns = ["amount", "coupon_count", "tx", "gp", "margin"]
            df_combined[numeric_columns] = df_combined[numeric_columns].fillna(0)

            # Rellenar valores faltantes en las columnas 'payer' y 'country' utilizando el método ffill
            df_combined[["payer", "country", "id_country", "id_main_branch"]] = (
                df_combined[
                    ["payer", "country", "id_country", "id_main_branch"]
                ].ffill()
            )
            # Agrego una columna extra que mantenga el ultimo dia en que opero ese payer
            # APLICAR CAMBIO AL PIPELINE!!
            df_combined["max_day"] = df_combined.day.max()
            # Rellenar valores faltantes en la columna 'day' con los valores de la columna 'date' cuando sea NaN
            df_combined["day"] = df_combined["day"].fillna(df_combined["date"])
            df_filled = pd.concat([df_filled, df_combined], ignore_index=True)

        return df_filled

    def create_last_daily_forex(self):
        # Read last_daily_forex
        database_name = "analytics"
        forex_table = LAST_DAILY_FOREX
        df_rates = wr.athena.read_sql_table(table=forex_table, database=database_name)
        # FOREX - Selecting columns & renaming
        df_rates["day"] = pd.to_datetime(df_rates["day"])
        # df_rates = df_rates[["day", "country", "max_feed_price", "id_country"]]
        df_rates = df_rates[["day", "country", "max_feed_price"]]
        return df_rates.sort_values(["country", "day"])

    def aging_filter(self, df):
        """
        Filter a DataFrame based on aging criteria described in aging.ipynb

        Args:
            df (pandas.DataFrame): Input DataFrame with columns 'date', 'payer_country', 'amount', and 'tx'.

        Returns:
            pandas.DataFrame: Filtered DataFrame containing only the rows that meet the aging criteria.
        """
        # Find the last date in the sample
        last_date_sample = df["date"].max()

        # Calculate the limit date, one day before the last date in the sample
        limit_date = last_date_sample - pd.Timedelta(days=self.args["date_lag"])

        # Aggregate data by 'payer_country'
        result = (
            df.groupby("payer_country")
            .agg(
                first_date=("day", "min"),
                last_date=("max_day", "max"),
                # last_date=('day', 'max'),
                total_amount=("amount", "sum"),
                total_transactions=("tx", "sum"),
            )
            .reset_index()
        )

        # Calculate age of payer
        result["age_payer"] = ((limit_date - result["first_date"]).dt.days / 30).round(
            2
        )

        # Calculate active time
        result["active_time"] = (
            (result["last_date"] - result["first_date"]).dt.days / 30
        ).round(2)

        # Calculate inactive time
        result["inactive_time"] = (
            (limit_date - result["last_date"]).dt.days / 30
        ).round(2)

        # Sort the DataFrame by 'total_amount' from highest to lowest
        result = result.sort_values(by="total_amount", ascending=False)

        # Filter the DataFrame based on conditions
        aging_universe = result.loc[
            (result.age_payer >= 3)
            & (result.inactive_time <= 3)
            & (result.total_amount > 10000)
            & (result.total_transactions > 50)
        ]

        return aging_universe

    def generate_lag_and_variation(self, df, num_lags):
        """
        Generate lagged values and variations for a given df

        Args:
            df (pandas.DataFrame): Input df with columns 'symbol' and 'feed_price'.
            num_lags (int): Number of lagged values to generate.

        Returns:
            pandas.DataFrame: df with lagged values and variations added as new columns.
        """
        # Create columns for each day's lag up to the defined maximum
        for i in range(1, num_lags + 1):
            col_name = f"rate_lag_{i}"
            # Shift the 'feed_price' column grouped by 'symbol'
            df[col_name] = df.groupby("country")["max_feed_price"].shift(i)

        # Calculate the variation columns between consecutive lags
        for i in range(1, num_lags):
            col_name = f"var_rate_lag_{i}"
            # Calculate the difference between consecutive lag columns
            df[col_name] = df[f"rate_lag_{i}"] - df[f"rate_lag_{i + 1}"]

        return df

    def generate_coupon_tx_lags(self, df, tx_count):
        """
        Generate lag columns for coupon_tx ratio

        Args:
        - df: DataFrame containing transaction data
        - tx_count: Number of periods for lag calculation

        Returns:
        - df: DataFrame with added lag and variation columns
        """
        # Sort the dataset based on country, payer, and date
        df = df.sort_values(by=["country", "payer", "date"])

        # Create columns for each day's lag up to the defined maximum
        for i in range(1, tx_count + 1):
            col_name = f"ratio_coupon_tx_lag_{i}"
            # Shift the 'ratio_coupon_tx' column grouped by 'country' and 'payer'
            df[col_name] = df.groupby(["country", "payer"])["ratio_coupon_tx"].shift(i)

        return df

    def generate_tx_lags(self, df, tx_count):
        """
        Generate lags columns for txs

        Args:
        - df: DataFrame containing transaction data
        - tx_count: Number of periods for lag calculation

        Returns:
        - df: DataFrame with added lag and variation columns
        """
        # Sort the dataset based on country, payer, and date
        df = df.sort_values(by=["country", "payer", "date"])

        # Create columns for each day's lag up to the defined maximum
        for i in range(1, tx_count + 1):
            col_name = f"tx_lag_{i}"
            # Shift the 'tx' column grouped by 'country' and 'payer'
            df[col_name] = df.groupby(["country", "payer"])["tx"].shift(i)

        return df

    def generate_margin_lags(self, df, margin_lags):
        """
        Generate lag columns for margin

        Args:
        - df: DataFrame containing transaction data
        - margin_lags: Number of periods for lag calculation

        Returns:
        - df: DataFrame with added lag columns for margin
        """
        # Sort the dataset based on country, payer, and date
        df = df.sort_values(by=["country", "payer", "date"])

        # Create columns for each day's lag up to the defined maximum
        for i in range(1, margin_lags + 1):
            col_name = f"margin_lag_{i}"
            # Shift the 'margin' column grouped by 'country' and 'payer'
            df[col_name] = df.groupby(["country", "payer"])["margin"].shift(i)

        return df

    def create_canceled_transactions(self, aux, data_table_eqs):
        ### EFFECT OF CANCELED TRANSACTIONS ###
        ##WE LOAD THE BASE WITH CANCELLATIONS
        database_name = "analytics"
        table2_name = DAILY_SALES_CANCELLED

        df2 = wr.athena.read_sql_table(table=table2_name, database=database_name)
        df2["date"] = pd.to_datetime(df2["date"])
        # Grouping by 'payer' and 'country' concatenated for this level of granularity
        df2["payer_country"] = df2["payer"] + "_" + df2["country"]
        # Specific date range
        df2 = df2[
            (df2["date"] >= self.args["start_date"])
            & (df2["date"] <= self.args["end_date"])
        ]

        # Merge with aux
        df_canc = pd.merge(df2, aux, on="payer_country", how="left")
        df_canc["ID_FULL"] = (
            df_canc["id_country"].str.strip() + df_canc["id_main_branch"].str.strip()
        )
        df_canc["date"] = pd.to_datetime(df_canc["date"]).dt.date
        df_canc = pd.merge(
            df_canc, data_table_eqs, on="ID_FULL", how="left", indicator=True
        )

        df_canc = df_canc.apply(self.apply_eq, axis=1)
        other_cols_canc = ["payer", "country", "day"]
        variables_to_sum_canc = ["tx_cancelled", "amount"]

        df_canc['amount'] = df_canc['amount'].astype('float')
        df_canc['tx_cancelled'] = df_canc['tx_cancelled'].astype('int')

        # Grouping columns
        df_grouped_canc = (
            df_canc.groupby(["date", "id_country", "id_main_branch"])[
                variables_to_sum_canc
            ]
            .agg("sum")
            .reset_index()
        )

        df_non_numeric_canc = (
            df_canc.groupby(["date", "id_main_branch", "id_country"])[other_cols_canc]
            .first()
            .reset_index()
        )
        eq_df_canc = pd.merge(
            df_grouped_canc,
            df_non_numeric_canc,
            on=["date", "id_main_branch", "id_country"],
        )

        df_canc = eq_df_canc.copy()
        df_canc["payer_country"] = df_canc["payer"] + "_" + df_canc["country"]

        # Call the function with the specified start_date and end_date
        df_full = self.fill_missing_dates(
            df_canc, self.args["start_date"], self.args["end_date"]
        )

        # Call the function and assign the result back to df2
        df2 = self.generate_tx_lags_and_variation(df_full, TX_CANCELLED_LAGS)
        df2["day"] = pd.to_datetime(df2["day"])
        df2["date"] = pd.to_datetime(df2["date"])

        return df2

    def fill_missing_dates(self, df, start_date, end_date):
        """
        Fill missing dates in the DataFrame with zero values and ensure all date ranges are covered.

        Args:
            df (pandas.DataFrame): Input DataFrame with columns 'date', 'amount', 'tx_cancelled', 'payer_country', etc.
            start_date (str or datetime.date): Start date of the desired date range.
            end_date (str or datetime.date): End date of the desired date range.

        Returns:
            pandas.DataFrame: DataFrame with missing dates filled and all date ranges covered.
        """
        # Create an empty DataFrame with the specified date range
        date_range = pd.date_range(start=start_date, end=end_date)
        df_fill = pd.DataFrame({"date": date_range, "amount": 0, "tx_cancelled": 0})
        df_fill["date"] = pd.to_datetime(df_fill["date"]).dt.date

        # Sort the original DataFrame by 'country', 'payer', and 'date'
        df = df.sort_values(by=["country", "payer", "date"])

        # Create an empty DataFrame to hold the result
        result_df = pd.DataFrame()

        # Loop through each 'payer_country'
        for payer_country in df["payer_country"].unique():
            # Filter DataFrame by 'payer_country'
            df_aux = df[df["payer_country"] == payer_country]

            # Combine df_aux (payer_country) with df_fill, keeping values from df_aux and filling missing dates
            merged_df = (
                df_aux.set_index("date")
                .combine_first(df_fill.set_index("date"))
                .reset_index()
            )

            # Fill missing values in specified columns
            columns_to_fill = ["payer", "country", "payer_country"]
            merged_df[columns_to_fill] = merged_df[columns_to_fill].ffill().bfill()

            # Concatenate the result with the final DataFrame
            result_df = pd.concat([result_df, merged_df], ignore_index=True)

        return result_df

    def generate_tx_lags_and_variation(self, df, tx_count):
        """
        Generate lag columns for cancelled transactions and their variations.

        Args:
        - df: DataFrame containing transaction data
        - tx_count: Number of periods for lag calculation

        Returns:
        - df: DataFrame with added lag and variation columns
        """
        # Sort the dataset based on country, payer, and date
        df = df.sort_values(by=["country", "payer", "date"])

        # Create columns for each day's lag up to the defined maximum
        for i in range(1, tx_count + 1):
            col_name = f"tx_cancelled_lag_{i}"
            # Shift the 'tx_cancelled' column grouped by 'country' and 'payer'
            df[col_name] = df.groupby(["country", "payer"])["tx_cancelled"].shift(i)

        # Calculate the variation columns between consecutive delays
        for i in range(1, tx_count):
            col_name = f"var_tx_cancelled_lag_{i}"
            # Calculate the difference between consecutive lag columns
            df[col_name] = df[f"tx_cancelled_lag_{i}"] - df[f"tx_cancelled_lag_{i + 1}"]

        return df

    def mark_us_holidays(self, df):
        """
        Mark US holidays, excluding specified holidays and those with 'Observed'.

        Args:
            df (DataFrame): DataFrame containing a 'date' column in datetime format.

        Returns:
            DataFrame: DataFrame with an additional 'is_holiday' column, where 1 indicates a US holiday and 0 otherwise.
        """
        # Obtener las fechas mínima y máxima del DataFrame
        min_date = df["date"].min().year
        max_date = df["date"].max().year + 1
        print(min_date, max_date)

        # Cargar los feriados de Estados Unidos
        us_holidays = holidays.US(years=range(min_date, max_date))

        # Lista de días festivos a excluir
        holidays_to_exclude = ["Washington's Birthday", "Columbus Day"]

        # Filtrar los días festivos que deben ser excluidos
        filtered_holidays = {
            date: name
            for date, name in us_holidays.items()
            if name not in holidays_to_exclude and "observed" not in name.lower()
        }
        #    print(filtered_holidays) # Habilitando este print puedo ver que feriados son los que estamos marcando

        # Crear una lista de fechas de feriados
        holidays_list = list(filtered_holidays.keys())

        # Marcar los días festivos en el DataFrame
        df["is_holiday"] = df["date"].isin(holidays_list).astype(int)

        return df

    def mark_fourth_july(self, df):
        """
        Mark the Fourth of July in the DataFrame.

        Args:
            df (DataFrame): DataFrame containing a 'date' column in datetime format.

        Returns:
            DataFrame: DataFrame with an additional 'is_fourth_of_july' column.
        """
        # Check if the date is the Fourth of July
        df["is_fourth_of_july"] = (
            (df["date"].dt.month == 7) & (df["date"].dt.day == 4)
        ).astype(int)

        return df

    def calculate_var_30ds(self, window, row, df_final):
        """
        Calculate the variable 'var_30ds' based on the average amount in the last 30 days.

        Parameters:
        window (int): The window size in days for the calculation.
        row (pandas.Series): The row containing the data for the current observation.
        df_final (pandas.DataFrame): The DataFrame containing the final dataset.

        Returns:
        float or None: The calculated variable 'var_30ds' if applicable, else None.
        """
        # Check if the current day is a holiday
        if row["is_holiday"] == 1:
            # Filter the DataFrame to get only the last 30 days for the current 'payer_country'
            filter_condition = (
                (df_final["payer_country"] == row["payer_country"])
                & (df_final["date"] >= (row["date"] - pd.Timedelta(days=window)))
                & (df_final["date"] < row["date"])
            )
            filtered_df = df_final[filter_condition]

            # Calculate the average amount for the current 'payer_country' in the last 30 days
            avg_amount = filtered_df["amount"].mean()

            # Print filtered DataFrame for debugging
            #        if (row['payer_country'] == 'ELEKTRA (MEXICO)_MEXICO') and (row['date'] == datetime.strptime('2023-09-04', '%Y-%m-%d')):
            #            print(filtered_df)

            # Calculate var_30ds according to the specified formula
            if avg_amount != 0 and row["amount"] != 0:
                var_30ds = (
                    float(row["amount"]) / float(avg_amount) - 1
                )  # Convert avg_amount to float before division
                return var_30ds
            else:
                return 0
        else:
            return None

    def mark_special_day(self, df, month, day, name):
        """
        Marks a special day in the DataFrame.

        This function identifies the specified day for each year present in the DataFrame
        and marks it with the given name in the DataFrame.

        Args:
        df (DataFrame): The DataFrame containing the date column.
        month (int): The month of the special day (1-12).
        day (int): The day of the special day (1-31).
        name (str): The name to mark the special day in the DataFrame.

        Returns:
        DataFrame: The DataFrame with the special day marked.

        Raises:
        ValueError: If the DataFrame does not contain a 'date' column.
        """
        # Verificar si la columna 'date' existe en el DataFrame
        if "date" not in df.columns:
            raise ValueError("DataFrame debe contener una columna 'date'.")

        # Crear una nueva columna para marcar el día especial
        df[name] = 0

        # Iterar sobre cada año presente en el DataFrame
        for year in df["date"].dt.year.unique():
            # Marcar el día especial para el año actual
            special_date = datetime(year, month, day)
            # Marcar filas correspondientes al día especial para el año actual
            df.loc[
                (df["date"].dt.year == year)
                & (df["date"].dt.month == month)
                & (df["date"].dt.day == day),
                name,
            ] = 1

        return df

    def thanksgiving_date(self, year):
        """
        Calcula la fecha de Acción de Gracias para un año dado.

        Args:
        year (int): El año para el que se quiere calcular la fecha de Acción de Gracias.

        Returns:
        datetime: La fecha de Acción de Gracias para el año dado.
        """
        # Se sabe que Acción de Gracias es el cuarto jueves de noviembre
        # Se determina el día del primer jueves de noviembre
        first_of_november = datetime(year, 11, 1)
        while first_of_november.weekday() != 3:  # 3 representa el jueves
            first_of_november += timedelta(days=1)

        # Luego se suma 3 semanas (21 días) para obtener el cuarto jueves
        thanksgiving = first_of_november + timedelta(weeks=3)
        return thanksgiving

    def mark_thanksgiving_day(self, df):
        """
        Marca el Día de Acción de Gracias en el DataFrame.

        Esta función identifica el Día de Acción de Gracias en noviembre para cada año presente en el DataFrame
        y lo marca en el DataFrame.

        Args:
        df (DataFrame): El DataFrame que contiene la columna de fecha.

        Returns:
        DataFrame: El DataFrame con el Día de Acción de Gracias marcado.

        Raises:
        ValueError: Si el DataFrame no contiene una columna 'date'.
        """
        # Verificar si la columna 'date' existe en el DataFrame
        if "date" not in df.columns:
            raise ValueError("El DataFrame debe contener una columna 'date'.")

        # Crear una nueva columna para marcar el Día de Acción de Gracias
        df["thanksgiving_day"] = 0

        # Iterar sobre cada año presente en el DataFrame
        for year in df["date"].dt.year.unique():
            # Calcular la fecha de Acción de Gracias para el año actual
            thanksgiving = self.thanksgiving_date(year)
            # Marcar filas correspondientes a Acción de Gracias para el año actual
            df.loc[
                (df["date"].dt.year == year)
                & (df["date"].dt.month == 11)
                & (df["date"].dt.day == thanksgiving.day),
                "thanksgiving_day",
            ] = 1
            # Marcar 'is_holiday' como 0 cuando se marca 1 en 'thanksgiving_day'
            df.loc[
                (df["date"].dt.year == year)
                & (df["date"].dt.month == 11)
                & (df["date"].dt.day == thanksgiving.day),
                "is_holiday",
            ] = 0

        return df

    def mark_post_holiday(self, df):
        """
        Mark days after holidays. Usually post holiday days tend to rise sales

        Args:
            df (DataFrame): DataFrame containing a 'is_holiday' column indicating holidays.

        Returns:
            DataFrame: DataFrame with an additional 'post_holiday' column, where 1 indicates a day after a holiday.
        """
        post_holiday = []
        for idx, row in df.iterrows():
            is_holiday = row["is_holiday"]
            if is_holiday == 1:
                post_holiday.append(0)
            else:
                if idx > 0 and df.loc[idx - 1, "is_holiday"] == 1:
                    post_holiday.append(1)
                else:
                    post_holiday.append(0)
        df["post_holiday"] = post_holiday

        return df

    def correcting_holidays(self, df_final, holidays_to_exclude):
        """
        Corrects the holiday markings in the DataFrame and adjusts the 'is_holiday' column based on exceptions.

        Args:
            df_final (DataFrame): DataFrame containing a 'date' column in datetime format and the 'is_holiday' column.
            separate_flags (list): List of column names where an exception should be considered.

        Returns:
            DataFrame: DataFrame with 'is_holiday' adjusted according to exceptions.
        """
        # Check if the 'date' column exists in the DataFrame
        if "date" not in df_final.columns:
            raise ValueError("The DataFrame must contain a 'date' column.")

        # Iterate over the holiday columns where exceptions should be considered
        for flag in holidays_to_exclude:
            if flag not in df_final.columns:
                raise ValueError(
                    f"The column '{flag}' does not exist in the DataFrame."
                )

            # If the holiday column has a value of 1, mark 'is_holiday' as 0 for the same row
            df_final.loc[df_final[flag] == 1, "is_holiday"] = 0

        return df_final

    # Fill missing values with zeros in numeric columns
    # Completa los valores faltantes con ceros de las columnas numericas
    def fill_missing_with_zeros(self, df, to_exclude):
        # Obtén una lista de las columnas numéricas
        numeric_columns = df.select_dtypes(include="number").columns
        # Filter
        numeric_columns_filtered = list(set(numeric_columns) - set(to_exclude))
        # Completa los valores faltantes con ceros
        df[numeric_columns_filtered] = df[numeric_columns_filtered].fillna(value=0)

        return df

    def convert_to_numeric(self, df, exlude_columns):
        for x in df.columns.to_list():
            # print ("column to float: ", x) #debug
            if x not in exlude_columns:
                df[x] = pd.to_numeric(df[x], errors="coerce")
        return df

    def save_partition(self, df):
        self.logger.info(df.info())
        self.logger.info(df.columns)
        # Define columns to exclude
        exclude_convert = [
            "date",
            "payer_country",
            "payer",
            "country",
            "id_main_branch",
            "id_main_branch_x",
            "id_country",
            "id_country_x",
            "day_y",
            "day_x",
            "id_country_y",
            "id_country_x",
        ]
        ## Fill nan numeric values with 0
        df = self.fill_missing_with_zeros(df, exclude_convert)
        ## Convert to numeric
        df = self.convert_to_numeric(df, exclude_convert)
        # Filling NaN in exogenous and lags
        # df = df.fillna(0, inplace=True)

        df = df.drop(columns=["id_country_y"], errors="ignore")
        ## Fill nan numeric values with 0
        df = self.fill_missing_with_zeros(df, exclude_convert)
        # Rename columns
        df.rename(
            columns={
                "id_main_branch_x": "id_main_branch",
                "id_country_x": "id_country",
            },
            inplace=True,
        )
        for str_col in [
            "payer_country",
            "payer",
            "country",
            "id_main_branch",
            "id_country",
        ]:
            df[str_col] = df[str_col].fillna(value="")
        self.logger.info("fil missing to numeric")
        self.logger.info(df[df["max_feed_price"] != 0.0]["country"].unique())
        self.logger.info(df.info())
        self.logger.info(df.columns)
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{self.args['bucket_name']}/abt_parquet/dt={self.partition_dt}",
            dataset=True,
            index=False,
            mode="overwrite_partitions",
            compression="snappy",
        )
        return df


### UTILS
def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response["SecretString"]
    secret_ = json.loads(secret)
    return secret_


def check_if_table_exist(args):
    secret = get_secret(SECRET_NAME, REGION_NAME)
    # SQLServer string connection
    jdbc_viamericas = secret["connectionString"]
    qryStr = f"""SELECT * FROM {args["database"]}.{args["schema"]}.{args["table_name"]} limit 10"""
    table_exist = None
    print(qryStr)
    try:
        jdbcDF = (
            spark.read.format("jdbc")
            .option("url", jdbc_viamericas)
            .option("driver", "com.amazon.redshift.jdbc42.Driver")
            .option("query", qryStr)
            .option("user", secret["username"])
            .option("password", secret["password"])
            .option("fetchsize", 1000)
            .load()
        )
        table_exist = True
    except Exception as e:
        print(e)
        print("Retry because Remote cluster is initializing. Try again in 60 secs.")
        time.sleep(60)
        try:
            jdbcDF = (
                spark.read.format("jdbc")
                .option("url", jdbc_viamericas)
                .option("driver", "com.amazon.redshift.jdbc42.Driver")
                .option("query", qryStr)
                .option("user", secret["username"])
                .option("password", secret["password"])
                .option("fetchsize", 1000)
                .load()
            )
            table_exist = True
        except Exception as e:
            # Handle the exception if the table does not exist
            if "does not exist in the database." in str(e):
                table_exist = False
            print("Table does not exist.")
            print(e)

    print(f"La tabla existe?? {table_exist}")
    return table_exist


def first_time_creation(args, glueContext, rds_conn, df_final_frame):
    # Create stage temp table with schema.
    pre_query = """
    begin;
    CREATE TABLE if not exists {database}.{schema}.{table_name}  (LIKE public.stage_table_temporary_abt);
    TRUNCATE TABLE {database}.{schema}.{table_name};
    end;
    """
    post_query = """
    begin;
    insert into {database}.{schema}.{table_name} select * from public.stage_table_temporary_abt;
    drop table public.stage_table_temporary_abt;
    end;
    """
    pre_query = pre_query.format(
        database=args["database"],
        schema=args["schema"],
        table_name=args["table_name"],
    )
    post_query = post_query.format(
        database=args["database"],
        schema=args["schema"],
        table_name=args["table_name"],
    )
    print(f"Pre query : {pre_query}")
    print(f"Post query : {post_query}")
    # Send data to Redshift
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=df_final_frame,
        catalog_connection=rds_conn,
        connection_options={
            "database": args["database"],
            "dbtable": f"public.stage_table_temporary_abt",
            "preactions": pre_query,
            "postactions": post_query,
        },
        redshift_tmp_dir=args["temp_s3_dir"],
        transformation_ctx="upsert_to_redshift",
    )


def insert_last_n_partitions(args, glueContext, rds_conn, df_final_frame):
    query = """
    begin;
    delete from {database}.{schema}.{table_name} using public.stage_table_temporary_abt where public.stage_table_temporary_abt.date = {database}.{schema}.{table_name}.date;
    insert into {database}.{schema}.{table_name} select * from public.stage_table_temporary_abt;
    drop table public.stage_table_temporary_abt;
    end;
    """

    post_query = query.format(
        database=args["database"],
        schema=args["schema"],
        table_name=args["table_name"],
    )
    print(f"Post query : {post_query}")
    # Send data to Redshift
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=df_final_frame,
        catalog_connection=rds_conn,
        connection_options={
            "database": args["database"],
            "dbtable": f"public.stage_table_temporary_abt",
            "postactions": post_query,
        },
        redshift_tmp_dir=args["temp_s3_dir"],
        transformation_ctx="upsert_to_redshift",
    )


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
        "start_date": "__required__",
        "end_date": "__required__",
        "database": "__required__",
        "schema": "__required__",
        "table_name": "__required__",
        "temp_s3_dir": "__required__",
        "upload_redshift": "__required__",
        "prefix_equivalence_table": "__required__",
        "endswith_equivalence_table": "__required__",
        "process_date": "None",
        "date_lag": DATE_LAG,
    }
    # prefix_equivalence_table -> PAYERS_EQUIVALENCE_TABLE
    # endswith_equivalence_table -> PAYERS_EQUIVALENCE_TABLE.csv
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    parser = ArgsGet(my_default_args)
    args = parser.loaded_args
    args["date_lag"] = int(args["date_lag"])
    # Object ABT
    abt = ABT(args=args)
    # Create abt partition (df)
    abt_partition = abt.create_partition()
    # Save partition
    df = abt.save_partition(df=abt_partition)
    print("Partition saved.")
    if args["upload_redshift"].upper() == "TRUE":
        # Check if table exist
        table_exist = check_if_table_exist(args=args)
        # Chose selected columns
        df = df[REDSHIFT_COLUMNS]
        if not table_exist:
            # Filter data historical
            df = df[df["date"] > REDSHIFT_FIRST_DATE]
        else:
            # Send last n days
            date_min = (datetime.now() - timedelta(days=REDSHIFT_UPDATE_DAYS)).strftime(
                "%Y-%m-%d"
            )
            df = df[df["date"] > date_min]
        # df = wr.s3.read_parquet('s3://viamericas-datalake-dev-us-east-1-283731589572-analytics/abt_parquet/dt=2024-05-08/5830aff9a4c94cd6b88641d29d3ccd36.snappy.parquet')
        # Create SparkDataframe
        df_final = spark.createDataFrame(df)
        # Detect numeric columns
        numeric_cols = [
            c[0] for c in df_final.dtypes if c[1] in ["bigint", "double", "float"]
        ]
        # Fill numeric values in spark dataframe
        df_filled = df_final.fillna(0, subset=numeric_cols)

        # Convert to Frame to upload to Redshift
        df_final_frame = DynamicFrame.fromDF(df_filled, glueContext, "df_final")
        # Redshift connection
        rds_conn = "via-redshift-connection"

        # Check if table exist or not
        if not table_exist:
            # Create tale for first time
            first_time_creation(args, glueContext, rds_conn, df_final_frame)
        else:
            # Only insert last 90 days
            insert_last_n_partitions(args, glueContext, rds_conn, df_final_frame)

        job.commit()
