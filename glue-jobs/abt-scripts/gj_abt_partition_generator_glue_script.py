################################################
################################################
################################################
#               IMPORTS & CONFIG               #
################################################
################################################
################################################

import awswrangler as wr
import pandas as pd
from datetime import datetime
import logging
import sys
import holidays
from awsglue.utils import getResolvedOptions

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

# Date ARGS
DATE_LAG = 1


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
#               ABT GENERATION               #
##############################################
##############################################
##############################################


class ABT:

    def __init__(self, args) -> None:
        self.args = args
        self.logger = LoggerInit(args["LOG_LEVEL"]).logger
        self.logger.info("Init ABT Generation")
        self.partition_dt = self.check_date()
        self.logger.info(f'Process date: {self.partition_dt}')

    def check_date(self):
        """Validate input process date.

        Raises:
            InputVaribleRequired: process date must be None or in format YYYY-MM-DD
        """
        if self.args["process_date"].upper() == "NONE":
            partition_dt = datetime.now().strftime("%Y-%m-%d")
        else:
            try:
                partition_dt = datetime.strptime(
                    self.args["process_date"], "%Y-%m-%d"
                )
            except ValueError:
                self.logger("Invalid format date.")
                raise InputVaribleRequired(
                    f"The variable 'process_date' must be in the format YYYY-MM-DD or 'None', please correct it."
                )
        
        return partition_dt

    def create_partition(self):
        # Create daily check dataframe
        self.logger.info('Create daily check')
        df = self.create_daily_check()
        # Read last daily forex
        self.logger.info('Create last daily forex')
        rates = self.create_last_daily_forex()

        # Filtering 'payer_country' based on Aging notebook
        self.logger.info('Filtering aging')
        df_aging = self.aging_filter(df)
        # Applying aging filters
        df_filtered = df[df["payer_country"].isin(df_aging["payer_country"])]
        df_filtered["date"] = pd.to_datetime(df_filtered["date"])

        # Generate Lag rates
        self.logger.info('Generate lag and variation')
        rates = self.generate_lag_and_variation(rates, RATES_MUMBER)
        # No es necesario, ya esta creado en last_daily_forex
        # rates['country'] = rates['symbol'].map(rates_dict)

        self.logger.info(rates.columns)
        self.logger.info("Other dict \n")
        self.logger.info(df_filtered.columns)

        # First df: rates df with universe filtered.
        self.logger.info('Mege')
        df1 = pd.merge(df_filtered, rates, on=["date", "country"], how="left")
        # Coupon ratio
        df1["ratio_coupon_tx"] = df1.coupon_count / df1.tx
        # Call the function and assign the result back to df1
        self.logger.info('df1 merge')
        self.logger.info(df1.info())
        self.logger.info('Generate coupon tx lags')
        df1 = self.generate_coupon_tx_lags(df1, TX_RATIO_COUPON_TX_LAGS)
        # Call the function and assign the result back to df1
        self.logger.info('Generate tx lags')
        df1 = self.generate_tx_lags(df1, TX_LAGS)

        # Second df
        self.logger.info('Create canceled transactions')
        df2 = self.create_canceled_transactions()

        # Merge df1 with df2
        self.logger.info('Final merge')
        df_final = pd.merge(
            df1, df2, on=["date", "payer", "country", "amount"], how="outer"
        )
        df_final["date"] = pd.to_datetime(df_final["date"])

        # Applying holiday function
        df_final = self.mark_us_holidays(df_final)
        self.logger.info(
            f"Holydays: {df_final[df_final['is_holiday'] == 1]['date'].unique()}"
        )

        return df_final

    def create_daily_check(self):
        # Create Daily check dataframe
        database_name = "analytics"
        table_name = "daily_check_gp"
        df = wr.athena.read_sql_table(table=table_name, database=database_name)
        # Convert the 'date' column to datetime format
        df["date"] = pd.to_datetime(df["date"])
        # Grouping by 'payer' and 'country' concatenated for this level of granularity
        df["payer_country"] = df["payer"] + "_" + df["country"]
        # Defining Universe
        df = df[df["amount"] != 0]  # Excluding 0 (flag A & Flag C), defined in EDA
        # Exclude special date
        df = df[df["date"] != "2020-12-31"]  # Excluyo el 31-12-2020
        return df

    def create_last_daily_forex(self):
        # Read last_daily_forex
        database_name = "analytics"
        forex_table = "last_daily_forex_country"
        df_rates = wr.athena.read_sql_table(table=forex_table, database=database_name)
        # FOREX - Selecting columns & renaming
        df_rates["day"] = pd.to_datetime(df_rates["day"])
        df_rates = df_rates.rename(
            columns={"day": "date", "max_feed_price": "feed_price"}
        )
        df_rates = df_rates.loc[:, ["date", "feed_price", "symbol", "country"]]
        return df_rates

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
                first_date=("date", "min"),
                last_date=("date", "max"),
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
            (result.age_payer >= MIN_AGE_PAYER)
            & (result.inactive_time <= MAX_INACTIVE_TIME)
            & (result.total_amount > MIN_TOTAL_AMOUNT)
            & (result.total_transactions > MIN_TOTAL_TRANSACTIONS)
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
            df[col_name] = df.groupby("symbol")["feed_price"].shift(i)

        # Calculate the variation columns between consecutive lags
        for i in range(1, num_lags):
            col_name = f"var_rate_lag_{i}"
            # Calculate the difference between consecutive lag columns
            df[col_name] = df[f"rate_lag_{i}"] - df[f"rate_lag_{i + 1}"]

        #self.logger.info(df.info())
        #df = df.fillna(0)
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
        #self.logger.info(f'Previous fillna: \n {df.columns}')
        #df = df.fillna(0)
        #self.logger.info(f'Info fillna: \n {df.columns}')
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

        #df = df.fillna(0)
        return df

    def create_canceled_transactions(self):
        ### EFFECT OF CANCELED TRANSACTIONS ###
        ##WE LOAD THE BASE WITH CANCELLATIONS
        database_name = "analytics"
        table2_name = "daily_sales_count_cancelled_v2"

        df2 = wr.athena.read_sql_table(table=table2_name, database=database_name)
        df2["date"] = pd.to_datetime(df2["date"])
        # Grouping by 'payer' and 'country' concatenated for this level of granularity
        df2["payer_country"] = df2["payer"] + "_" + df2["country"]

        # Call the function with the specified start_date and end_date
        df_full = self.fill_missing_dates(
            df2, self.args["start_date"], self.args["end_date"]
        )

        # Call the function and assign the result back to df2
        df2 = self.generate_tx_lags_and_variation(df_full, TX_CANCELLED_LAGS)

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

        #df = df.fillna(0)
        return df

    def mark_us_holidays(self, df):
        """
        Mark US holidays

        Args:
            df (df): DataFrame containing a 'date' column in datetime format.

        Returns:
            DataFrame: DataFrame with an additional 'is_holiday' column, where 1 indicates a US holiday and 0 otherwise.
        """
        # Load US holidays
        us_holidays = holidays.US()

        # Define a function to check if a date is a holiday in the US
        def is_us_holiday(date):
            return 1 if date in us_holidays else 0

        # Apply the function to the 'date' column and create a new 'is_holiday' column
        df["is_holiday"] = df["date"].apply(is_us_holiday)

        return df

    # Fill missing values with zeros in numeric columns
    # Completa los valores faltantes con ceros de las columnas numericas
    def fill_missing_with_zeros(self,df):
        # Obtén una lista de las columnas numéricas
        numeric_columns = df.select_dtypes(include="number").columns

        # Completa los valores faltantes con ceros
        df[numeric_columns] = df[numeric_columns].fillna(0)

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
            'payer',
            'country',
            'date',
            'day',
            'day_x',
            'payer_country_x',
            'symbol',
            'payer_country_y']
        # Fill nan numeric values with 0
        df = self.fill_missing_with_zeros(df)
        # Convert to numeric
        df = self.convert_to_numeric(df,exclude_convert)

        wr.s3.to_parquet(
            df=df,
            path=f"s3://{self.args['bucket_name']}/abt_parquet/dt={self.partition_dt}",
            dataset=True,
            index=False,
            mode='overwrite_partitions',
            compression='snappy'
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
        "process_date": "None",
        "date_lag": DATE_LAG,
    }

    parser = ArgsGet(my_default_args)
    args = parser.loaded_args
    args["date_lag"] = int(args["date_lag"])
    # Object ABT
    abt = ABT(args=args)
    # Create abt partition (df)
    abt_partition = abt.create_partition()
    # Save partition
    abt.save_partition(df=abt_partition)


"""
#Write table in parquet
df_final = df_final.astype(str)
wr_response =  wr.s3.to_parquet(
df=df_final,
path=f's3://viamericas-datalake-dev-us-east-1-283731589572-raw/test_abt/',
dataset=True,
mode='overwrite_partitions',
database='viamericas',
table=f'test_abt',
partition_cols=['day'],
concurrent_partitioning=True,
index=False,
schema_evolution=True,
compression = 'snappy')
"""
