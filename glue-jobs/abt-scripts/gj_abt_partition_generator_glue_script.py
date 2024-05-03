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
import holidays
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

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
WINDOW = 30
HOLIDAYS_TO_EXCLUDE = ["07-04", "12-25", "01-01"]  # Formato mes-día


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
        self.partition_dt = self.check_date(key='process_date')
        self.args['end_date'] = self.check_date(key='end_date')
        self.logger.info(f"Process date: {self.partition_dt}")

    def check_date(self, key):
        """Validate input process date.

        Raises:
            InputVaribleRequired: process date must be None or in format YYYY-MM-DD
        """
        if self.args[key].upper() == "NONE":
            if key == 'process_date':
                date = datetime.now().strftime("%Y-%m-%d")
            elif key == 'end_date':
                date = (datetime.now() - timedelta(days=DATE_LAG)).strftime("%Y-%m-%d")
        else:
            try:
                date = datetime.strptime(self.args[key], "%Y-%m-%d")
            except ValueError:
                self.logger.info("Invalid format date.")
                raise InputVaribleRequired(
                    f"The variable '{key}' must be in the format YYYY-MM-DD or 'None', please correct it."
                )

        return date

    def create_partition(self):
        # Create daily check dataframe
        self.logger.info("Create daily check")
        df = self.create_daily_check()
        # Read last daily forex
        self.logger.info("Create last daily forex")
        rates = self.create_last_daily_forex()

        # Filtering 'payer_country' based on Aging notebook
        self.logger.info("Filtering aging")
        df_aging = self.aging_filter(df)
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

        # First df: rates df with universe filtered.
        self.logger.info("Mege")
        df1 = pd.merge(df_filtered, rates, on=["day", "country"], how="left")
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

        # Second df
        self.logger.info("Create canceled transactions")
        df2 = self.create_canceled_transactions()

        # Merge df1 with df2
        self.logger.info("Final merge")
        df_final = pd.merge(
            df1,
            df2,
            on=["date", "payer", "country", "payer_country", "amount"],
            how="inner",
        )
        df_final["date"] = pd.to_datetime(df_final["date"])

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

        # Aplicar la función
        df_final = self.correcting_holidays(df_final, HOLIDAYS_TO_EXCLUDE)

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
        df["day"] = pd.to_datetime(df["day"])
        # Grouping by 'payer' and 'country' concatenated for this level of granularity
        df["payer_country"] = df["payer"] + "_" + df["country"]
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
        return df_filled

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
        #'payer_country','id_country','id_main_branch'

        payer_country_ranges = (
            df.groupby(["payer_country", "id_country", "id_main_branch"])["date"]
            .agg(["min", "max"])
            .reset_index()
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
            end_payer = row["max"]
            payer_id_country = row["id_country"]
            payer_id_main_branch = row["id_main_branch"]

            # Filtrar el DataFrame original por 'payer_country'
            df_payer = df[df["payer_country"] == payer_country]

            # Rellenar valores faltantes en el rango de fechas del 'payer_country'
            date_range_payer = pd.date_range(start=start_payer, end=end_payer)
            date_combinations = pd.DataFrame(
                {
                    "date": date_range_payer,
                    "payer_country": payer_country,
                    "id_country": payer_id_country,
                    "id_main_branch": payer_id_main_branch,
                }
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
                    ["payer", "country", "id_country_x", "id_main_branch_x"]
                ].ffill()
            )

            # Rellenar valores faltantes en la columna 'day' con los valores de la columna 'date' cuando sea NaN
            df_combined["day"] = df_combined["day"].fillna(df_combined["date"])

            df_filled = pd.concat([df_filled, df_combined], ignore_index=True)

        df_filled = df_filled.drop(
            columns=[
                "id_country_x",
                "id_country_y",
                "id_main_branch_x",
                "id_main_branch_y",
            ]
        )
        return df_filled

    def create_last_daily_forex(self):
        # Read last_daily_forex
        database_name = "analytics"
        forex_table = "last_daily_forex_country"
        df_rates = wr.athena.read_sql_table(table=forex_table, database=database_name)
        # FOREX - Selecting columns & renaming
        df_rates["day"] = pd.to_datetime(df_rates["day"])
        df_rates = df_rates[["day", "country", "max_feed_price", "id_country"]]
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
                last_date=("day", "max"),
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

    def create_canceled_transactions(self):
        ### EFFECT OF CANCELED TRANSACTIONS ###
        ##WE LOAD THE BASE WITH CANCELLATIONS
        database_name = "analytics"
        table2_name = "daily_sales_count_cancelled_v2"

        df2 = wr.athena.read_sql_table(table=table2_name, database=database_name)
        df2["date"] = pd.to_datetime(df2["date"])
        # Grouping by 'payer' and 'country' concatenated for this level of granularity
        df2["payer_country"] = df2["payer"] + "_" + df2["country"]
        # Specific date range
        df2 = df2[
            (df2["date"] >= self.args["start_date"])
            & (df2["date"] <= self.args["end_date"])
        ]

        # Call the function with the specified start_date and end_date
        df_full = self.fill_missing_dates(
            df2, self.args["start_date"], self.args["end_date"]
        )

        # Call the function and assign the result back to df2
        df2 = self.generate_tx_lags_and_variation(df_full, TX_CANCELLED_LAGS)
        df2["day"] = pd.to_datetime(df2["day"])

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

        # df = df.fillna(0)
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

    def correcting_holidays(self, df, holidays_to_exclude):
        """
        Marca como no festivos los días especificados en la lista de fechas.

        Args:
            df (DataFrame): DataFrame que contiene una columna 'is_holiday' indicando los días festivos.
            holidays_to_exclude (list): Lista de fechas en formato mes-año que se deben marcar como no festivas.

        Returns:
            DataFrame: DataFrame modificado con los días marcados como no festivos en la columna 'is_holiday'.
        """
        # Convertir las fechas a formato mes-día (mm-dd) para comparación
        df["month_day"] = df["date"].dt.strftime("%m-%d")

        # Marcar como no festivo (0) los días que están en la lista de fechas a excluir
        df.loc[df["month_day"].isin(holidays_to_exclude), "is_holiday"] = 0

        # Eliminar la columna 'month_day' temporal
        df.drop(columns=["month_day"], inplace=True)

        return df

    # Fill missing values with zeros in numeric columns
    # Completa los valores faltantes con ceros de las columnas numericas
    def fill_missing_with_zeros(self, df):
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
            "date",
            "payer_country",
            "payer",
            "country",
            "day_y",
            "day_x",
            "id_country",
            "id_country_y",
            "id_country_x",
            "id_main_branch",
        ]
        ## Fill nan numeric values with 0
        df = self.fill_missing_with_zeros(df)
        ## Convert to numeric
        df = self.convert_to_numeric(df, exclude_convert)
        # Filling NaN in exogenous and lags
        # df = df.fillna(0, inplace=True)

        df = df.drop(
            columns=[
                "id_country_y"
                ]
        )
        ## Fill nan numeric values with 0
        df = self.fill_missing_with_zeros(df)
        self.logger.info(df.info())
        self.logger.info(df.columns)
        #TODO: descomentar
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{self.args['bucket_name']}/abt_parquet/dt={self.partition_dt}",
            dataset=True,
            index=False,
            mode="overwrite_partitions",
            compression="snappy",
        )

        return df


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
        "process_date": "None",
        "date_lag": DATE_LAG,
    }
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
    # Create SparkDataframe
    df_final = spark.createDataFrame(df)
    # Detect numeric columns
    numeric_cols = [c[0] for c in df_final.dtypes if c[1] in ['bigint','double','float']]
    # Fill numeric values in spark dataframe
    df_filled = df_final.fillna(0, subset=numeric_cols)

    # Convert to Frame to upload to Redshift
    df_final_frame = DynamicFrame.fromDF(df_filled, glueContext, "df_final")
    # Redshift connection
    rds_conn = "via-redshift-connection"
    # Create stage temp table with schema.
    pre_query = """
    begin;
    DROP TABLE if exists {database}.{schema}.{table_name};
    CREATE TABLE if not exists {database}.{schema}.{table_name}  (LIKE public.stage_table_temporary_abt);
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
    job.commit()
