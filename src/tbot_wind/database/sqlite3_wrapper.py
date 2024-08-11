from datetime import datetime, timedelta
from loguru import logger
import pandas as pd
import sqlite3
from zoneinfo import ZoneInfo
from typing import Tuple
import math
import numpy as np

from ..utils.tbot_log import trace_function, pretty_print_df
from ..utils.tbot_env import shared
from ..utils.constants import (
    TBOT_COL,
    ALT_SELL,
    ALT_BUY,
    IND0_COL,
    IND1_COL,
    IND2_COL,
    IND3_BOOL_COL,
    IND4_BOOL_COL,
    IND5_COL,
    IND6_COL,
    IND7_COL,
    IND8_COL,
    IND9_COL,
)
from ..utils.tbot_utils import convert_duration_to_seconds


TBOT_WIND_SQL_DATE_FMT = "%Y-%m-%d %H:%M:%S"

# Constants for trading days and hours
TRADING_DAYS_PER_WEEK = 3  # Adjust this value to change trading days per week
TRADING_HOURS_PER_DAY = 6.5  # Trading hours per day


class Sqlite3Wrapper:
    def __init__(self):
        """Initialize the Sqlite3Wrapper to manage SQLite3 connections."""
        self.sqlite_db_path = shared.db_office
        self.sqlite_conn = None
        self.cursor = None

    @trace_function
    def setup_sqlite_connection(self):
        """Setup the SQLite connection if not already established."""
        if not self.sqlite_conn:
            try:
                # Establish the connection once and reuse it
                self.sqlite_conn = sqlite3.connect(self.sqlite_db_path)
                self.cursor = self.sqlite_conn.cursor()
                logger.info("SQLite connection established.")
            except sqlite3.Error as err:
                logger.error(f"Error setting up SQLite connection: {err}")
                raise

    @trace_function
    def create_table_for_order(self, table_name: str):
        """Create a table for a given order based on its table name."""
        try:
            # Define the default OHLCV columns and the missing 'average' column
            columns = f"""
                date DATETIME PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                average REAL,
                barCount INTEGER,
                {TBOT_COL} INTEGER,
                {ALT_BUY} INTEGER,
                {ALT_SELL} INTEGER,
                {IND0_COL} REAL,
                {IND1_COL} REAL,
                {IND2_COL} REAL,
                {IND3_BOOL_COL} INTEGER,
                {IND4_BOOL_COL} INTEGER,
                {IND5_COL} REAL,
                {IND6_COL} REAL,
                {IND7_COL} REAL,
                {IND8_COL} REAL,
                {IND9_COL} REAL
        """
            # Create the table if it doesn't already exist
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
            self.cursor.execute(query)
            self.sqlite_conn.commit()
            logger.info(
                f"Table {table_name} created with OHLCV, average, barCount, TBOT, BUY, SELL"
            )

        except sqlite3.Error as err:
            logger.error(f"Error creating table {table_name}: {err}")
            raise

    @trace_function
    def close(self):
        """Close the SQLite connection."""
        if self.sqlite_conn:
            self.sqlite_conn.close()
            logger.info("SQLite connection closed.")

    @trace_function
    def fetch_data_between_dates(
        self,
        table_name: str,
        key: str,
        start_date: datetime,
        end_date: datetime,
        tz: ZoneInfo,
    ) -> pd.DataFrame:
        """
        Fetch data between specific dates and localize with the given timezone.
        Args:
            table_name (str): The table from which data is being fetched.
            key (str): The column to filter by (e.g., "date").
            start_date (datetime): The start date (timezone-naive).
            end_date (datetime): The end date (timezone-naive).
            tz (ZoneInfo): The timezone to apply to the date column.
        Returns:
            pd.DataFrame: The fetched data in DataFrame format with timezone-aware dates.
        """
        # Convert start_date and end_date to naive datetime before passing to SQLite

        try:
            # Convert both start_date and end_date to UTC for SQLite queries
            start_utc = start_date.astimezone(ZoneInfo("UTC")).strftime(
                TBOT_WIND_SQL_DATE_FMT
            )
            end_utc = end_date.astimezone(ZoneInfo("UTC")).strftime(
                TBOT_WIND_SQL_DATE_FMT
            )

            logger.info(
                f"Fetching data from {table_name} between {start_utc} and {end_utc}, "
                f"original timezone: {tz}"
            )
            query = f"SELECT * FROM {table_name} WHERE {key} BETWEEN ? AND ? ORDER BY {key} ASC"
            params = (start_utc, end_utc)
            df = self.fetch_sqlite_data(query, params, tz)

            # Log the output start and end dates after the query execution
            if not df.empty:
                o_start = (
                    df["date"]
                    .min()
                    .astimezone(ZoneInfo("UTC"))
                    .strftime(TBOT_WIND_SQL_DATE_FMT)
                )
                o_end = (
                    df["date"]
                    .max()
                    .astimezone(ZoneInfo("UTC"))
                    .strftime(TBOT_WIND_SQL_DATE_FMT)
                )
                logger.info(
                    f"Fetched data between output start date: {o_start} and output end date: {o_end}"
                )
                # Check for duplicate dates and log if found
                duplicated_dates = df["date"].duplicated(keep=False)
                if duplicated_dates.any():
                    duplicate_rows = df[duplicated_dates]
                    logger.critical(
                        f"Found {duplicate_rows.shape[0]} duplicated rows. Duplicated dates:"
                    )
                    logger.debug(duplicate_rows["date"].to_list())
                else:
                    logger.debug(
                        "No duplicated dates found in the fetched data."
                    )
            else:
                logger.info("No data fetched for the given date range.")
                return pd.DataFrame()

        except Exception as e:
            logger.critical(f"fetch_sqlite_data failed: {e}")
            return pd.DataFrame()

        return df

    def calculate_expected_min_bars(
        self, timeframe: str, duration: str
    ) -> int:
        """
        Calculate the expected minimum number of bars based on timeframe and duration.

        Args:
            timeframe (str): The timeframe string (e.g., '1 H', '30 M').
            duration (str): The duration string (e.g., '2 D', '1 W').

        Returns:
            int: The expected minimum number of bars.

        Raises:
            ValueError: If the timeframe format is invalid.
        """
        try:
            # Convert duration to seconds
            duration_seconds = convert_duration_to_seconds(duration)

            # Calculate time in trading days and hours using constants
            seconds_per_day = (
                TRADING_HOURS_PER_DAY * 3600
            )  # Trading hours per day
            seconds_per_week = (
                TRADING_DAYS_PER_WEEK * seconds_per_day
            )  # Trading days per week

            # Calculate the number of full weeks and remaining days
            full_weeks = duration_seconds // seconds_per_week
            remaining_seconds = duration_seconds % seconds_per_week
            full_days = remaining_seconds // seconds_per_day
            # If there are remaining seconds that constitute a partial day, count it as a full day
            if remaining_seconds % seconds_per_day > 0:
                full_days += 1

            total_trading_days = (
                full_weeks * TRADING_DAYS_PER_WEEK + full_days
            )  # Use constant for trading days per week

            # Convert timeframe to seconds
            timeframe_seconds = convert_duration_to_seconds(timeframe)
            timeframe_hours = timeframe_seconds / 3600  # Convert to hours

            # Calculate expected number of bars
            expected_min_bars = math.ceil(
                (total_trading_days * TRADING_HOURS_PER_DAY) / timeframe_hours
            )

            logger.debug(
                f"Expected minimum bars: {expected_min_bars} based on timeframe '{timeframe}' and duration '{duration}'"
            )
            return expected_min_bars
        except Exception as e:
            logger.error(f"Error calculating expected minimum bars: {e}")
            raise

    def calculate_start_end_dates(
        self, end_date: datetime, duration: str
    ) -> Tuple[datetime, datetime]:
        """
        Calculate the start and end dates based on the duration and end_date.

        Args:
            duration (str): The duration string (e.g., '2 D', '5 H').
            end_date (datetime): The end date (timezone-aware).

        Returns:
            Tuple[datetime, datetime]: A tuple containing the start_date and end_date.
        """
        try:
            duration_seconds = convert_duration_to_seconds(duration)
            start_date = end_date - timedelta(seconds=duration_seconds)
            logger.debug(
                f"Calculated start_date: {start_date.isoformat()} based on duration '{duration}' and end_date '{end_date.isoformat()}'"
            )
            return start_date, end_date
        except Exception as e:
            logger.error(f"Error calculating start and end dates: {e}")
            raise

    @trace_function
    def check_date_range_mapping_data(
        self,
        table_name: str,
        timeframe: str,
        end_date: datetime,
        duration: str,
    ) -> bool:
        """
        Check if there are enough bars within the specified duration and timeframe.

        Args:
            table_name (str): The name of the SQLite table to query.
            timeframe (str): The timeframe string (e.g., '1 H', '30 M').
            duration (str): The duration string (e.g., '2 D', '5 H').
            end_date (datetime): The end date of the range (timezone-aware).

        Returns:
            bool: True if the number of bars >= expected_min_bars, False otherwise.
        """
        try:
            # Calculate start_date and end_date
            start_date, end_date = self.calculate_start_end_dates(
                end_date, duration
            )

            # Convert to UTC and format
            start_utc = start_date.astimezone(ZoneInfo("UTC")).strftime(
                TBOT_WIND_SQL_DATE_FMT
            )
            end_utc = end_date.astimezone(ZoneInfo("UTC")).strftime(
                TBOT_WIND_SQL_DATE_FMT
            )

            logger.debug(
                f"Checking bar count from '{start_utc}' to '{end_utc}' in table '{table_name}'."
            )

            # Calculate expected minimum number of bars
            expected_min_bars = self.calculate_expected_min_bars(
                timeframe, duration
            )
            logger.debug(
                f"Expected minimum number of bars: {expected_min_bars}"
            )

            # Query to count the number of bars within the date range
            query = f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE date BETWEEN ? AND ?
            """
            logger.debug(
                f"Executing query to count bars: {query} with params ({start_utc}, {end_utc})"
            )
            self.cursor.execute(query, (start_utc, end_utc))
            result = self.cursor.fetchone()

            actual_bars = result[0] if result else 0
            logger.debug(f"Actual number of bars found: {actual_bars}")

            # Compare actual bars with expected minimum
            if actual_bars >= expected_min_bars:
                logger.info(
                    f"Sufficient bars found: {actual_bars} >= {expected_min_bars}"
                )
                return True
            else:
                logger.info(
                    f"Insufficient bars found: {actual_bars} < {expected_min_bars}"
                )
                return False

        except Exception as e:
            logger.error(f"Error in check_date_range_mapping_data: {e}")
            return False

    def prepare_dataframe_for_sqlite(self, df: pd.DataFrame) -> pd.DataFrame:
        # Convert boolean columns to integers (0 or 1) for SQLite
        boolean_columns = [ALT_BUY, ALT_SELL, IND3_BOOL_COL, IND4_BOOL_COL]
        for col in boolean_columns:
            if col in df.columns:
                try:
                    # Replace NaN with a default value (e.g., 0) before converting to int
                    df[col] = df[col].fillna(0).astype(int)
                except ValueError as e:
                    logger.critical(f"Error converting {col} to int: {e}")
                    # Optionally, handle specific cases or re-raise the exception
                    continue

        # Convert NaN to None in the entire DataFrame for SQLite
        df = df.where(pd.notnull(df), None)
        return df

    def prepare_sqlite_for_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        # Convert integer columns back to boolean for DataFrame
        boolean_columns = [ALT_BUY, ALT_SELL, IND3_BOOL_COL, IND4_BOOL_COL]
        for col in boolean_columns:
            if col in df.columns:
                try:
                    df[col] = df[col].astype(bool)
                except ValueError as e:
                    logger.critical(f"Error converting {col} to bool: {e}")
                    continue

        # Convert None to NaN in the entire DataFrame
        df = df.replace({None: np.nan})
        return df

    @trace_function
    def insert_ordered_dataframe(
        self, table_name: str, data: pd.DataFrame, tz: ZoneInfo
    ):
        data = self.prepare_dataframe_for_sqlite(data)
        self.insert_ordered_data(table_name, data, tz)

    @trace_function
    def insert_ordered_data(
        self, table_name: str, data: pd.DataFrame, tz: ZoneInfo
    ):
        """
        Insert data directly into the SQLite table while ensuring the primary key (date) remains in order.
        Args:
            table_name (str): The name of the table where data will be inserted.
            data (pd.DataFrame): The DataFrame containing the data to be inserted.
            tz (ZoneInfo): The timezone to apply to the date column before inserting.
        """
        try:
            required_columns = [
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "average",
                "barCount",
                TBOT_COL,
                ALT_BUY,
                ALT_SELL,
                IND0_COL,
                IND1_COL,
                IND2_COL,
                IND3_BOOL_COL,
                IND4_BOOL_COL,
                IND5_COL,
                IND6_COL,
                IND7_COL,
                IND8_COL,
                IND9_COL,
            ]

            # Ensure the DataFrame has all required columns
            for col in required_columns:
                if col not in data.columns:
                    data[col] = (
                        0  # Fill missing columns with default value (e.g., 0)
                    )

            # Sort data by the 'date' column to ensure ascending order
            # data = data.sort_values(by="date", ascending=True)

            # Convert the 'date' column to UTC and format it for SQLite
            data["date"] = (
                data["date"]
                .dt.tz_convert("UTC")
                .dt.strftime(TBOT_WIND_SQL_DATE_FMT)
            )

            logger.info(
                f"Inserting data between start: {data['date'].min()} and end: {data['date'].max()}"
            )

            placeholders = ", ".join(["?"] * len(required_columns))
            columns_str = ", ".join(required_columns)
            insert_query = f"""
                INSERT OR REPLACE INTO {table_name} ({columns_str})
                VALUES ({placeholders})
            """

            # Prepare the data as a list of tuples for execution
            records = [tuple(row) for row in data[required_columns].values]

            # Establish the SQLite connection and insert the data in batches
            self.setup_sqlite_connection()
            self.cursor.executemany(insert_query, records)
            self.sqlite_conn.commit()

            logger.info(
                f"Data successfully inserted or replaced in {table_name}."
            )

        except Exception as e:
            logger.error(f"Error inserting data into table {table_name}: {e}")

    @trace_function
    def fetch_last_row(
        self, table_name: str, key: str, tz: ZoneInfo
    ) -> pd.Series:
        """
        Fetch the last row from the table based on date ordering.

        Args:
            table_name (str): The name of the table.
            key (str): The column to order by (typically the 'date' column).

        Returns:
            pd.DataFrame: A DataFrame with the last row of the table.
        """
        query = f"SELECT * FROM {table_name} ORDER BY {key} DESC LIMIT 1"
        df = self.fetch_sqlite_data(query, (), tz)

        return pd.Series() if df.empty else df.iloc[0]

    @trace_function
    def fetch_sqlite_data(
        self, query: str, params: tuple, tz: ZoneInfo
    ) -> pd.DataFrame:
        """Fetch data from SQLite as a pandas DataFrame."""
        self.setup_sqlite_connection()

        try:
            df = pd.read_sql_query(query, self.sqlite_conn, params=params)
            df = self.prepare_sqlite_for_dataframe(df)
            logger.debug(f"Fetched {len(df)} rows from SQLite database.")
            try:
                if not df.empty and "date" in df.columns:
                    logger.trace(
                        f"Raw fetched data (as stored in SQLite) between start date: {df['date'].min()} and end date: {df['date'].max()}"
                    )
                    pretty_print_df(df)

                    df["date"] = pd.to_datetime(
                        df["date"], format=TBOT_WIND_SQL_DATE_FMT
                    ).dt.tz_localize("UTC")
                    logger.trace(
                        f"Fetched data localized to UTC between start date: {df['date'].min()} and end date: {df['date'].max()}"
                    )

                    df["date"] = df["date"].dt.tz_convert(tz)
                    logger.trace(
                        f"Fetched data converted to {tz} between start date: {df['date'].min()} and end date: {df['date'].max()}"
                    )

            except Exception as e:
                logger.critical(
                    f"Failed to localize 'date' column to timezone {tz}: {e}"
                )
                return pd.DataFrame()
            return df
        except sqlite3.Error as e:
            logger.error(f"SQLite fetch error: {e}")
            return pd.DataFrame()

    @trace_function
    def execute_sqlite_query(self, query: str, params: tuple = ()):
        """Execute an arbitrary SQL query (e.g., for INSERT/UPDATE/DELETE)."""
        try:
            cursor = self.sqlite_conn.cursor()
            cursor.execute(query, params)
            self.sqlite_conn.commit()
            logger.info(f"Executed SQLite query: {query}")
        except sqlite3.Error as e:
            logger.error(f"SQLite error during query execution: {e}")

    @trace_function
    def create_trigger(
        self, table_name: str, key: str, max_records: int = 3600
    ):
        """Create a trigger to delete old records when a table reaches a certain size."""
        query = f"""
        CREATE TRIGGER IF NOT EXISTS TRIG_{table_name.upper()}
        AFTER INSERT ON {table_name}
        BEGIN
            DELETE FROM {table_name} WHERE {key} NOT IN (
                SELECT {key} FROM {table_name} ORDER BY {key} DESC LIMIT {max_records}
            );
        END;
        """
        self.execute_sqlite_query(query)
