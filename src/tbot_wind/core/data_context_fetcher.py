from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from typing import Dict, Optional
from loguru import logger

from .stock_datafetcher import StockDataFetcher
from .overlapping_data_manager import DataMergeManager

from ..utils.tbot_status import TBOTStatus
from ..database.database_wrapper import DataBaseWrapper
from ..utils.objects import OrderTVEx, OrderTV
from ..utils.constants import TBOT_COL
from ..utils.tbot_log import trace_function, pretty_msg_df, pretty_print_df
from ..utils.tbot_utils import (
    get_overlap_duration,
    get_timedelta_duration,
    calculate_tf_interval,
)


class DataFetcherContext:
    def __init__(
        self,
        db_wrapper: DataBaseWrapper,
        warm_win_loopback: int,
        fetcher: Optional[StockDataFetcher] = None,
    ):
        """
        Initialize the DataFetcherContext with caching capabilities.
        """
        self.db_wrapper = db_wrapper
        self.last_api_fetch_mapping: Dict[
            str, Dict[str, Optional[datetime]]
        ] = {}
        self.warm_win_loopback = warm_win_loopback

        self.merger = DataMergeManager(self.db_wrapper)
        self.hot_window: Dict[str, str] = {}
        self.warm_window: Dict[str, int] = {}
        self.is_first_boot_mapping: Dict[str, bool] = {}
        self.test_data_fetched = (
            False  # State variable to track test data fetching
        )
        self.fetcher = fetcher

    async def fetch_realtime_data(
        self, order: OrderTV, duration: str, end_date: str
    ) -> pd.DataFrame:
        if not self.fetcher:
            raise ValueError("No data fetcher strategy set!")
        return await self.fetcher.fetch_realtime_data(
            order, duration, end_date
        )

    def initialize_durations(self, key: str, duration: str, tf: str):
        """Initialize the API duration for a cache key if not already set."""
        if key not in self.hot_window:
            self.hot_window[key] = duration
        if key not in self.warm_window:
            self.warm_window[key] = (
                self.warm_win_loopback * calculate_tf_interval(tf)
            )

    @trace_function
    def check_overlap_with_existing_data(
        self, key: str, api_date: datetime
    ) -> bool:
        """
        Check if the existing data overlaps with the new data fetched from the API.
        """
        row = self.db_wrapper.get_last_row(key)
        if not row.empty:
            last_cached_date = row["date"]
            if last_cached_date >= api_date:
                logger.info(
                    f"incoming window confirmed: last cached {last_cached_date} >=  earliest live {api_date}"
                )
                return True
        return False

    @trace_function
    def update_overlap_duration(self, key):
        """
        Update the API overlap duration if there is insufficient overlap.
        """
        new_duration = get_overlap_duration(self.hot_window[key], 2)
        logger.critical(
            f"Increasing incoming window from {self.hot_window[key]} to {new_duration}"
        )
        self.hot_window[key] = new_duration

    @trace_function
    async def fetch_stream_data(self, order: OrderTVEx) -> pd.DataFrame:
        """
        Fetch data in a streaming manner, processing smaller chunks of data as they become available.

        This method primarily works in real-time, fetching data from the API in smaller, continuous chunks
        It assumes the hot window is slightly larger than expected to cover potential overlaps
        If it doesn't find sufficient overlap in the cache,
        it increases the size of the hot window for subsequent requests. The user-provided duration is
        more important here because it controls how much data is fetched in each request, and
        the method relies on dynamically adjusting the window size.

        Args:
            order (OrderTV): The order details.
            duration (str): The duration of the data fetch.
            end_date (str): The end date for the data fetch.

        Returns:
            pd.DataFrame: The merged DataFrame, or an empty DataFrame if no data is available or there is no new data.
        """
        key = self.db_wrapper.get_key(order)

        # Check if this is the first boot for the specific cache key
        if self.is_first_boot_mapping.get(key, True):
            self.initialize_durations(key, order.duration, order.timeframe)
            self.is_first_boot_mapping[key] = False

        last_row = self.db_wrapper.get_last_row(key)
        if not last_row.empty and not self.validate_last_tbot_status(last_row):
            logger.warning(
                "Invalid TBOT status detected in the last cached row. Fetching batched data for correction."
            )
            return await self.fetch_batched_data(order)

        # Fetch new data and merge with cached data
        cached = await self.retrieve_realtime_and_stream_merge(
            key, order, order.end_date
        )
        return cached

    def get_duration_for_incremental_fetch(
        self, last_row: pd.Series, order: OrderTVEx
    ) -> str:
        """
        Calculate the duration required for incremental data fetching based on the last row in the database.
        If no last row is present, return the full duration from the order.

        Args:
            last_row (pd.Series): The last row of data from the database.
            order (OrderTVEx): The order details.

        Returns:
            str: The duration string for data fetching.
        """
        if last_row.empty:
            logger.info(
                f"No last row found in cache for key {order.symbol}. Fetching full data."
            )
            return order.duration

        last_date = last_row["date"]
        logger.debug(f"Last cached date: {last_date}")
        return get_timedelta_duration(
            last_date, order.end_date, order.timeframe
        )

    @trace_function
    async def fetch_incremental_data(self, order: OrderTVEx) -> pd.DataFrame:
        """
        Fetch both warm (historical) and hot (real-time) data, merge them, and return the result.

        This method reads historical data from the cache first, then fetches the remaining real-time data
        just before the current time from the IB API. The user-provided duration is less critical in this method
        because the focus is on merging the latest cached data with live data to ensure continuity up to the present."
        Args:
            order (OrderTV): The order details.
            duration (str): The duration of the data fetch.
            end_date (str): The end date for the data fetch.

        Returns:
            pd.DataFrame: The merged DataFrame, or an empty DataFrame if no data is available or there is no new data.
        """
        key = self.db_wrapper.get_key(order)

        # Check if this is the first boot for the specific cache key
        if self.is_first_boot_mapping.get(key, True):
            self.initialize_durations(key, order.duration, order.timeframe)
            self.is_first_boot_mapping[key] = False

        # Get the last cached data to determine overlap and validate TBOT status
        last_row = self.db_wrapper.get_last_row(key)
        if not last_row.empty and not self.validate_last_tbot_status(last_row):
            logger.warning(
                "Invalid TBOT status detected in the last cached row. Fetching batched data for correction."
            )
            return await self.fetch_batched_data(order)

        # Calculate duration needed for incremental data fetch
        calculated = self.get_duration_for_incremental_fetch(last_row, order)

        # Fetch hot data from IB API
        logger.info(
            f"Fetching hot data from IB API with duration: {calculated}"
        )
        hot_data = await self.fetch_realtime_data(
            order, calculated, order.end_date
        )

        if hot_data.empty:
            logger.info(
                "No new data retrieved from IB API. Returning cached warm data."
            )
            return pd.DataFrame()

        # Merge warm and hot data
        return self.merge_warm_and_hot_data(key, last_row, hot_data)

    def merge_warm_and_hot_data(
        self, key: str, last_row: pd.Series, hot_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merge the warm data from the database with the hot data retrieved from the IB API.

        Args:
            key (str): The key for the data in the database.
            last_row (pd.Series): The last row of data from the database.
            hot_data (pd.DataFrame): The hot data fetched from the IB API.

        Returns:
            pd.DataFrame: The merged DataFrame.
        """
        if last_row.empty:
            # No warm data to merge, return the hot data directly
            logger.info("No warm data found in the cache, returning hot data.")
            return hot_data

        last_date = last_row["date"]
        most_recent_idx = self.find_specific_timestamp_index(
            last_date, hot_data
        )

        if most_recent_idx < 0:
            # No overlap found, update the overlap duration and return hot data
            logger.info(
                "No overlap found. Increasing the incoming window for the next fetch."
            )
            self.update_overlap_duration(key)
            return hot_data

        if most_recent_idx == (len(hot_data) - 1):
            # Overlap is at the end of the hot data, no new data to merge
            logger.info(
                "Overlap found at the end of the retrieved data. No new data to merge."
            )
            return pd.DataFrame()

        # Merge warm data with hot data
        overlap_date = hot_data.iloc[most_recent_idx]["date"]
        merged_data = self.merger.get_stream_merged_data(
            key, hot_data, overlap_date, self.warm_window[key]
        )
        logger.info(
            f"Merging complete. Merged data contains {len(merged_data)} rows."
        )
        return merged_data

    @trace_function
    async def fetch_batched_data(self, order: OrderTVEx) -> pd.DataFrame:
        """
        Fetch data in larger batches, processing larger sets of data at scheduled intervals.

        Args:
            order (OrderTV): The order details.
            duration (str): The duration of the data fetch.
            end_date (str): The end date for the data fetch.

        Returns:
            pd.DataFrame: The merged DataFrame, or an empty DataFrame if no data is available or there is no new data.
        """
        key = self.db_wrapper.get_key(order)

        # Check if this is the first boot for the specific cache key
        if self.is_first_boot_mapping.get(key, True):
            self.initialize_durations(key, order.duration, order.timeframe)
            self.is_first_boot_mapping[key] = False

        # Fetch new data and merge with cached data
        cached = await self.retrieve_realtime_and_batched_merge(
            key, order, order.end_date
        )
        return cached

    @trace_function
    async def fetch_test_data(self, order: OrderTVEx) -> pd.DataFrame:
        """
        Fetch test data by calling `fetch_batched_data()` once.
        If called again, return an empty DataFrame.

        This function is intended to be used for verification of indicators.
        """
        # Check if test data has already been fetched
        if self.test_data_fetched:
            logger.info(
                "Test data already fetched once. Returning an empty DataFrame."
            )
            return (
                pd.DataFrame()
            )  # Return an empty DataFrame if already fetched

        # Fetch test data using the `fetch_batched_data` method
        test_data = await self.fetch_batched_data(order)

        # Mark that test data has been fetched to prevent future calls
        self.test_data_fetched = True

        # Return the fetched test data
        return test_data

    def get_earliest_after_today(
        self, order: OrderTV, new_df: pd.DataFrame
    ) -> datetime:
        """
        Get the earliest boundary date for the cache window from new_df.

        This function finds the closest date to today's date that is within the last 14 bars in the new_df.
        If today's date is in the DataFrame, it returns the earliest date after today; otherwise,
        it returns the latest date that is within the last 14 bars.

        Args:
            order (OrderTV): The order details.
            new_df (pd.DataFrame): The DataFrame containing the new data fetched from IB.

        Returns:
            datetime: The earliest boundary date for the cache window.
        """
        # Sort the DataFrame by date to ensure it's in order
        new_df = new_df.sort_values(by="date")

        # Today's date in the same timezone as the order
        today = datetime.now(ZoneInfo(order.timezone))

        # Find the dates that are within the last 14 bars
        recent_dates = new_df["date"].iloc[-14:]

        # Find the earliest date that is closest to today's date or just after it
        future_dates = recent_dates[recent_dates >= today]

        if not future_dates.empty:
            earliest_date = future_dates.iloc[0]  # Get the closest future date
        else:
            earliest_date = recent_dates.iloc[
                -1
            ]  # Get the latest date if no future dates

        return earliest_date

    @trace_function
    async def retrieve_realtime_and_batched_merge(
        self, key: str, order: OrderTV, end_date: str
    ) -> pd.DataFrame:
        """
        Fetch data from IB API and merge it with existing data from the Database
        to create the largest possible merged window.

        The function retrieves new data from the Interactive Brokers API for a specified duration and end date,
        then merges it with the existing warm data from the database.
        If there is no overlap or a data gap is detected, the hot window duration for the next fetch is increased.

        Args:
            key (str): The unique key representing the stock symbol and other parameters.
            order (OrderTV): The order details for the stock being processed.
            end_date (str): The end date for the data fetch from IB API in the format "YYYY-MM-DD".

        Returns:
            pd.DataFrame: A DataFrame containing the merged data, including the newly fetched and previously data.
        """
        # Fetch new data from the IB API
        logger.info(
            f"hot window duration {self.hot_window[key]} end_date {end_date}"
        )
        new_df = await self.fetch_realtime_data(
            order, self.hot_window[key], end_date
        )

        if not new_df.empty:

            # Find the oldest date from the newly fetched data
            oldest = new_df.iloc[0]["date"]
            logger.info(
                f"fetched hot window from {oldest} to {new_df.iloc[-1]['date']}"
            )

            # Merge the new data with the warm data stored in the database
            merged, is_overlap = self.merger.get_batch_merged_data(
                key, new_df, oldest, self.warm_window[key]
            )

            if not is_overlap:
                # If no overlap or data gap is detected, increase the window for the next fetch
                self.update_overlap_duration(key)
                logger.critical(
                    "Data gap detected or no overlap found, increasing incoming window for the next fetch."
                )

            return merged
        else:
            return pd.DataFrame()

    @trace_function
    def find_specific_timestamp_index(
        self, search: datetime, new_df: pd.DataFrame
    ) -> int:
        """
        Find the element in `new_df` where the date overlaps with the element.

        Returns the index of the overlap in `new_df`, or -1 if no overlap is found.
        """
        try:
            if search is None:
                return -1

            if not isinstance(search, datetime):
                raise TypeError("Expected 'search' to be a datetime object")

            last_row_timestamp = round(
                search.timestamp(), 0
            )  # Convert to timestamp
            # Convert new_df dates to timestamps
            new_df_timestamps = (
                new_df["date"]
                .apply(lambda x: round(x.timestamp(), 0))
                .tolist()
            )

            # Check if the last row's timestamp exists in the new_df timestamps
            if last_row_timestamp in new_df_timestamps:
                overlap_index = new_df_timestamps.index(last_row_timestamp)
                logger.info(
                    f"Found exact match for last row date at index {overlap_index} in new data."
                )
                return overlap_index
            else:
                logger.info(
                    "No exact match found for last row date in new data."
                )
                return -1

        except Exception as e:
            logger.error(f"Error during overlap index calculation: {e}")
            return -1

    def validate_last_tbot_status(self, row: pd.Series = None) -> bool:
        """
        Validate whether the row is marked as ALERT_COMPLETED, which means all indicators are calculated and
        the alert is calculated. This row should only be used for warm-up.
        """
        try:
            if not isinstance(row, pd.Series):
                logger.critical(
                    "TBOT_COL value is a Series, expected a scalar. Please verify data integrity."
                )
                return True

            if "date" not in row.index:
                logger.warning(
                    "'date' not found in the Series index. Suggest to run --init-db to initialize DB"
                )
                return True

            # Check if TBOT_COL is in the Series index
            if TBOT_COL not in row.index:
                logger.info(f"{TBOT_COL} not found in the Series.")
                return False

            tbot = row.loc[TBOT_COL]

            if not TBOTStatus.has_status(tbot, TBOTStatus.ALERT_COMPLETED):
                pretty_msg_df(row, "invalid TBOT status:", "WARNING")
                return False

            pretty_msg_df(row, "Valid TBOT status: ALERT_COMPLETED")

        except Exception as e:
            logger.error(f"Error during TBOT status validation: {e}")
            return False

        return True

    @trace_function
    def find_db_last_row_overlap_index(
        self, key: str, new_df: pd.DataFrame
    ) -> int:
        """
        Find the index in `new_df` where the date overlaps with the last row from Database.

        Returns the index of the overlap in `new_df`, or -1 if no overlap is found.
        """
        try:
            last_row = self.db_wrapper.get_last_row(key)

            if last_row.empty or "date" not in last_row:
                logger.info("No valid last row from Redis to compare.")
                return -1

            last_row_timestamp = round(
                last_row["date"].timestamp(), 0
            )  # Convert to timestamp

            # Convert new_df dates to timestamps
            new_df_timestamps = (
                new_df["date"]
                .apply(lambda x: round(x.timestamp(), 0))
                .tolist()
            )

            # Check if the last row's timestamp exists in the new_df timestamps
            if last_row_timestamp in new_df_timestamps:
                overlap_index = new_df_timestamps.index(last_row_timestamp)
                logger.info(
                    f"Found exact match for last row date at index {overlap_index} in new data."
                )
                return overlap_index
            else:
                logger.info(
                    "No exact match found for last row date in new data."
                )
                return -1

        except Exception as e:
            logger.error(f"Error during overlap index calculation: {e}")
            return -1

    @trace_function
    async def retrieve_realtime_and_stream_merge(
        self, key: str, order: OrderTV, end_date: str
    ) -> pd.DataFrame:
        """
        Fetch data from IB API and merge it with existing data from Redis.

        Args:
            key (str): The Redis cache key.
            order (OrderTV): The order details.
            end_date (str): The end date for the data fetch.

        Returns:
            pd.DataFrame: The merged DataFrame, or an empty DataFrame if no data is available or there is no new data.
        """
        logger.info(
            f"Starting data retrieval and merge process for key: {key}"
        )
        logger.info(
            f"Incoming data window duration: {self.hot_window[key]}, end_date: {end_date}"
        )

        # Fetch new data from IB API
        new_df = await self.fetch_realtime_data(
            order, self.hot_window[key], end_date
        )

        if new_df.empty:
            logger.info("No new data retrieved from IB API.")
            return pd.DataFrame()

        logger.info(
            f"Retrieved {len(new_df)} rows of data from IB API. Checking for overlaps with existing data."
        )

        # Find the most recent overlap index
        most_recent_idx = self.find_db_last_row_overlap_index(key, new_df)

        if most_recent_idx < 0:
            # If no overlap is found, increase the incoming window for the next fetch
            self.update_overlap_duration(key)
            logger.critical(
                "No overlap found. Data gap detected. Increasing incoming window for the next fetch."
            )
            return new_df

        if most_recent_idx == (len(new_df) - 1):
            logger.warning(
                "Overlap found at the end of the retrieved data. No new data to merge."
            )
            return pd.DataFrame()

        logger.info(
            f"Overlap found at index {most_recent_idx}. Proceeding with data merge."
        )

        # If overlap is found, merge the data
        overlap_date = new_df.iloc[most_recent_idx]["date"]
        merged_data = self.merger.get_stream_merged_data(
            key, new_df, overlap_date, self.warm_window[key]
        )

        logger.info(
            f"Merging complete. Final merged data contains {len(merged_data)} rows."
        )

        return merged_data

    @trace_function
    def cache_data(self, order: OrderTV, df: pd.DataFrame):
        """
        Push data that has calculations from indicator into Database.
        Only cache the rows where 'TBOT' status has changed, if tbot_changes is provided.

        Args:
            order (OrderTV): The order details used for generating the cache key.
            df (pd.DataFrame): Updated DataFrame after calculations.
        """
        # Determine the cache key
        key = self.db_wrapper.get_key(order)

        # Store the changed data in cache
        self.db_wrapper.store_data(key, df)
        logger.info(
            f"Data cached from {df['date'].min()} to {df['date'].max()} with {len(df)},{len(df)} rows"
        )

        # Optionally, pretty-print the last few rows of the cached data
        pretty_print_df(df.tail(3), "DEBUG")

        last_row = self.db_wrapper.get_last_row(key)
        if not last_row.empty:
            pretty_msg_df(last_row, "Readback", "INFO")
        else:
            logger.error("Failed to fetch the last row")
