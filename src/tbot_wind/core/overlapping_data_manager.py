from datetime import datetime
import pandas as pd
from typing import Optional, Tuple

from loguru import logger

from ..database.database_wrapper import DataBaseWrapper
from ..utils.tbot_log import trace_function, pretty_print_df


COMMON_COLUMNS = ["open", "high", "low", "close"]


class DataValidator:
    def __init__(self):
        pass

    @trace_function
    def validate_earliest_date_in_db(
        self, warm_df: pd.DataFrame, hot_df: pd.DataFrame
    ) -> bool:
        """
        Validate that the earliest date in new data exists in DB.

        Args:
            warm_df (pd.DataFrame): The DataFrame fetched from DB.
            hot_df (pd.DataFrame): The new DataFrame fetched from IB.

        Returns:
            bool: True if the earliest date exists in DB, False otherwise.
        """
        try:
            earliest_hot_date = hot_df["date"].min()

            # Convert both to Unix timestamps, round them, and then compare
            earliest_hot_timestamp = round(earliest_hot_date.timestamp(), 0)
            warm_timestamps = warm_df["date"].apply(lambda x: x.timestamp())
            warm_ts_list = [round(x, 0) for x in warm_timestamps]

            if earliest_hot_timestamp not in warm_ts_list:
                logger.critical(
                    f"Earliest date {earliest_hot_date} in new data does not exist in DB."
                )
                logger.debug(
                    f"Earliest date {earliest_hot_timestamp} not in {warm_ts_list}."
                )
                pretty_print_df(warm_df, "DEBUG")
                return False
            else:
                logger.trace(
                    f"{earliest_hot_timestamp} found in {warm_ts_list}"
                )
                return True

        except Exception as e:
            logger.error(f"Error during earliest date validation: {e}")
            return False

    @trace_function
    def validate_sums_of_key_columns(
        self,
        warm_df: pd.DataFrame,
        hot_df: pd.DataFrame,
        overlap_date: datetime,
    ) -> bool:
        """
        Validate the sums of key columns between DB data and new data for the earliest date.

        Args:
            warm_df (pd.DataFrame): The DataFrame fetched from DB.
            hot_df (pd.DataFrame): The new DataFrame fetched from IB.
            earliest_new_date (datetime): The earliest date in the new data.

        Returns:
            bool: True if the sums are within tolerance, False otherwise.
        """
        try:
            overlapping_row_redis = warm_df.loc[
                warm_df["date"] == overlap_date
            ]
            overlapping_row_new = hot_df.loc[hot_df["date"] == overlap_date]

            # Calculate the sum for each row
            sum_redis = (
                overlapping_row_redis[COMMON_COLUMNS].sum(axis=1).values[0]
            )
            sum_new = overlapping_row_new[COMMON_COLUMNS].sum(axis=1).values[0]

            # Use simple arithmetic comparison for tolerance check
            relative_tolerance = 1e-3  # 0.1% tolerance
            if abs(sum_redis - sum_new) <= relative_tolerance * abs(sum_new):
                return True
            else:
                logger.critical(
                    f"Sum of key columns differ significantly: DB={sum_redis}, New={sum_new}. "
                    "This might be due to market adjustments. Proceeding with caution."
                )
                pretty_print_df(overlapping_row_redis, "CRITICAL")
                pretty_print_df(overlapping_row_new, "CRITICAL")
                return False

        except Exception as e:
            logger.error(f"Error during sums validation: {e}")
            return False

    @trace_function
    def validate_data_continuity(
        self, warm_df: pd.DataFrame, hot_df: pd.DataFrame
    ) -> bool:
        """
        Validate the continuity of data between DB and new data.

        Args:
            warm_df (pd.DataFrame): The DataFrame fetched from DB.
            hot_df (pd.DataFrame): The new DataFrame fetched from IB.

        Returns:
            bool: True if data continuity is validated, False otherwise.
        """
        assert isinstance(
            hot_df, pd.DataFrame
        ), f"hot_df must be a DataFrame, {type(hot_df)}"

        try:
            if warm_df.empty or hot_df.empty:
                logger.error("One of the DataFrames is empty.")
                return False

            # Assert that both DataFrames have timezone-aware datetime objects
            assert (
                warm_df["date"].dt.tz is not None
            ), "warm_df['date'] must be timezone-aware"
            assert (
                hot_df["date"].dt.tz is not None
            ), "hot_df['date'] must be timezone-aware"

            # Level 1 Check: Ensure the earliest date in new data exists in DB
            if not self.validate_earliest_date_in_db(warm_df, hot_df):
                return False

            # Level 2 Check: Compare sums of the key columns
            earliest_new_date = hot_df["date"].min()
            if not self.validate_sums_of_key_columns(
                warm_df, hot_df, earliest_new_date
            ):
                return False

            logger.info("Data continuity validated.")
            return True

        except Exception as e:
            logger.error(
                f"An error occurred during data continuity validation: {e}"
            )
            return False


class DataFetcher:
    def __init__(self, db_wrapper: DataBaseWrapper):
        self.db_wrapper = db_wrapper

    def get_cached_data(
        self, cache_key: str, end_date: datetime, warm_len: int
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve cached data from the database up to the specified end date with the given length.
        """
        return self.db_wrapper.get_cached_data(cache_key, end_date, warm_len)


class DataMergeManager:
    def __init__(self, db_wrapper: DataBaseWrapper):
        """
        Initialize the OverlappingWin class.

        Args:
            redis_manager (DBDataCacheManager): Instance of DBDataCacheManager to interact with DB.
            win_sec_size (int): The size of the sliding window in seconds
        """
        self.fetcher = DataFetcher(db_wrapper)
        self.validator = DataValidator()

    @trace_function
    def get_stream_merged_data(
        self, key: str, hot_df: pd.DataFrame, overlap: datetime, warm_len: int
    ) -> pd.DataFrame:
        """
        Continuously merge or process data as it is received in a real-time or near-real-time manner.

        Assume that overlap is found and valid
        Args:
            key (str): The DB cache key.
            hot_df (pd.DataFrame): The new data received that needs to be merged.
            overlap (datetime): The datetime representing the intersection point in the data.

        Returns:
            pd.DataFrame: The merged DataFrame or the new data if no merge is possible.
        """
        assert isinstance(
            overlap, datetime
        ), f"overlap must be a datetime object, got {type(overlap)}"

        # Retrieve the cached data up to the overlap point
        warm_df = self.fetcher.get_cached_data(key, overlap, warm_len)
        if warm_df is None or warm_df.empty:
            logger.critical(
                "No overlapping data found in cache. Returning only new data."
            )
            return hot_df

        # Check for timezone consistency
        if hot_df["date"].dt.tz != warm_df["date"].dt.tz:
            logger.critical(
                f"Timezone mismatch between new and cached: {hot_df['date'].dt.tz} vs. {warm_df['date'].dt.tz}"
            )
            return hot_df

        # Validate data continuity, with a warning instead of a block
        if not self.validator.validate_sums_of_key_columns(
            warm_df, hot_df, overlap
        ):
            logger.critical(
                f"{'*' * 80}\nData continuity check failed, but continuing the merge process.\n{'*' * 80}"
            )

        # Identify and exclude overlapping dates
        cached_timestamps = (
            warm_df["date"].apply(lambda x: x.timestamp()).values
        )
        hot_df_cleaned = hot_df[
            ~hot_df["date"]
            .apply(lambda x: round(x.timestamp(), 0))
            .isin(cached_timestamps)
        ]

        # Merge the cleaned new data with the cached data
        merged_data = pd.concat([warm_df, hot_df_cleaned]).reset_index(
            drop=True
        )
        logger.info(
            f"Merged DataFrame created with {len(merged_data)} rows (cached: {len(warm_df)}, new: {len(hot_df)})."
        )

        # Check if the 'date' column is sorted
        is_sorted = (
            merged_data["date"].diff().iloc[1:].ge(pd.Timedelta(0)).all()
        )
        if not is_sorted:
            logger.critical(f"{'*' * 80}\nDates are not in order.\n{'*' * 80}")
            return pd.DataFrame()

        return merged_data

    @trace_function
    def get_batch_merged_data(
        self, key: str, hot_df: pd.DataFrame, overlap: datetime, warm_len: int
    ) -> Tuple[pd.DataFrame, bool]:
        """
        Batch merging involves collecting a set of data over a period and then processing or merging
        that data all at once. This approach is used when working with large volumes of data that can be
        processed in chunks or batches.
        Returns:
            merged_data (pd.DataFrame): The merged DataFrame.
            overlap_found (bool): Indicates whether an overlap with existing data was found.
        """
        assert isinstance(
            overlap, datetime
        ), f"end_date must be a datetime object, got {type(overlap)}"

        logger.debug(f"overlap {overlap}, warm_len {warm_len}")

        warm_df = self.fetcher.get_cached_data(key, overlap, warm_len)
        if warm_df is None or warm_df.empty:
            logger.info(
                "Failed to retrieve overlapping data. Returning only new data."
            )
            return hot_df, False

        if hot_df["date"].dt.tz != warm_df["date"].dt.tz:
            logger.critical(
                f"Timezone mismatch between cached and hot_df {hot_df['date'].dt.tz}"
                f" to {warm_df['date'].dt.tz}"
            )
            logger.info(
                f"hot_df timezone: {hot_df['date'].dt.tz}, {hot_df['date'].iloc[0].tzinfo}"
            )
            logger.info(
                f"cached_data timezone: {warm_df['date'].dt.tz}, {warm_df['date'].iloc[0].tzinfo}"
            )
            return hot_df, False

        if not self.validator.validate_data_continuity(warm_df, hot_df):
            logger.critical(
                "Data continuity check failed. Returning only new data."
            )
            return hot_df, False

        common_dates = []
        for new_date in hot_df["date"]:
            new_timestamp = new_date.timestamp()
            cached_timestamps = warm_df["date"].apply(lambda x: x.timestamp())
            if new_timestamp in cached_timestamps.values:
                common_dates.append(new_date)

        hot_df_cleaned = hot_df[~hot_df["date"].isin(common_dates)]
        merged_data = pd.concat([warm_df, hot_df_cleaned]).reset_index(
            drop=True
        )

        is_sorted = (
            merged_data["date"].diff().iloc[1:].ge(pd.Timedelta(0)).all()
        )

        if not is_sorted:
            logger.critical(f"{'*' * 80}\nDates are not in order.\n{'*' * 80}")
            return pd.DataFrame(), True

        logger.info(
            f"Merged DataFrame created with {len(merged_data)} <= {len(warm_df)} + {len(hot_df)}."
        )

        return merged_data, True
