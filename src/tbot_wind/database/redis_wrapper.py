from datetime import datetime, timedelta
import pandas as pd
import msgpack
import redis
from typing import Optional, List
from loguru import logger

from .redis_historical_date_range import RedisHistoricalDataRangeManager
from ..utils.constants import TBOT_WIND_REDIS_DATE_FORMAT
from ..utils.tbot_utils import convert_duration_to_seconds
from ..utils.tbot_log import trace_function, pretty_print_df
from ..utils.tbot_env import shared
from ..utils.tbot_datetime import get_key_timezone


class RedisWrapper:
    def __init__(self):
        """
        Initialize the RedisWrapper.
        Args:
            host (str): Redis server hostname.
            port (int): Redis server port.
            ttl (int): Time-to-live for cache entries in seconds.
        """
        self.ttl = shared.r_ttl
        self.host = shared.r_host
        self.port = shared.r_port
        self.redis = redis.StrictRedis(
            host=self.host, port=self.port, decode_responses=False
        )
        self.date_range_mapper = RedisHistoricalDataRangeManager(self.redis)

    def close(self):
        """
        Close the Redis connection.
        """
        try:
            if self.redis:
                self.redis.close()
                logger.info("Redis connection closed.")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

    def unpack_and_create_dataframe(
        self, key: str, cached_data: List
    ) -> pd.DataFrame:
        """
        Unpack cached data from Redis and create a DataFrame.
        """
        unpacked_data = [
            msgpack.unpackb(item, raw=False) for item in cached_data
        ]
        df = pd.DataFrame(unpacked_data)

        if df.empty:
            logger.critical(
                "DataFrame is empty after unpacking and processing."
            )
            return pd.DataFrame()

        if "date" in df.columns:
            df["date"] = pd.to_datetime(
                df["date"], format=TBOT_WIND_REDIS_DATE_FORMAT
            )
            df["date"] = df["date"].dt.tz_localize(get_key_timezone(key))
        else:
            logger.critical("Date column missing after unpacking the data.")
            return pd.DataFrame()

        return df

    def get_closest_data(
        self, key: str, timeframe: str, end_date: datetime, duration: str
    ) -> pd.DataFrame:
        """
        Retrieve data from Redis cache for the closest available date to the specified end_date.
        """
        duration_seconds = convert_duration_to_seconds(duration)
        duration_timedelta = timedelta(seconds=duration_seconds)
        actual_dates = self.date_range_mapper.get_actual_dates(
            f"{key}:date_mapping", timeframe, end_date, duration
        )

        if not actual_dates:
            closest_end_date = self._find_closest_earlier_date(key, end_date)
            if closest_end_date:
                actual_dates = (
                    closest_end_date - duration_timedelta,
                    closest_end_date,
                )
            else:
                return pd.DataFrame()

        actual_start_date, actual_end_date = actual_dates
        cached_data = self.redis.zrangebyscore(
            key, actual_start_date.timestamp(), actual_end_date.timestamp()
        )

        if cached_data:
            return self.unpack_and_create_dataframe(key, cached_data)
        return pd.DataFrame()

    def _find_closest_earlier_date(
        self, key: str, end_date: datetime
    ) -> Optional[datetime]:
        """
        Finds the closest earlier date to the specified end_date in the Redis cache.
        """
        score = end_date.timestamp()
        members = self.redis.zrevrangebyscore(
            key, score, "-inf", start=0, num=1
        )
        if members:
            closest_data = self.redis_data_to_series(key, members[0])
            if "date" in closest_data:
                return closest_data["date"]
        return None

    def redis_data_to_series(self, key: str, redis_data: bytes) -> pd.Series:
        """
        Convert Redis data (in msgpack format) into a pandas Series with timezone-aware date conversion.
        """
        unpacked_data = msgpack.unpackb(redis_data, raw=False)
        series_data = pd.Series(unpacked_data)

        if "date" in series_data:
            date_value = series_data["date"]
            if isinstance(date_value, str):
                date_obj = datetime.strptime(
                    date_value, TBOT_WIND_REDIS_DATE_FORMAT
                )
                series_data["date"] = date_obj.replace(
                    tzinfo=get_key_timezone(key)
                )
            elif isinstance(date_value, datetime):
                series_data["date"] = date_value.replace(
                    tzinfo=get_key_timezone(key)
                )
        return series_data

    def _fetch_data_from_redis(
        self, key: str, start_date: datetime, end_date: datetime
    ) -> Optional[pd.DataFrame]:
        """
        Fetch data from Redis using zrangebyscore.
        """
        cached_data = self.redis.zrangebyscore(
            key, start_date.timestamp(), end_date.timestamp()
        )
        if cached_data:
            return self.unpack_and_create_dataframe(key, cached_data)
        return None

    def _add_unique_element(self, pipeline, key, msg, score):
        """
        Add a unique element in Redis, removing duplicates.
        """
        existing_members = self.redis.zrangebyscore(key, score, score)
        for member in existing_members:
            pipeline.zrem(key, member)
        pipeline.zadd(key, {msg: score})

    @trace_function
    def get_last_row(self, key: str) -> pd.Series:
        """
        Retrieve the last row of data from the Redis cache.

        Args:
            key (str): The cache key to retrieve the last row.

        Returns:
            pd.Series: The last row as a pandas Series if found, otherwise an empty Series.
        """
        try:
            # Use Redis sorted set to get the last element in the sorted set by score
            last_data = self.redis.zrange(key, -1, -1)

            if not last_data:
                logger.info(
                    f"No data found for key: {key}. Returning an empty Series."
                )
                return pd.Series()

            # Use the common function to convert the Redis data into a Series
            last_row = self.redis_data_to_series(key, last_data[0])

            logger.trace(
                f"Retrieved last row data with date: {last_row['date'] if 'date' in last_row else 'No date'}"
            )
            return last_row

        except Exception as e:
            logger.error(f"Error retrieving last row for key {key}: {e}")
            return pd.Series()

    @trace_function
    def check_date_range_mapper_data(
        self, key: str, timeframe: str, end_date: datetime, duration: str
    ) -> bool:
        """
        Retrieve data from Redis cache using sorted sets.
        This function is made when start_date and end_date are not fixed at the initial boot-up
        This is usually used during development
        Args:
            key (str): The cache key to retrieve the data.
            end_date (datetime): The end date of the data range.
            duration (str): The duration of the data range.

        Returns:
            Optional[pd.DataFrame]: The cached data if available, otherwise None.
        """
        assert isinstance(end_date, datetime) or (
            not end_date
        ), "Invalidate end_date format"

        # Fetch actual date range from HistoricalDataRangeManager
        actual_dates = self.date_range_mapper.get_actual_dates(
            f"{key}:date_mapping", timeframe, end_date, duration
        )
        if not actual_dates:
            logger.info(
                f"No actual dates found for key: {key}. Skipping cache search."
            )
            return False
        else:
            return True

    @trace_function
    def remove_recent_duplicate_scores(
        self,
        key: str,
        time_limit: timedelta = timedelta(days=30),
        batch_size: int = 4000,
    ):
        """
        Checks for duplicate scores in the Redis sorted set within a specific time range and removes all
        but the latest duplicate if found.

        Args:
            key (str): The Redis cache key to check for duplicates.
            time_limit (timedelta): The time range within which to check for duplicates (e.g., last 30 days).
            batch_size (int): The number of elements to fetch per batch.
        """
        try:
            cursor = 0
            score_to_elements = {}
            current_time = datetime.now()
            time_threshold = (current_time - time_limit).timestamp()

            while True:
                # Fetch a batch of elements with their scores
                cursor, elements_with_scores = self.redis.zscan(
                    key, cursor, count=batch_size
                )

                if not elements_with_scores:
                    break

                for element, score in elements_with_scores:
                    # Only consider elements within the time threshold
                    if score >= time_threshold:
                        if score not in score_to_elements:
                            score_to_elements[score] = []
                        score_to_elements[score].append((element, score))

                # Check for duplicates and keep only the latest element
                for score, elements in score_to_elements.items():
                    if len(elements) > 1:
                        # Sort elements by timestamp (assuming the score is a timestamp)
                        elements.sort(key=lambda x: x[1], reverse=True)

                        # Print the timestamp in a readable format
                        timestamp = datetime.fromtimestamp(score).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        logger.critical(
                            f"Duplicate score {score} (timestamp: {timestamp}) found"
                            f" with {len(elements)} elements within time range."
                        )

                        # Remove all but the latest element
                        for element, _ in elements[1:]:
                            self.redis.zrem(key, element)
                            logger.critical(
                                f"Removed duplicate element with score {score} within time range."
                            )

                # Clear the dictionary for the next batch
                score_to_elements.clear()

                # Stop if we've reached the end of the set
                if cursor == 0:
                    break

        except Exception as e:
            logger.error(f"Error while checking and removing duplicates: {e}")

    @trace_function
    def _store_data(self, key: str, data: pd.DataFrame):
        """
        Store data in Redis cache.
        """
        pipeline = self.redis.pipeline()
        for index, row in data.iterrows():
            try:
                score = row["date"].timestamp()
                row_copy = row.copy()
                row_copy["date"] = row_copy["date"].strftime(
                    TBOT_WIND_REDIS_DATE_FORMAT
                )
                # logger.trace(f"Converted date to ISO format: {row_copy['date']}.")
                packed_data = msgpack.packb(
                    row_copy.to_dict(), use_bin_type=True
                )
                self._add_unique_element(pipeline, key, packed_data, score)
            except Exception as e:
                logger.error(f"Error processing row at index {index}: {e}")
                continue

        pipeline.expire(key, self.ttl)
        pipeline.execute()

    @trace_function
    def store_data_with_mapping(
        self,
        key: str,
        data: pd.DataFrame,
        timeframe: str,
        end_date: datetime,
        duration: str,
    ):
        """
        Store data in Redis cache using sorted sets.

        Args:
            key (str): The cache key to store the data.
            data (pd.DataFrame): The data to store in the cache.
            timeframe (str): The timeframe.
            end_date (datetime): The end date of the data range.
            duration (str): The duration of the data range.
        """
        assert isinstance(end_date, datetime) or (
            not end_date
        ), "Invalidate end_date format"

        if "date" not in data.columns:
            logger.critical(
                f"Missing 'date' column in data. Cannot cache data for {key}."
            )
            pretty_print_df(data)
            raise KeyError("Missing 'date' column in DataFrame.")

        # Ensure the 'date' column is timezone-aware
        assert (
            data["date"].dt.tz is not None
        ), "The 'date' column must be timezone-aware. Failing due to missing timezone info."

        try:
            self._store_data(key, data)
            logger.trace(f"cache:set: Data cached for {key}.")

            # Update actual dates in the HistoricalDataRangeManager
            actual_start_date = data["date"].min()
            actual_end_date = data["date"].max()
            self.date_range_mapper.set_actual_dates(
                f"{key}:date_mapping",
                timeframe,
                end_date,
                duration,
                actual_start_date,
                actual_end_date,
            )
            logger.warning(
                f"Max date {actual_end_date.isoformat()}, {actual_end_date.timestamp()} in the cache."
            )

        except Exception as e:
            logger.error(f"Failed to cache data for {key}: {e}")
            raise  # Re-raise the exception to propagate it further

    @trace_function
    def store_data_with_cache_cleanup(self, key: str, data: pd.DataFrame):
        """
        Store data in Redis cache using sorted sets.

        Args:
            key (str): The cache key to store the data.
            data (pd.DataFrame): The data to store in the cache.
        """

        if "date" not in data.columns:
            logger.critical(
                f"Missing 'date' column in data. Cannot cache data for {key}."
            )
            pretty_print_df(data)
            raise KeyError("Missing 'date' column in DataFrame.")

        # Ensure the 'date' column is timezone-aware
        if data["date"].dt.tz is None:
            logger.critical(
                "'date' column is naive. Ensure timezone localization is handled elsewhere."
            )
            raise KeyError("Missing 'timezone' column in DataFrame.")

        try:
            pipeline = self.redis.pipeline()
            for index, row in data.iterrows():
                try:
                    score = row["date"].timestamp()
                    row_copy = row.copy()
                    row_copy["date"] = row_copy["date"].strftime(
                        TBOT_WIND_REDIS_DATE_FORMAT
                    )
                    packed_data = msgpack.packb(
                        row_copy.to_dict(), use_bin_type=True
                    )
                    self._add_unique_element(pipeline, key, packed_data, score)
                except Exception as e:
                    logger.error(f"Error processing row at index {index}: {e}")
                    continue

            pipeline.expire(key, self.ttl)
            pipeline.execute()
            logger.trace(f"cache:set: Data cached for {key}.")
            _end_date = data["date"].max()
            logger.info(
                f"Save max date {_end_date.isoformat()}, {_end_date.timestamp()} in the cache."
            )

        except Exception as e:
            logger.error(f"Failed to cache data for {key}: {e}")
            raise  # Re-raise the exception to propagate it further

    @trace_function
    def check_date_range_mapping_data(
        self, key: str, timeframe: str, end_date: datetime, duration: str
    ) -> bool:
        """
        Retrieve data from Redis cache using sorted sets.
        This function is made when start_date and end_date are not fixed at the initial boot-up
        This is usually used during development
        Args:
            key (str): The cache key to retrieve the data.
            end_date (datetime): The end date of the data range.
            duration (str): The duration of the data range.

        Returns:
            Optional[pd.DataFrame]: The cached data if available, otherwise None.
        """
        assert isinstance(end_date, datetime) or (
            not end_date
        ), "Invalidate end_date format"

        # Fetch actual date range from HistoricalDataRangeManager
        actual_dates = self.date_range_mapper.get_actual_dates(
            f"{key}:date_mapping", timeframe, end_date, duration
        )
        if not actual_dates:
            logger.info(
                f"No actual dates found for key: {key}. Skipping cache search."
            )
            return False
        else:
            return True

    def _is_end_date_in_cache(self, key: str, end_date: datetime) -> bool:
        score = end_date.timestamp()
        members = self.redis.zrangebyscore(key, score, score)
        if not members:
            logger.warning(
                f"End date {end_date.isoformat()}, {end_date.timestamp()} is not present in the cache."
            )
            return False
        logger.trace(
            f"End date {end_date} found in cache, proceeding to fetch data."
        )
        return True

    @trace_function
    def get_cached_data(
        self, key: str, end_date: datetime, win_size_sec: int
    ) -> Optional[pd.DataFrame]:
        """
        Use get_filtered_cached_data when you need to ensure the data is strictly
        within a specific time range, including additional filtering.
        """
        assert isinstance(end_date, datetime), "Invalid end_date format"

        # Log the window size in different formats
        window_size_hours = round(win_size_sec / 3600, 2)
        window_size_days = round(
            win_size_sec / 86400, 2
        )  # 86400 seconds in a day

        logger.debug(
            f"Fetching cached data window with size: {win_size_sec} seconds, "
            f"{window_size_hours} hours, {window_size_days} days."
        )

        calculated_start_date = end_date - timedelta(seconds=win_size_sec)
        logger.info(
            f"Calculated cached start date: {calculated_start_date}, End date: {end_date}"
        )

        cached_data = self._fetch_data_from_redis(
            key, calculated_start_date, end_date
        )
        if cached_data is None or cached_data.empty:
            return pd.DataFrame()

        return self._filter_dataframe_by_date(
            cached_data, calculated_start_date, end_date
        )
        score = end_date.timestamp()
        members = self.redis.zrangebyscore(key, score, score)
        if not members:
            logger.warning(
                f"End date {end_date.isoformat()}, {end_date.timestamp()} is not present in the cache."
            )
            return False
        logger.trace(
            f"End date {end_date} found in cache, proceeding to fetch data."
        )
        return True

    def _filter_dataframe_by_date(
        self, df: pd.DataFrame, start_date: datetime, end_date: datetime
    ) -> Optional[pd.DataFrame]:
        # Capture the original length of the DataFrame
        original_len = len(df)
        actual_start_date = df["date"].min()

        # Log a message if the actual start date differs from the expected start date
        if actual_start_date != start_date:
            logger.debug(
                f"Calculated cached start_date {start_date.isoformat()} differs "
                f"from actual cached start_date {actual_start_date.isoformat()}."
            )

        # Filter the DataFrame directly based on the datetime comparison
        filtered_df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
        filtered_len = len(filtered_df)

        # Compare the original length with the filtered length
        if filtered_len != original_len:
            logger.warning(
                f"Cached is filtered due to discrepancies: Origin len={original_len}, filtered len={filtered_len}."
            )

        # Log the filtered DataFrame size and date range
        logger.info(
            f"Filtered cached: len={filtered_len}, start_date {filtered_df['date'].min()} to {filtered_df['date'].max()}"
        )

        return filtered_df
