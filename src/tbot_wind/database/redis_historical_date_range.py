from datetime import datetime, timedelta
import redis
from loguru import logger
from typing import Optional, Tuple
from typing import Dict
import msgpack
from ..utils.tbot_log import trace_function
from ..utils.tbot_env import shared


class RedisHistoricalDataRangeManager:
    def __init__(self, redis_client: redis.StrictRedis):
        """
        Initialize the RedisHistoricalDataRangeManager for managing start and end dates
        of historical data in a Redis database.
        Args:
            redis_client (redis.StrictRedis): The Redis client to use for storing and fetching mappings.
        """
        self.ttl = shared.r_ttl
        self.redis_client = redis_client
        self.date_range_mapping: Dict[str, Dict[str, Optional[datetime]]] = {}

    def initialize_estimated_date_range(
        self,
        key: str,
        timeframe: str,
        duration: str,
        end_date: Optional[datetime] = None,
    ):
        """
        Set the estimate start_date and end_date
        """
        if key not in self.date_range_mapping:
            self.date_range_mapping[key] = {}

        if not end_date:
            end_date = self._round_to_nearest_bar(timeframe)

        start_date = self._calculate_start_date(end_date, duration)
        # calculated start_date over duration + end_date(current local time when the API is called)
        self.date_range_mapping[key]["start_date"] = start_date
        # calculated end_date(current local time when the API is called)
        self.date_range_mapping[key]["end_date"] = end_date

        # The start date of the received bars
        self.date_range_mapping[key]["actual_start_date"] = None
        # The end date of the received bars
        self.date_range_mapping[key]["actual_end_date"] = None

        logger.debug(
            f"Initialized time mapping for key: {key} with start_date: {start_date}, end_date: {end_date}"
        )

    def _get_redis_hash_key(self, key: str, flag: bool) -> str:
        """
        Generates the hash key used for mapping start and end dates.

        Args:
            key (str): The original key.
            flag (bool): True for end date mapping, False for start date mapping.

        Returns:
            str: The Redis hash key for the time mapping.
        """
        return f"{key}:end" if flag else f"{key}:start"

    @trace_function
    def _get_actual_date_range_from_cache(self, key: str):
        """
        .

        Args:
            key (str): The cache key to retrieve the date range.
        """
        logger.debug(f"Fetching actual date range mapping for key: {key}.")

        try:
            start_mapping_value = self.redis_client.hget(
                self._get_redis_hash_key(key, False),
                self.date_range_mapping[key]["start_date"].timestamp(),
            )
            end_mapping_value = self.redis_client.hget(
                self._get_redis_hash_key(key, True),
                self.date_range_mapping[key]["end_date"].timestamp(),
            )

            if start_mapping_value:
                unpacked_value = msgpack.unpackb(
                    start_mapping_value, raw=False
                )
                actual_start_date = datetime.fromisoformat(
                    unpacked_value["actual_start_date"]
                )
                logger.debug(
                    f"Found actual start date mapping for key: {key} with {unpacked_value}"
                )
                self.date_range_mapping[key][
                    "actual_start_date"
                ] = actual_start_date

            if end_mapping_value:
                unpacked_value = msgpack.unpackb(end_mapping_value, raw=False)
                actual_end_date = datetime.fromisoformat(
                    unpacked_value["actual_end_date"]
                )
                logger.debug(
                    f"Found actual end date mapping for key: {key} with {unpacked_value}"
                )
                self.date_range_mapping[key][
                    "actual_end_date"
                ] = actual_end_date

            if not start_mapping_value:
                self.date_range_mapping[key]["actual_start_date"] = None

            if not end_mapping_value:
                self.date_range_mapping[key]["actual_end_date"] = None

        except Exception as e:
            logger.error(
                f"Failed to fetch actual date range mapping for key: {key}: {e}"
            )
            raise

    @trace_function
    def _round_to_nearest_bar(
        self, timeframe: str = "1D", date: Optional[datetime] = None
    ) -> datetime:
        """
        Round the given datetime to the nearest bar based on the timeframe.
        If no datetime is provided, use the current local time.

        Args:
            timeframe (str): The timeframe for trading.
            date (Optional[datetime]): The datetime to be rounded. Defaults to None.

        Returns:
            datetime: The rounded datetime.
        """
        if date is None:
            date = datetime.now()

        if not isinstance(date, datetime):
            raise TypeError("Expected 'date' to be a datetime object")

        if timeframe in ["1S", "5S", "10S", "15S", "30S"]:
            seconds = int(timeframe[:-1])
            rounded = date - timedelta(
                seconds=date.second % seconds, microseconds=date.microsecond
            )

        elif timeframe in ["1", "5", "15", "30", "60", "120", "240"]:
            minutes = int(timeframe) if timeframe != "60" else 60
            rounded = date - timedelta(
                minutes=date.minute % minutes,
                seconds=date.second,
                microseconds=date.microsecond,
            )

        elif timeframe == "1D":
            rounded = date.replace(hour=0, minute=0, second=0, microsecond=0)

        elif timeframe == "1W":
            start_of_week = date - timedelta(days=date.weekday())
            rounded = start_of_week.replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        elif timeframe == "1M":
            rounded = date.replace(
                day=1, hour=0, minute=0, second=0, microsecond=0
            )

        else:
            logger.error(f"Unrecognized timeframe: {timeframe}")
            raise ValueError(f"Invalid timeframe: {timeframe}")

        logger.debug(
            f"Rounded time to nearest bar for timeframe {timeframe}: {rounded}"
        )
        return rounded

    @trace_function
    def _calculate_start_date(
        self, end_date: datetime, duration: str
    ) -> datetime:
        unit_to_seconds = {
            "S": 1,
            "D": 86400,  # 1 day
            "W": 7 * 86400,  # 1 week
            "M": 30 * 86400,  # 1 month
            "Y": 365 * 86400,  # 1 year
        }

        value = int(duration[:-1])
        unit = duration[-1]
        duration_seconds = value * unit_to_seconds.get(unit, 0)

        start_date = end_date - timedelta(seconds=duration_seconds)

        while start_date.weekday() > 4:  # Adjust if start_date is a weekend
            start_date -= timedelta(days=1)

        logger.debug(f"Calculated start_date: {start_date.isoformat()}")
        return start_date

    def get_actual_dates(
        self,
        key: str,
        timeframe: str,
        end_date: Optional[datetime] = None,
        duration: str = None,
    ) -> Optional[Tuple[datetime, datetime]]:
        """
        Retrieve the actual start and end dates based on the provided key, duration, and end date.

        Args:
            key (str): The cache key.
            duration (str): The duration string.
            end_date (Optional[datetime]): The ending date. Defaults to None.

        Returns:
            Optional[Tuple[datetime, datetime]]: The actual start and end dates, or None if not found.
        """
        # Always call initialize_estimated_date_range to refresh the time mapping
        self.initialize_estimated_date_range(
            key,
            timeframe,
            duration,
            (
                self._round_to_nearest_bar(timeframe, end_date)
                if end_date
                else None
            ),
        )

        # Load actual date range from Redis cache
        self._get_actual_date_range_from_cache(key)

        actual_start_date = self.date_range_mapping[key]["actual_start_date"]
        actual_end_date = self.date_range_mapping[key]["actual_end_date"]

        if actual_start_date is None or actual_end_date is None:
            logger.debug(
                f"Actual dates not found for key: {key}. Returning None."
            )
            return None

        logger.debug(
            f"Retrieved actual dates for key: {key} - start: {actual_start_date}, end: {actual_end_date}"
        )
        return actual_start_date, actual_end_date

    @trace_function
    def set_actual_dates(
        self,
        key: str,
        timeframe: str,
        end_date: datetime,
        duration: str,
        actual_start_date: datetime,
        actual_end_date: datetime,
    ):
        """
        Update the actual start and end dates in the time mapping and Redis cache.

        Args:
            key (str): The cache key.
            actual_start_date (datetime): The actual start date.
            actual_end_date (datetime): The actual end date.
        """
        # Ensure the end_date is a datetime object
        if isinstance(end_date, str):
            raise TypeError(
                "Expected 'end_date' to be a datetime object, not a string."
            )

        rounded_end_date = (
            self._round_to_nearest_bar(timeframe, end_date)
            if end_date
            else None
        )

        # Always call initialize_estimated_date_range to refresh the time mapping
        self.initialize_estimated_date_range(
            key, timeframe, duration, rounded_end_date
        )
        if key in self.date_range_mapping:
            self.date_range_mapping[key][
                "actual_start_date"
            ] = actual_start_date
            self.date_range_mapping[key]["actual_end_date"] = actual_end_date

            self._set_actual_date_range_in_redis(
                key, actual_start_date, actual_end_date
            )
            logger.debug(
                f"Updated actual dates for key: {key} - start: {actual_start_date}, end: {actual_end_date}"
            )

    @trace_function
    def _set_actual_date_range_in_redis(
        self, key: str, actual_start_date: datetime, actual_end_date: datetime
    ):
        """
        Set the actual start and end date range in Redis cache.

        Args:
            key (str): The cache key.
            actual_start_date (datetime): The actual start date.
            actual_end_date (datetime): The actual end date.
        """
        logger.debug(f"Setting actual date range mapping for key: {key}.")

        try:
            # Ensure dates are datetime objects
            assert isinstance(
                actual_start_date, datetime
            ), f"actual_start_date must be a datetime object, got {type(actual_start_date)}"
            assert isinstance(
                actual_end_date, datetime
            ), f"actual_end_date must be a datetime object, got {type(actual_end_date)}"

            start_mapping_key = self._get_redis_hash_key(key, False)
            end_mapping_key = self._get_redis_hash_key(key, True)

            # Store start date mapping
            start_mapping_value = {
                "actual_start_date": actual_start_date.isoformat(),
            }
            self.redis_client.hset(
                start_mapping_key,
                self.date_range_mapping[key]["start_date"].timestamp(),
                msgpack.packb(start_mapping_value, use_bin_type=True),
            )
            # Store end date mapping
            end_mapping_value = {
                "actual_end_date": actual_end_date.isoformat(),
            }
            self.redis_client.hset(
                end_mapping_key,
                self.date_range_mapping[key]["end_date"].timestamp(),
                msgpack.packb(end_mapping_value, use_bin_type=True),
            )

            # Set TTL if provided
            if self.ttl is not None:
                self.redis_client.expire(start_mapping_key, self.ttl)
                self.redis_client.expire(end_mapping_key, self.ttl)

            logger.info(
                f"Successfully set actual date range mapping for key: {key}"
            )

        except Exception as e:
            logger.error(
                f"Failed to set actual date range mapping for {key}: {e}"
            )
            raise
