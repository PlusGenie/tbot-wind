from typing import List
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger

from .redis_wrapper import RedisWrapper
from .sqlite3_wrapper import Sqlite3Wrapper

from ..utils.objects import OrderTV
from ..utils.constants import (
    TBOT_WIND_INDICATOR_PREFIX,
    TBOT_WIND_USES_SQLITE_DATABASE,
)
from ..utils.tbot_log import trace_function
from ..utils.tbot_datetime import get_key_timezone


class DataBaseWrapper:
    def __init__(
        self,
        orders: List[OrderTV] = [],
        indicator_name: str = "def_indicator",
    ):
        """
        Initialize the DatabaseWrapper.

        Args:
            redis_wrapper: Instance of RedisWrapper.
            sqlite_wrapper: Instance of Sqlite3Wrapper.
            use_redis: A flag to choose Redis operations.
            use_sqlite: A flag to choose SQLite operations.
        """
        self.use_sqlite = True if TBOT_WIND_USES_SQLITE_DATABASE else False
        self.indicator_name = indicator_name
        if self.use_sqlite:
            self.sqlite = Sqlite3Wrapper()
            self.use_redis = False
            self.redis = None
        else:
            self.sqlite = None
            self.use_redis = True
            self.redis = RedisWrapper()

        if self.use_sqlite:
            self.setup_sqlite_connection(orders)

    @trace_function
    def store_data(self, key: str, data: pd.DataFrame):
        """Store data into Redis or SQLite depending on the configuration."""
        if self.use_redis:
            self.redis.store_data_with_cache_cleanup(key, data)

        if self.use_sqlite:
            table_name = self.get_table_name(key)
            tz = get_key_timezone(key)
            self.sqlite.insert_ordered_dataframe(table_name, data, tz)

    def get_table_name(self, key: str) -> str:
        """
        Derive table name based on the key format, replacing invalid characters for SQLite.
        """
        # Replace invalid characters like "/" with an underscore or another allowed character
        table_name = key.replace(":", "_").replace("/", "_")
        return table_name

    def get_last_row(self, key: str) -> pd.Series:
        """Retrieve the last row of data from Redis or SQLite."""
        if self.use_redis:
            last_row = self.redis.get_last_row(key)

        if self.use_sqlite:
            tz = get_key_timezone(key)
            last_row = self.sqlite.fetch_last_row(
                self.get_table_name(key), "date", tz
            )

        return last_row

    @trace_function
    def get_closest_data(
        self, key: str, timeframe: str, end_date: datetime, duration: str
    ) -> pd.DataFrame:
        """
        Retrieve data from Redis or SQLite depending on the configuration.
        """
        if self.use_redis:
            redis_data = self.redis.get_closest_data(
                key, timeframe, end_date, duration
            )
            if not redis_data.empty:
                return redis_data

        if self.use_sqlite:
            return self.sqlite.fetch_last_row(self.get_table_name(key), "date")

        return pd.DataFrame()

    @trace_function
    def get_cached_data(
        self, key: str, end_date: datetime, win_size_sec: int
    ) -> pd.DataFrame:
        """
        Retrieve cached data from Redis or SQLite depending on the configuration.
        """
        if self.use_redis:
            redis_data = self.redis.get_cached_data(
                key, end_date, win_size_sec
            )
            if not redis_data.empty:
                return redis_data

        if self.use_sqlite:
            tz = get_key_timezone(key)
            start_date = end_date - timedelta(seconds=win_size_sec)
            return self.sqlite.fetch_data_between_dates(
                self.get_table_name(key), "date", start_date, end_date, tz
            )

        return pd.DataFrame()

    def get_key(self, ord: OrderTV) -> str:
        """
        Generate the unique name for a table name that includes the indicator name.
        """
        return (
            f"sy:{ord.symbol}:ex:{ord.exchange}:tf:{ord.timeframe}:tz:{ord.timezone}:"
            f"cr:{ord.currency}:ind:{self.indicator_name}:pf:{TBOT_WIND_INDICATOR_PREFIX}"
        )

    @trace_function
    def setup_sqlite_connection(self, orders: List[OrderTV]):
        """Set up SQLite tables for each order."""
        if self.use_sqlite and self.sqlite:
            # Initialize the SQLite connection once
            self.sqlite.setup_sqlite_connection()

            # Create tables for each order
            for order in orders:
                table_name = self.get_table_name(self.get_key(order))
                self.sqlite.create_table_for_order(table_name)
        return None

    @trace_function
    def remove_recent_duplicate_scores(
        self, key: str, time_limit: timedelta, batch_size: int
    ):
        if self.use_redis:
            return self.redis.remove_recent_duplicate_scores(
                key, time_limit, batch_size
            )
        return None

    def store_data_with_mapping(
        self,
        key: str,
        data: pd.DataFrame,
        timeframe: str,
        end_date: datetime,
        duration: str,
    ):
        if self.use_redis:
            self.redis.store_data_with_mapping(
                key, data, timeframe, end_date, duration
            )

        if self.use_sqlite:
            logger.warning("store_data_with_mapping called over sqlite")
            self.store_data(key, data)

    @trace_function
    def check_date_range_mapping_data(
        self, key: str, timeframe: str, end_date: datetime, duration: str
    ) -> bool:
        if self.use_redis:
            return self.redis.check_date_range_mapping_data(
                key, timeframe, end_date, duration
            )

        if self.use_sqlite:
            table_name = self.get_table_name(key)
            return self.sqlite.check_date_range_mapping_data(
                table_name, timeframe, end_date, duration
            )
        return False

    @trace_function
    def close(self):
        if self.use_redis and self.redis:
            return self.redis.close()

        if self.use_sqlite and self.sqlite:
            return self.sqlite.close()
