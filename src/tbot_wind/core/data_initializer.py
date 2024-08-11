from datetime import timedelta
import pandas as pd
from loguru import logger
from typing import Optional

from .stock_datafetcher import StockDataFetcher

from ..utils.objects import OrderTVEx, OrderTV
from ..database.database_wrapper import DataBaseWrapper
from ..utils.tbot_log import trace_function, pretty_print_df
from ..utils.tbot_utils import (
    get_last_trading_day,
)


class DataInitializerContext:
    def __init__(
        self,
        db_wrapper: DataBaseWrapper,
        fetcher: Optional[StockDataFetcher] = None,
    ):
        """
        Initializes the DataInitializer to fetch and cache historical data required for running indicators.
        This class ensures that sufficient warmup data is available before starting live trading.
        """
        self.db_wrapper = db_wrapper
        self.fetcher = fetcher

    @trace_function
    def retrieve_full_cached_data(
        self, cache_key: str, timeframe: str, duration: str, end_date: str
    ) -> pd.DataFrame:
        """
        Retrieves data from cache based on the cache_key and duration.

        Args:
            cache_key (str): The Redis cache key.
            timeframe (str): The timeframe of the data.
            duration (str): The duration string.
            end_date (str): The end date for data retrieval.

        Returns:
            pd.DataFrame: DataFrame containing the cached data.
        """
        logger.trace(f"cache:get: duration={duration}, end={end_date}")
        return self.db_wrapper.get_date_range_mapping_data(
            cache_key, timeframe, end_date, duration
        )

    @trace_function
    def check_full_cached_data(
        self, cache_key: str, timeframe: str, end_date: str, duration: str
    ) -> bool:
        """
        Check data from cache based on the cache_key and duration.

        Args:
            cache_key (str): The Redis cache key.
            timeframe (str): The timeframe of the data.
            duration (str): The duration string.
            end_date (str): The end date for data retrieval.

        Returns:
            True if data available, otherwise False
        """
        logger.trace(f"cache:get: duration={duration}, end={end_date}")
        return self.db_wrapper.check_date_range_mapping_data(
            cache_key, timeframe, end_date, duration
        )

    @trace_function
    async def retrieve_data_in_chunks(
        self, order: OrderTV, duration: str, end_date: str
    ) -> pd.DataFrame:
        if not self.fetcher:
            raise ValueError("No data fetcher strategy set!")
        return await self.fetcher.retrieve_data_in_chunks(
            order, duration, end_date
        )

    @trace_function
    async def warmup_data(self, order: OrderTVEx):
        """
        Populates the database with warmup data to ensure that indicators have sufficient historical data.

        Args:
            order (OrderTV): The order details containing symbol, exchange, etc.
            duration (str): The duration string for data retrieval.
        """
        logger.info("Starting warmup process...")
        key = self.db_wrapper.get_key(order)
        end_date_str, end_date = get_last_trading_day()
        cached = self.check_full_cached_data(
            key, order.timeframe, end_date, order.bootup_duration
        )

        if not cached:
            new_df = await self.retrieve_data_in_chunks(
                order, order.bootup_duration, end_date_str
            )
            if not new_df.empty:
                self.cache_warmup_data(new_df, order, end_date)
        else:
            logger.success("Sufficient historical data in Cache")

        self.db_wrapper.remove_recent_duplicate_scores(
            key, time_limit=timedelta(days=365), batch_size=4000
        )
        logger.success(
            f"Warmup process for {order.bootup_duration} completed successfully"
        )

    def cache_warmup_data(self, df: pd.DataFrame, order: OrderTVEx, end_date):
        """
        Caches the raw data into the Redis database after the initial fetch.

        Args:
            df (pd.DataFrame): The DataFrame containing the fetched data.
            order (OrderTV): The order details.
        """
        cache_key = self.db_wrapper.get_key(order)
        self.db_wrapper.store_data_with_mapping(
            cache_key, df, order.timeframe, end_date, order.duration
        )
        logger.success(
            f"Raw Data cached from {df['date'].min()} to {df['date'].max()} with {len(df)} rows"
        )
        pretty_print_df(df.head(3), "INFO")
        pretty_print_df(df.tail(3), "INFO")
