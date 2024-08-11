import asyncio
from typing import List
from loguru import logger

from .scheduler import Scheduler
from .ib_connection_manager import IBConnectionManager
from .ib_data_fetcher import IBDataFetcher
from .data_context_fetcher import DataFetcherContext

from ..database.database_wrapper import DataBaseWrapper
from ..utils.objects import OrderTV
from ..utils.constants import (
    TBOT_WIND_CONSUMER_NUM,
    FETCH_MODE_FUNCTION_MAP,
    current_fetch_mode,
)


class TbotWind:
    def __init__(
        self, indicator, db_wrapper: DataBaseWrapper, orders: List[OrderTV]
    ):
        logger.info("Initializing TBOT-Wind to TradingBoat")

        # Filter out orders that do not match the indicator's name
        self.orders = [
            order for order in orders
            if indicator.__class__.__name__.lower().startswith(order.indicator.lower())
        ]

        if not self.orders:
            logger.warning(f"No orders match the indicator: {indicator.__class__.__name__}")
            return
        self.indicator = indicator

        self.db_wrapper = db_wrapper
        self.connection_manager = IBConnectionManager()
        self.fetcher = IBDataFetcher(self.connection_manager)
        self.context_fetcher = DataFetcherContext(
            self.db_wrapper, indicator.warm_win_loopback, self.fetcher
        )

        self.order_queue = asyncio.Queue()
        self.schedulers = {
            f"{order.symbol}_{order.timeframe}": Scheduler(
                order, market_close_time="16:00", threshold_seconds=90
            )
            for order in self.orders
        }

        logger.success(f"TBOT-Wind initialized for {len(self.orders)} orders.")

    async def fetch_data_producer(self):
        """Producer coroutine to fetch data for each order and put it into the queue."""
        while True:
            for order in self.orders:
                fetch_function_name = FETCH_MODE_FUNCTION_MAP.get(
                    current_fetch_mode, "fetch_stream_data"
                )
                fetch_function = getattr(
                    self.context_fetcher, fetch_function_name, None
                )
                if fetch_function:
                    # Call the chosen function
                    data = await fetch_function(order)
                    if not data.empty:
                        await self.order_queue.put((order, data))
                    else:
                        logger.info(
                            f"Fetched 0 rows for {order.symbol}. Skipping."
                        )
                else:
                    logger.error(
                        f"Fetch function {fetch_function_name} not found in context_fetcher."
                    )
            await asyncio.sleep(self._calculate_min_sleep_duration())

    async def process_data_consumer(self):
        """Consumer coroutine to process data from the queue."""
        while True:
            order, data = await self.order_queue.get()
            if self.indicator.__class__.__name__.lower().startswith(order.indicator.lower()):
                processed_data = await self.indicator.event_loop(order, data)
                if not processed_data.empty:
                    self.context_fetcher.cache_data(order, processed_data)
                self.order_queue.task_done()
            else:
                logger.info(f"Skipping {order.symbol} {order.indicator}- Indicator mismatch.")

    async def process_data_every_interval(self):
        """Run both producer and consumer tasks concurrently."""
        producer = asyncio.create_task(self.fetch_data_producer())
        consumers = [
            asyncio.create_task(self.process_data_consumer())
            for _ in range(TBOT_WIND_CONSUMER_NUM)
        ]

        # Wait for producer and consumers to complete (if you plan to have a stopping condition)
        await asyncio.gather(producer, *consumers)

    def _calculate_min_sleep_duration(self):
        sleep_durations = [
            scheduler.determine_sleep_duration()
            for scheduler in self.schedulers.values()
        ]
        return min(sleep_durations) if sleep_durations else 1

    async def blow(self):
        try:
            await self.process_data_every_interval()
        except asyncio.CancelledError:
            logger.exception("Tasks were cancelled. Cleaning up...")
        except Exception as e:
            logger.exception(f"An unexpected error occurred during run: {e}")
        finally:
            logger.info("Disconnecting IBDataFetcher...")
            self.connection_manager.disconnect()
            logger.info("Disconnected successfully.")
