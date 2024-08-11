from ib_async import Stock
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
from loguru import logger

from .ib_connection_manager import IBConnectionManager
from .stock_datafetcher import StockDataFetcher

from ..utils.objects import OrderTV
from ..utils.constants import (
    TBOT_WIND_REDIS_DATE_FORMAT,
    TBOT_WIND_IB_VIOLATION_SLEEP_SEC,
)
from ..utils.tbot_log import trace_function
from ..utils.tbot_utils import (
    tf_to_barsize,
    validate_historical_data_request,
    convert_duration_to_seconds,
    calculate_tf_interval,
    bar_size_limits,
)


class IBDataFetcher(StockDataFetcher):
    def __init__(self, connection_manager: IBConnectionManager):
        self.cm = connection_manager

    @trace_function
    async def fetch_realtime_data(
        self, order: OrderTV, duration: str, end_date: str
    ) -> pd.DataFrame:
        """
        Fetch data from IB API based on the order and update the cache.
        """
        barSize = tf_to_barsize(order.timeframe)

        if not validate_historical_data_request(barSize, duration):
            logger.critical(
                f"Historical data request violated IB pacing limits. Aborting: {barSize}, {duration}"
            )
            return pd.DataFrame()

        await self.cm.connect()

        logger.debug(
            f"reqHistoricalDataAsync: {order.symbol}, '{end_date}', {duration}, {barSize}"
        )
        stock = Stock(order.symbol, order.exchange, order.currency)
        try:
            bars = await self.cm.ib.reqHistoricalDataAsync(
                stock,
                endDateTime=end_date,
                durationStr=duration,
                barSizeSetting=barSize,
                whatToShow="TRADES",
                useRTH=True,
            )
            logger.debug(
                f"Data({len(bars)}) fetched from reqHistoricalDataAsync"
            )
        except Exception as e:
            logger.error(f"Error fetching data from API: {e}")
            return pd.DataFrame()

        df = pd.DataFrame(bars)
        if df.empty:
            logger.warning(
                "Received empty DataFrame from reqHistoricalDataAsync"
            )
            return df

        try:
            # Ensure the 'date' column is in datetime format
            if not pd.api.types.is_datetime64_any_dtype(df["date"]):
                logger.debug("Converting 'date' column to datetime format.")
                df["date"] = pd.to_datetime(
                    df["date"], format=TBOT_WIND_REDIS_DATE_FORMAT
                )

            # Now, check if the datetime is naive or has a different timezone than expected
            if df["date"].dt.tz is None:
                # If datetime is naive, localize it to the desired timezone
                logger.debug(
                    f"'date' column is naive. Localizing to timezone: {order.timezone}."
                )
                df["date"] = df["date"].dt.tz_localize(
                    ZoneInfo(order.timezone)
                )
            elif not isinstance(df["date"].dt.tz, ZoneInfo):
                # If the timezone is not a ZoneInfo instance, convert it
                current_tz = df["date"].dt.tz
                logger.critical(
                    f"Converting 'date' column timezone from {current_tz} (likely pytz) to ZoneInfo"
                )
                df["date"] = df["date"].dt.tz_convert(
                    ZoneInfo(str(current_tz))
                )
            elif df["date"].dt.tz != ZoneInfo(order.timezone):
                # If datetime has a different timezone, convert it to the desired timezone
                logger.critical(
                    f"Timezone mismatch: Converting date from {df['date'].dt.tz} to {order.timezone}"
                )
                df["date"] = df["date"].dt.tz_convert(ZoneInfo(order.timezone))
            else:
                # If datetime is already in the correct timezone, no action needed
                logger.trace("'date' column already has the correct timezone.")
        except Exception as e:
            logger.error(f"Failed to process 'date' column. Error: {e}")
            return (
                pd.DataFrame()
            )  # Return an empty DataFrame or handle it as per your needs

        return df

    def seconds_to_duration(self, seconds: int) -> str:
        """
        Convert a duration in seconds to an IB-compatible duration string (e.g., '1 D', '2 W').
        Valid Duration String units
            Unit	Description
            S	Seconds
            D	Day
            W	Week
            M	Month
            Y	Year
        """
        if seconds < 60:
            return f"{seconds} S"
        elif seconds < 3600:
            return f"{seconds // 60} M"
        elif seconds < 86400:
            return f"{seconds // 3600} H"
        elif seconds < 604800:
            return f"{seconds // 86400} D"
        elif seconds < 2592000:
            return f"{seconds // 604800} W"
        elif seconds < 31536000:
            return f"{seconds // 2592000} M"
        else:
            return f"{seconds // 31536000} Y"

    def get_max_duration_seconds_for_bar_size(self, bar_size: str) -> int:
        """
        Returns the maximum duration in seconds allowed for a given bar size.
        """
        if bar_size not in bar_size_limits:
            raise ValueError(f"Unsupported bar size: {bar_size}")

        return bar_size_limits[bar_size]

    async def retrieve_data_in_chunks(
        self, order: OrderTV, total_duration: str, end_date: str
    ) -> pd.DataFrame:
        """
        Fetch historical data from IB in chunks to respect pacing limits.

        :param order: The order information, including symbol, exchange, and currency.
        :param total_duration: Total duration requested (e.g., '30 D').
        :param end_date: The end date for the data request in IB format (e.g., '20230901 16:00:00 US/Eastern').
        :return: A concatenated DataFrame of all chunks.
        """
        bar_size = tf_to_barsize(order.timeframe)

        # Split the end_date string into datetime and timezone parts
        date_parts = end_date.split(" ", 2)

        try:
            end_date_str = (
                f"{date_parts[0]} {date_parts[1]}"  # Combine date and time
            )
            timezone_str = date_parts[2]  # Timezone

            # Parse the date and time part
            end_datetime = datetime.strptime(end_date_str, "%Y%m%d %H:%M:%S")

            # Add timezone using ZoneInfo
            timezone = ZoneInfo(timezone_str)
            end_datetime = end_datetime.replace(tzinfo=timezone)
            end_datetime_utc = end_datetime.astimezone(ZoneInfo("UTC"))

        except Exception as e:
            logger.error(f"Error parsing end date: {end_date}. Error: {e}")
            raise

        # Get the maximum duration allowed by IB for the requested bar size
        max_duration_seconds = self.get_max_duration_seconds_for_bar_size(
            bar_size
        )
        total_duration_seconds = convert_duration_to_seconds(total_duration)

        # Calculate the overlap unit based on the timeframe
        overlap_seconds = calculate_tf_interval(order.timeframe)
        # If timeframe is less than or equal to a day, add one overlap unit
        if overlap_seconds <= 86400:  # 1 day = 86400 seconds
            overlap_seconds += calculate_tf_interval(order.timeframe)

        if total_duration_seconds <= max_duration_seconds:
            # If the total duration is within the allowed limit, just use the existing function
            logger.info(
                f"Duration {total_duration} is within IB limits, fetching data in a single request."
            )
            end_date_str = end_datetime_utc.strftime("%Y%m%d-%H:%M:%S")

            return await self.fetch_realtime_data(
                order,
                total_duration,
                end_datetime.strftime("%Y%m%d %H:%M:%S %Z"),
            )

        logger.info(
            f"Splitting the request for {total_duration} into chunks due to IB pacing limits."
        )

        # Calculate the number of chunks and the duration for each chunk
        num_chunks = (total_duration_seconds // max_duration_seconds) + 1
        chunk_duration_seconds = max_duration_seconds

        all_data = pd.DataFrame()

        # Loop to request data in smaller chunks
        for chunk in range(num_chunks):
            # Calculate the new end date for each chunk
            chunk_end_date = end_datetime_utc - timedelta(
                seconds=(chunk * chunk_duration_seconds) - overlap_seconds
            )

            chunk_end_date_str = chunk_end_date.strftime("%Y%m%d %H:%M:%S %Z")

            # Convert the chunk duration back into IB-compatible format (e.g., '2 D', '1 W')
            chunk_duration_str = self.seconds_to_duration(
                chunk_duration_seconds
            )

            logger.info(
                f"Fetching chunk {chunk+1}/{num_chunks} w/ duration {chunk_duration_str} ending at {chunk_end_date_str}"
            )

            # Fetch the data for the current chunk
            chunk_data = await self.fetch_realtime_data(
                order, chunk_duration_str, chunk_end_date_str
            )

            if chunk_data.empty:
                logger.warning(
                    f"Received empty data for chunk {chunk+1}/{num_chunks}"
                )
            else:
                # Ensure no duplicate rows based on 'date' column before concatenating
                chunk_data = chunk_data.sort_values(by="date").drop_duplicates(
                    subset=["date"], keep="first"
                )
                all_data = pd.concat([all_data, chunk_data], ignore_index=True)

            # Sleep between chunks to avoid pacing violations
            logger.info(
                f"Sleeping for {TBOT_WIND_IB_VIOLATION_SLEEP_SEC} seconds to avoid IB pacing violations."
            )
            await asyncio.sleep(
                TBOT_WIND_IB_VIOLATION_SLEEP_SEC
            )  # Adjust sleep duration based on IB pacing limits

        # Sort and drop duplicates after the final concatenation, just in case
        all_data = all_data.sort_values(by="date").drop_duplicates(
            subset=["date"], keep="first"
        )

        logger.info(f"Finished fetching all data in {num_chunks} chunks.")
        return all_data
