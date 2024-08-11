import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from loguru import logger
from .stock_datafetcher import StockDataFetcher

from ..utils.objects import OrderTV
from ..utils.tbot_utils import (
    calculate_tf_interval,
    convert_duration_to_seconds,
)


class YFinanceDataFetcher(StockDataFetcher):
    def __init__(self):
        pass

    def timeframe_to_yfinance_interval(self, tf: str) -> str:
        """
        Converts custom timeframes to yfinance-compatible intervals.
        """
        tf_to_yf = {
            "1S": "1m",  # yfinance does not support second-based intervals, map to the smallest: 1 minute
            "5S": "1m",  # No 5-second support, map to 1 minute
            "10S": "1m",  # No 10-second support, map to 1 minute
            "15S": "1m",  # No 15-second support, map to 1 minute
            "30S": "1m",  # No 30-second support, map to 1 minute
            "1": "1m",  # 1 minute
            "5": "5m",  # 5 minutes
            "15": "15m",  # 15 minutes
            "30": "30m",  # 30 minutes
            "60": "1h",  # 1 hour
            "120": "2h",  # 2 hours
            "240": "4h",  # 4 hours
            "1D": "1d",  # 1 day
            "1W": "1wk",  # 1 week
            "1M": "1mo",  # 1 month
        }

        interval = tf_to_yf.get(tf)
        if not interval:
            raise ValueError(f"Unsupported timeframe: {tf}")
        return interval

    async def fetch_realtime_data(
        self, order: OrderTV, duration: str, end_date: str = None
    ) -> pd.DataFrame:
        """
        Fetch historical data from yfinance based on the order.

        Args:
            order (OrderTV): The order details, including symbol, timeframe, etc.
            duration (str): The duration string (e.g., '30 D', '1 M').
            end_date (str, optional): The end date for the data fetch (in 'YYYY-MM-DD' format).

        Returns:
            pd.DataFrame: The historical stock data.
        """
        # Convert timeframe to yfinance-compatible interval
        interval = self.timeframe_to_yfinance_interval(order.timeframe)

        # If end_date is not provided, set it based on the interval
        if not end_date:
            if interval in ["1m", "5m", "15m", "30m", "60m", "1h"]:
                # Use current date and time for intraday data
                end_date_dt = datetime.now()
            else:
                # Use current date for daily or longer intervals
                end_date_dt = datetime.now().replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
        else:
            # Convert end_date to a datetime object if provided
            end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

        duration_seconds = convert_duration_to_seconds(duration)
        start_date_dt = end_date_dt - timedelta(seconds=duration_seconds)

        # Handle intraday data restriction (up to 30 or 60 days back for intraday intervals)
        if interval in ["1m", "5m", "15m", "30m", "60m", "1h"]:
            # Restrict intraday data to 60 days
            max_intraday_days = 60
            intraday_cutoff_date = end_date_dt - timedelta(
                days=max_intraday_days
            )
            if start_date_dt < intraday_cutoff_date:
                logger.warning(
                    f"Intraday data for {order.symbol} is restricted to 60 days. Adjusting start date."
                )
                start_date_dt = intraday_cutoff_date

        logger.info(
            f"Fetching data for {order.symbol} from {start_date_dt} to {end_date_dt} with interval: {interval}"
        )

        # Fetch the historical data using yfinance
        df = yf.download(
            order.symbol,
            start=start_date_dt,
            end=end_date_dt,
            interval=interval,
        )

        # Ensure the required columns are present and handle missing columns
        required_columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
        missing_columns = [
            col for col in required_columns if col not in df.columns
        ]
        if missing_columns:
            logger.critical(
                f"Missing columns in fetched data: {missing_columns} from {df.columns}"
            )
            return (
                pd.DataFrame()
            )  # Return empty DataFrame if columns are missing

        # Reset index and make sure the 'Date' column is part of the DataFrame
        df = df.reset_index()

        # Rename the 'Date' column to 'date' to maintain consistency
        df.rename(columns={"Date": "date"}, inplace=True)

        if df.empty:
            logger.warning(
                f"No data fetched for {order.symbol} using yfinance."
            )
            return df

        # Ensure the 'date' column is in the correct timezone
        df["date"] = pd.to_datetime(df["date"])
        df["date"] = df["date"].dt.tz_localize(ZoneInfo(order.timezone))

        # Ensure the DataFrame has the correct columns and return only the required ones
        df = df[["date", "open", "high", "low", "close", "volume"]]

        logger.info(
            f"Fetched {len(df)} rows for {order.symbol} from yfinance."
        )
        return df

    async def retrieve_data_in_chunks(
        self, order: OrderTV, total_duration: str, end_date: str
    ) -> pd.DataFrame:
        """
        Fetch historical data from yfinance in chunks.

        Args:
            order (OrderTV): The order details, including symbol, timeframe, etc.
            total_duration (str): Total duration string (e.g., '30 D').
            end_date (str): The end date for the data fetch (in 'YYYY-MM-DD' format).

        Returns:
            pd.DataFrame: A concatenated DataFrame with all the data chunks.
        """
        # Calculate the end date and total duration in seconds
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
        total_duration_seconds = convert_duration_to_seconds(total_duration)

        # Get the timeframe interval in seconds
        interval_seconds = calculate_tf_interval(order.timeframe)

        # Calculate the chunk size based on the interval
        chunk_duration_seconds = (
            interval_seconds * 100
        )  # Adjust chunk size (100 intervals per chunk)

        all_data = pd.DataFrame()

        logger.info(
            f"Fetching data for {order.symbol} in chunks. Total duration: {total_duration}"
        )

        # Fetch data in chunks
        for chunk_start_seconds in range(
            0, total_duration_seconds, chunk_duration_seconds
        ):
            chunk_end_seconds = min(
                chunk_start_seconds + chunk_duration_seconds,
                total_duration_seconds,
            )
            chunk_start_date = end_date_dt - timedelta(
                seconds=chunk_end_seconds
            )
            chunk_end_date = end_date_dt - timedelta(
                seconds=chunk_start_seconds
            )

            logger.info(
                f"Fetching data chunk from {chunk_start_date} to {chunk_end_date}"
            )

            chunk_data = await self.fetch_realtime_data(
                order,
                duration=self.seconds_to_duration(
                    chunk_end_seconds - chunk_start_seconds
                ),
                end_date=chunk_end_date.strftime("%Y-%m-%d"),
            )

            if chunk_data.empty:
                logger.warning(
                    f"No data fetched for chunk {chunk_start_date} to {chunk_end_date}"
                )
                continue

            # Concatenate the chunk data
            all_data = pd.concat([all_data, chunk_data], ignore_index=True)

        # Sort the combined data by date and drop duplicates
        all_data = all_data.sort_values(by="date").drop_duplicates(
            subset=["date"], keep="first"
        )

        logger.info(
            f"Finished fetching all data in chunks for {order.symbol}."
        )
        return all_data

    def seconds_to_duration(self, seconds: int) -> str:
        """
        Convert a duration in seconds to a human-readable format (e.g., '1 D', '2 W').
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
