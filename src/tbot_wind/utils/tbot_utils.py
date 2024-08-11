from datetime import datetime, timedelta
from loguru import logger
from zoneinfo import ZoneInfo
import math
from typing import Optional, Tuple

from .tbot_env import shared


PADDING_SEC = 14  # Padding in seconds for timeframes less than a day

bar_size_limits = {
    "1 secs": 60,        # 1 minute
    "5 secs": 120,       # 2 minutes
    "10 secs": 1800,     # 30 minutes
    "15 secs": 3600,     # 1 hour
    "30 secs": 14400,    # 4 hours
    "1 min": 28800,      # 8 hours
    "5 mins": 86400,     # 1 day
    "15 mins": 172800,   # 2 days
    "30 mins": 604800,   # 1 week
    "1 hour": 2592000,   # 1 month
    "2 hours": 5184000,  # 2 months
    "4 hours": 7776000,  # 3 months
    "1 day": 31536000,   # 1 year
    "1 week": 157680000,  # 5 years
}

def calculate_tf_interval(tf: str) -> int:
    """Calculate the interval in seconds for fetching data based on the timeframe."""
    tf_to_int = {
        "1S": 1,
        "5S": 5,
        "10S": 10,
        "15S": 15,
        "30S": 30,
        "1": 60,
        "5": 300,
        "15": 900,
        "30": 1800,
        "60": 3600,
        "120": 7200,
        "240": 14400,
        "1D": 86400,
        "1W": 604800,
        "1M": 2592000,
    }

    interval = tf_to_int.get(tf)
    if interval is None:
        logger.error(f"Invalid timeframe: {tf}")
        raise ValueError(f"Invalid timeframe: {tf}")

    if interval < 60:
        readable = f"{interval} second(s)"
    elif interval < 3600:
        readable = f"{interval // 60} minute(s)"
    elif interval < 86400:
        readable = f"{interval // 3600} hour(s)"
    elif interval < 604800:
        readable = f"{interval // 86400} day(s)"
    else:
        readable = f"{interval // 604800} week(s)"

    logger.debug(f"Timeframe: {tf}, Interval: {readable} ({interval} seconds)")
    return interval


def get_last_trading_day() -> Tuple[str, datetime]:
    """Calculate the last trading day, accounting for weekends and format with timezone."""
    logger.debug("Entering get_last_trading_day function.")

    # Set timezone to US/Eastern by default using ZoneInfo
    tz = ZoneInfo(shared.ib_timezone)
    today = datetime.now(tz)
    logger.debug(f"Current date and time in {shared.ib_timezone}: {today}")

    # If today is Sunday (6), subtract two days to get to Friday
    if today.weekday() == 6:
        last_trading_day = today - timedelta(days=2)
    # If today is Saturday (5), subtract one day to get to Friday
    elif today.weekday() == 5:
        last_trading_day = today - timedelta(days=1)
    else:
        # If today is a weekday, simply subtract one day
        last_trading_day = today - timedelta(days=1)

    # Ensure the time is set to market close (assuming 16:00:00)
    last_trading_day = last_trading_day.replace(
        hour=16, minute=0, second=0, microsecond=0
    )

    # Format the last trading day in the required format with timezone
    formatted_date = (
        f"{last_trading_day.strftime('%Y%m%d %H:%M:%S')} {shared.ib_timezone}"
    )
    logger.info(f"Last trading day calculated: {formatted_date}")

    # Return both the formatted string and the timezone-aware datetime
    return formatted_date, last_trading_day


def tz_format_date(date_str: str) -> datetime:
    """
    Format a datetime string with a specific timezone format.

    Args:
        date_str (str): The date string in the format 'YYYYMMDD HH:MM:SS TIMEZONE'.
                        Example: '20230814 15:30:00 US/Eastern'

    Returns:
        datetime: A timezone-aware datetime object.

    Raises:
        ValueError: If the date string does not match the expected format or the timezone is invalid.
    """
    logger.debug(f"tz_format_date: Received date_str: {date_str}")

    try:
        # Expected format: 'YYYYMMDD HH:MM:SS TIMEZONE'
        dt_str, tz_str = date_str.rsplit(" ", 1)
        logger.debug(
            f"tz_format_date: Split date_str into dt_str: '{dt_str}' and tz_str: '{tz_str}'"
        )

        # Parse the datetime part
        dt = datetime.strptime(dt_str, "%Y%m%d %H:%M:%S")

        # Ensure the timezone is valid
        try:
            timezone = ZoneInfo(tz_str)
        except Exception as e:
            logger.critical(
                f"tz_format_date: Invalid timezone '{tz_str}'. Error: {e}"
            )
            raise ValueError(f"Invalid timezone in date string: {tz_str}")

        # Localize the datetime object to the provided timezone
        dt = dt.replace(tzinfo=timezone)
        logger.debug(
            f"tz_format_date: Successfully parsed and localized datetime: {dt.isoformat()}"
        )
        return dt

    except Exception as e:
        logger.critical(
            f"tz_format_date: Error parsing date_str '{date_str}'. Error: {e}"
        )
        raise ValueError(f"Invalid date format or timezone: {date_str}")


def tf_to_barsize(tf: str) -> str:
    """Convert TradingView timeframes to Interactive Brokers bar size settings."""
    tf_to_bar = {
        "1S": "1 secs",
        "5S": "5 secs",
        "10S": "10 secs",
        "15S": "15 secs",
        "30S": "30 secs",
        "1": "1 min",
        "5": "5 mins",
        "15": "15 mins",
        "30": "30 mins",
        "60": "1 hour",
        "120": "2 hours",
        "240": "4 hours",
        "1D": "1 day",
        "1W": "1 week",
        "1M": "1 month",
    }

    bar_size = tf_to_bar.get(tf)
    if bar_size is None:
        logger.error(f"Invalid timeframe: {tf}")
        raise ValueError(f"Invalid timeframe: {tf}")

    logger.trace(f'Timeframe "{tf}" converted to BarSize "{bar_size}"')
    return bar_size


def tf_to_seconds(tf: str) -> int:
    """Convert timeframe strings to seconds."""
    tf_to_sec = {
        "S": 1,
        "D": 86400,  # 1 day
        "W": 604800,  # 1 week
        "M": 2592000,  # 1 month
        "Y": 31536000,  # 1 year (365 days)
    }

    if tf.isdigit():  # Handle cases like "30" which is 30 minutes
        return int(tf) * 60  # Convert minutes to seconds

    unit = tf[-1]  # Last character is the unit
    multiplier = tf[
        :-1
    ]  # Everything before the last character is the multiplier

    if not multiplier:  # If no multiplier, assume 1
        multiplier = 1
    else:
        multiplier = int(multiplier)

    if unit not in tf_to_sec:
        logger.error(f"Invalid timeframe: {tf}")
        raise ValueError(f"Invalid timeframe: {tf}")

    seconds = multiplier * tf_to_sec[unit]
    logger.trace(f'Timeframe "{tf}" converted to "{seconds}" seconds')
    return seconds


def calculate_window_duration_seconds(timeframe: str, win_size: int) -> int:
    """
    Calculate the actual window size in seconds using the timeframe from the order.

    Args:
        order (OrderTV): The order object containing the timeframe.
        win_size (int): The window size in terms of the number of periods.

    Returns:
        int: The actual window size in seconds.
    """
    actual_window_size = tf_to_seconds(timeframe) * win_size
    logger.debug(
        f"Calculated actual window size: {actual_window_size} seconds,{win_size}."
    )
    return actual_window_size


def get_overlap_duration(timeframe: str, factor: int) -> str:
    """
    Calculate the overlapping window duration for fetching historical data.

    The duration is calculated based on the timeframe (bar size) and an overlapping factor.

    Args:
        timeframe (str): The timeframe (bar size) string, e.g., '1D', '1W'.
        factor (int): The overlapping factor.

    Returns:
        str: The duration string to be used for the reqHistoricalDataAsync method.
    """
    # Convert timeframe to seconds
    timeframe_seconds = tf_to_seconds(timeframe)

    # Calculate the overlapping window size
    overlapping_window_seconds = timeframe_seconds * factor

    # Convert the overlapping window size back to a duration string
    if overlapping_window_seconds % 60 != 0:
        # If it doesn't perfectly convert to a higher unit, return in seconds
        return f"{overlapping_window_seconds} S"

    # Convert the overlapping window size back to a duration string
    if overlapping_window_seconds < 86400:  # Less than a day
        duration_str = f"{overlapping_window_seconds} S"  # Seconds
    elif overlapping_window_seconds < 604800:  # Less than a week
        duration_str = f"{overlapping_window_seconds // 86400} D"  # Days
    elif overlapping_window_seconds < 2592000:  # Less than a month
        duration_str = f"{overlapping_window_seconds // 604800} W"  # Weeks
    elif overlapping_window_seconds < 31536000:  # Less than a year
        duration_str = f"{overlapping_window_seconds // 2592000} M"  # Months
    else:
        duration_str = f"{overlapping_window_seconds // 31536000} Y"  # Years

    logger.debug(
        f"Calculated incoming window duration: {duration_str} for timeframe: {timeframe}"
    )
    return duration_str


def get_timedelta_duration(
    start: datetime, end: Optional[datetime], tf: str
) -> str:
    """
    Calculate the overlapping window duration for fetching historical data.

    The duration is calculated based on the time delta between `start` and `end`.
    the duration have rounds up to the nearest integer or padding in case of seconds

    Args:
        start (datetime): The start datetime (timezone-aware).
        end (Optional[datetime]): The end datetime (timezone-aware). If None, the current time is used.
        tf (str): The timeframe string (e.g., "1S", "1D").

    Returns:
        str: The duration string to be used for the reqHistoricalDataAsync method.
        This string represents the amount of historical data to fetch.

        Duration formats:
            - "S" for seconds (e.g., "60 S" means 60 seconds).
            - "D" for days (e.g., "2 D" means 2 days).
            - "W" for weeks (e.g., "3 W" means 3 weeks).
            - "M" for months (e.g., "6 M" means 6 months).
            - "Y" for years (e.g., "1 Y" means 1 year).

        The returned duration string will reflect the time difference, such as:
            - "5 D" for a 5-day difference.
            - "1 W" for a 1-week difference.
            - "6 M" for a 6-month difference.
            - "1 Y" for a 1-year difference.
            - If the time delta doesn't exactly fit into a larger unit, seconds will be returned (e.g., "86461 S").
    """

    # Ensure that both start and end are timezone-aware
    assert (
        start.tzinfo is not None and start.tzinfo.utcoffset(start) is not None
    ), "Start time must be timezone-aware"

    if end:
        assert (
            end.tzinfo is not None and end.tzinfo.utcoffset(end) is not None
        ), "End time must be timezone-aware"
    else:
        # If end is None, use the current time with the same timezone as start
        end = datetime.now(start.tzinfo)

    # Calculate the time difference in total seconds
    delta_seconds = int(end.timestamp()) - int(start.timestamp())

    # Define thresholds for conversion units
    seconds_in_hour = 3600

    seconds_in_day = 86400
    seconds_in_week = 604800
    seconds_in_month = 2592000
    seconds_in_year = 31536000

    padding_seconds = 14
    if delta_seconds <= seconds_in_hour:
        delta_seconds += padding_seconds

    if delta_seconds < seconds_in_day:
        duration_str = f"{delta_seconds} S"
    elif delta_seconds < seconds_in_week:
        days = math.ceil(delta_seconds / seconds_in_day)
        duration_str = f"{days} D"
    elif delta_seconds < seconds_in_month:
        weeks = math.ceil(delta_seconds / seconds_in_week)
        duration_str = f"{weeks} W"
    elif delta_seconds < seconds_in_year:
        months = math.ceil(delta_seconds / seconds_in_month)
        duration_str = f"{months} M"
    elif delta_seconds % seconds_in_year == 0:
        years = delta_seconds // seconds_in_year
        duration_str = f"{years} Y"

    logger.debug(
        f"Calculated duration: {duration_str} from start {start} to end {end}"
    )
    return duration_str


def convert_duration_to_seconds(duration: str) -> int:
    """
    Convert a duration string to seconds.

    Args:
        duration (str): The duration string (e.g., '1 D', '1 W', '1 M').

    Returns:
        int: The duration in seconds.
    """
    unit_to_seconds = {
        "S": 1,
        "D": 86400,  # 1 day
        "W": 7 * 86400,  # 1 week
        "M": 30 * 86400,  # 1 month
        "Y": 365 * 86400,  # 1 year
    }

    try:
        value = int(duration[:-1])
        unit = duration[-1]
        return value * unit_to_seconds[unit]
    except (ValueError, KeyError):
        logger.error(f"Invalid duration format: {duration}")
        return 0


def validate_historical_data_request(bar_size: str, duration: str) -> bool:
    """
    Validates that the historical data request is within IB's pacing limits.
        https://interactivebrokers.github.io/tws-api/historical_limitations.html
    Args:
        bar_size (str): The bar size of the historical data request.
        duration (str): The duration of the historical data request.

    Returns:
        bool: True if the request is within limits, False otherwise.
    """

    duration_seconds = convert_duration_to_seconds(duration)

    if bar_size not in bar_size_limits:
        logger.error(f"Unsupported bar size: {bar_size}")
        return False

    if duration_seconds > bar_size_limits[bar_size]:
        logger.error(
            f"Request violates IB historical data limits: "
            f"{duration} exceeds the allowed duration for bar size {bar_size}."
        )
        return False

    logger.debug("Historical data request is within allowed limits.")
    return True
