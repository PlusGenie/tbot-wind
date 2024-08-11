from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from loguru import logger
from ..utils.objects import OrderTV


class Scheduler:
    SUPPORTED_TIMEFRAMES = {
        "1S": 1,
        "5S": 5,
        "10S": 10,
        "15S": 15,
        "30S": 30,
        "1": 60,
        "5": 5 * 60,
        "15": 15 * 60,
        "30": 30 * 60,
        "60": 60 * 60,
        "120": 2 * 60 * 60,
        "240": 4 * 60 * 60,
        "1D": 24 * 60 * 60,
        "1W": 7 * 24 * 60 * 60,
        "1M": 30 * 24 * 60 * 60,
    }

    def __init__(
        self,
        order: OrderTV,
        market_close_time: str = "16:00",
        threshold_seconds: int = 60,  # Set a default threshold in seconds
        period_seconds: int = 60,  # Default periodic schedule interval in seconds
    ):
        """
        Initialize the scheduler with a timeframe, market close time, and timezone.

        Args:
                order (OrderTV): Order Information.
                market_close_time (str): The market close time in HH:MM format.
                threshold_seconds (int): The threshold in seconds to determine the sleep duration.
                period_seconds (int): The interval for the periodic schedule in seconds.
        """
        if order.timeframe not in self.SUPPORTED_TIMEFRAMES:
            logger.error(f"Unsupported timeframe: {order.timeframe}")
            raise ValueError("Unsupported timeframe")

        self.timeframe = order.timeframe
        self.market_close_time = market_close_time
        self.timezone = ZoneInfo(order.timezone)
        self.threshold_seconds = threshold_seconds
        self.period_seconds = period_seconds  # Initialize period_seconds

        logger.debug(
            f"Scheduler initialized with threshold_seconds={self.threshold_seconds}"
        )

    def get_current_time(self) -> datetime:
        """
        Get the current time in the exchange's timezone.
        """
        return datetime.now(self.timezone)

    def calculate_next_execution_time(self) -> datetime:
        """
        Calculate the next execution time based on the current time and timeframe.

        Returns:
                datetime: The next execution time.
        """
        now = self.get_current_time()

        if self.timeframe.endswith("D"):
            return self.calculate_daily_execution_time(now)
        elif self.timeframe.endswith("W"):
            return self.calculate_weekly_execution_time(now)
        elif self.timeframe.endswith("M"):
            return self.calculate_monthly_execution_time(now)
        else:
            return self.calculate_time_based_execution_time(now)

    def calculate_daily_execution_time(
        self, current_time: datetime
    ) -> datetime:
        """
        Calculate the next execution time for daily timeframe.

        Args:
                current_time (datetime): The current time.

        Returns:
                datetime: The next execution time.
        """
        market_close = datetime.combine(
            current_time.date(),
            datetime.strptime(self.market_close_time, "%H:%M").time(),
            self.timezone,
        )

        if current_time >= market_close:
            # If it's past market close today, schedule for tomorrow
            market_close += timedelta(days=1)

        return market_close

    def calculate_weekly_execution_time(
        self, current_time: datetime
    ) -> datetime:
        """
        Calculate the next execution time for weekly timeframe.

        Args:
                current_time (datetime): The current time.

        Returns:
                datetime: The next execution time.
        """
        market_close = datetime.combine(
            current_time.date(),
            datetime.strptime(self.market_close_time, "%H:%M").time(),
            self.timezone,
        )

        # Find the next Monday (start of the week)
        days_until_monday = (7 - current_time.weekday()) % 7
        if days_until_monday == 0 and current_time >= market_close:
            days_until_monday = 7

        next_monday = market_close + timedelta(days=days_until_monday)
        return next_monday

    def calculate_monthly_execution_time(
        self, current_time: datetime
    ) -> datetime:
        """
        Calculate the next execution time for monthly timeframe.

        Args:
                current_time (datetime): The current time.

        Returns:
                datetime: The next execution time.
        """
        market_close = datetime.combine(
            current_time.date(),
            datetime.strptime(self.market_close_time, "%H:%M").time(),
            self.timezone,
        )

        # Calculate the first day of the next month
        if current_time.month == 12:
            next_month_start = market_close.replace(
                year=current_time.year + 1, month=1, day=1
            )
        else:
            next_month_start = market_close.replace(
                month=current_time.month + 1, day=1
            )

        return next_month_start

    def calculate_time_based_execution_time(
        self, current_time: datetime
    ) -> datetime:
        """
        Calculate the next execution time for time-based intervals (seconds, minutes, hours).

        Args:
                current_time (datetime): The current time.

        Returns:
                datetime: The next execution time.
        """
        interval_seconds = self.SUPPORTED_TIMEFRAMES[self.timeframe]
        next_execution = current_time + timedelta(seconds=interval_seconds)
        return next_execution.replace(microsecond=0)

    def calculate_periodic_schedule(self) -> datetime:
        """
        Calculate the next periodic schedule time.

        Returns:
                datetime: The next periodic time.
        """
        current_time = (
            self.get_current_time()
        )  # Get current time in the scheduler's timezone
        # Align the next periodic time with the interval defined by period_seconds
        elapsed_time = current_time - current_time.replace(
            second=0, microsecond=0
        )
        seconds_past_hour = elapsed_time.total_seconds() % self.period_seconds
        next_periodic_time = current_time + timedelta(
            seconds=(self.period_seconds - seconds_past_hour)
        )
        return next_periodic_time

    def determine_sleep_duration(self) -> int:
        """
        Determine the sleep duration based on the bar close time or periodic time.

        Returns:
                tuple[int, datetime]: The sleep duration in seconds and the wake-up time as a datetime object.
        """
        current_time = self.get_current_time()
        next_bar_close_time = self.calculate_next_execution_time()

        # Check if the time until the next bar close is below the threshold
        if (
            next_bar_close_time - current_time
        ).total_seconds() < self.threshold_seconds:
            sleep_duration = (
                next_bar_close_time - current_time
            ).total_seconds()
            wake_up_time = next_bar_close_time
            logger.debug(
                f"Sleeping until next bar close: {wake_up_time.isoformat()}"
            )
        else:
            next_periodic_time = self.calculate_periodic_schedule()
            sleep_duration = (
                next_periodic_time - current_time
            ).total_seconds()
            wake_up_time = next_periodic_time
            logger.debug(
                f"Sleeping for periodic time: {wake_up_time.isoformat()}"
            )

        logger.debug(
            f"Sleeping for {sleep_duration} seconds. "
            f"Will wake up at {wake_up_time.strftime('%Y-%m-%d %H:%M:%S %Z')}."
        )
        return sleep_duration

    def log_wait_time(self, wait_time: int, duration: str, timeframe: str):
        """
        Log the wait time for the next execution based on the interval, duration, and timeframe.

        Args:
                wait_time (int): The wait time in seconds.
                duration (str): The duration string (e.g., "30 D").
                timeframe (str): The timeframe string (e.g., "1D", "4H").
        """
        logger.debug("Logging wait time for the next execution...")
        interval_unit = "seconds"
        interval = wait_time
        if interval >= 60:
            interval = interval / 60
            interval_unit = "minutes"
        if interval >= 3600:
            interval = interval / 3600
            interval_unit = "hours"
        if interval >= 86400:
            interval = interval / 86400
            interval_unit = "days"

        logger.info(
            f"Timeframe: {timeframe}, waiting for {interval} {interval_unit} before next execution"
        )
