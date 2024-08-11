from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Dict
import pandas as pd
import numpy as np
from loguru import logger


from .openai_decision import OpenAIDecisionEngine

from ..dispatch.redis_message_guard import RedisMessageGuard
from ..dispatch.http_message_sender import HTTPMessageSender
from ..dispatch.webhook_message_generator import WebhookMessageGenerator
from ..dispatch.message_deduplicator import MessageDeduplicator
from ..dispatch.redis_message_publisher import RedisMessagePublisher

from ..utils.objects import OrderTV
from ..utils.tbot_log import trace_function
from ..utils.tbot_status import TBOTStatus
from ..utils.constants import (
    TBOT_WIND_SEND_MSG_TO_REDIS,
    TBOT_COL,
    ALT_BUY,
    ALT_SELL,
)


class IndicatorBase(ABC):
    """
    IndicatorBase is an abstract base class that provides a framework for creating and managing financial indicators.
    It includes methods for registering indicators, initializing and sanitizing data, sending alert messages, and processing
    data incrementally in an event loop.
    Attributes:
        use_redis (bool): A flag indicating whether to use Redis for message sending.
        msg_sender (Union[RedisMessageGuard, HTTPMessageSender]): The message sender object, which can be either Redis or HTTP-based.
        indicators (OrderedDict): An ordered dictionary to store registered indicators.
        buffer (pd.DataFrame): A buffer to accumulate data until enough points are available.
        event_loop_min_len (int): The minimum length of the event loop.
        w_idx (int): An index for debugging.
        w_len (int): A length for debugging.
    Methods:
        register_indicator(event: str, callback): Registers an indicator with a specific event and callback function.
        calculate_initialize(df: pd.DataFrame): Abstract method to initialize necessary columns in the DataFrame.
        initialize_missing_columns(df: pd.DataFrame): Initializes missing columns in the DataFrame with default values.
        sanitize_existing_data(df: pd.DataFrame): Sanitizes existing data in the DataFrame to ensure correct data types
        and values.
        send_alert_message(msg): Sends an alert message, either synchronously via Redis or asynchronously via HTTP.
        make_webhook_json(timestamp, direction, ticker, timeframe, contract, currency, qty, entry_limit, entry_stop,
        exit_limit exit_stop, lastTradeDateOrContractMonth, exchange, multiplier, orderRef): Creates a JSON object for
        webhook messages.
        post_alert_messages(order: OrderTV, row: pd.Series, update_status_callback=None): Abstract method to post
        alert messages.
        calculate_alerts(df_in: pd.DataFrame, prev_row: pd.Series, prev_prev_row: pd.Series):
        Abstract method to calculate alerts based on the DataFrame.
        event_loop(order: OrderTV, df: pd.DataFrame): Processes incoming data incrementally and triggers alerts
        immediately.
    """

    def __init__(self, event_loop_min_len=14):
        self.use_redis = True if TBOT_WIND_SEND_MSG_TO_REDIS else False
        redis_cl = RedisMessagePublisher() if self.use_redis else None
        self.tbot = WebhookMessageGenerator()
        deduplicator = MessageDeduplicator(redis_cl)
        self.msg_sender = (
            RedisMessageGuard(deduplicator, redis_cl)
            if self.use_redis
            else HTTPMessageSender(deduplicator)
        )
        self.indicators = OrderedDict([])
        # Buffer to accumulate data until enough points are available
        self.buffer = pd.DataFrame()
        self.event_loop_min_len = event_loop_min_len

        # Initialize OpenAI Decision Engine
        self.ai_engine = OpenAIDecisionEngine()

        # property for accumulating statistics
        self.w_idx = 0  # For debugging index
        self.w_len = 0

    def register_indicator(self, event: str, callback):
        if event not in self.indicators:
            self.indicators[event] = callback

    @abstractmethod
    def calculate_initialize(self, df: pd.DataFrame) -> None:
        pass

    def initialize_missing_columns(self, df: pd.DataFrame) -> None:
        """
        Initialize TBOT_COL, ALT_BUY, and ALT_SELL columns if missing.
        Additional columns should be handled in the child class.
        """
        columns_with_defaults = {TBOT_COL: 0.0}
        # Initialize the necessary columns
        for column, default in columns_with_defaults.items():
            if column not in df.columns:
                df[column] = default

        boolean_columns = [ALT_BUY, ALT_SELL]
        for column in boolean_columns:
            if column not in df.columns:
                df[column] = False

    def sanitize_existing_data(self, df: pd.DataFrame) -> None:
        """
        Sanitize TBOT_COL, ALT_BUY, and ALT_SELL columns.
        Additional columns should be handled in the child class.
        """
        # Ensure TBOT_COL values are correct and the dtype is float64
        df.loc[df[TBOT_COL].isnull(), TBOT_COL] = 0.0

        # Ensure signal columns are boolean and don't contain NaN values
        signal_columns = [ALT_BUY, ALT_SELL]
        for column in signal_columns:
            df.loc[df[column].isnull(), column] = False
            df[column] = df[column].astype(bool)

        # Final type checks
        assert df[TBOT_COL].dtype == np.float64, f"{TBOT_COL} must be float64"
        assert df[ALT_SELL].dtype == np.bool_, f"{ALT_SELL} must be boolean"

    @trace_function
    async def send_alert_message(self, msg):
        try:
            if self.use_redis:
                # For Redis, synchronous send
                self.msg_sender.send_message(msg)
            else:
                # For HTTP, use async send
                await self.msg_sender.send_message(msg)
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

    def make_webhook_json(
        self,
        timestamp,
        direction,
        ticker,
        timeframe,
        contract,
        currency,
        qty=-1e10,
        entry_limit=0.0,
        entry_stop=0.0,
        exit_limit=0.0,
        exit_stop=0.0,
        lastTradeDateOrContractMonth="",
        exchange="",
        multiplier="",
        orderRef="",
    ) -> Dict:
        return self.tbot.make_webhook_json(
            timestamp=timestamp,
            direction=direction,
            ticker=ticker,
            timeframe=timeframe,
            contract=contract,
            currency=currency,
            qty=qty,
            entry_limit=entry_limit,
            entry_stop=entry_stop,
            exit_limit=exit_limit,
            exit_stop=exit_stop,
            lastTradeDateOrContractMonth=lastTradeDateOrContractMonth,
            exchange=exchange,
            multiplier=multiplier,
            orderRef=orderRef,
        )

    @abstractmethod
    async def post_alert_messages(
        self, order: OrderTV, row: pd.Series, update_status_callback=None
    ):
        pass

    @abstractmethod
    def calculate_alerts(
        self,
        df_in: pd.DataFrame,
        prev_row: pd.Series,
        prev_prev_row: pd.Series,
    ) -> pd.Series:
        pass

    @trace_function
    async def event_loop(
        self, order: OrderTV, df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Event loop to process incoming data incrementally and trigger alerts immediately.

        Design Principle:
        - The function processes the incoming data row by row, ensuring that alerts are generated and sent out
        as soon as they are triggered.
        - To minimize redundant calculations, the TBOT_COL is used to skip sub-indicators that have already
        been processed for a row.
        - A single loop is used for efficient data slicing and row-by-row processing, enabling immediate alert
        generation without waiting for batch processing.
        - The focus is on simplicity and readability while still maintaining efficiency.

        Key Features:
        - **Immediate Alert Generation**: As soon as an alert condition is met, the alert is generated and sent out
        immediately, avoiding unnecessary delays.
        - **Single Loop with Efficient Data Slicing**: Each row of the DataFrame is processed in a single loop.
        This ensures that calculations and alerts are handled in real-time.
        - **Avoid Unnecessary Work**: If an alert or calculation for a particular row has already been completed
        (as indicated by TBOT_COL), the row is skipped to prevent redundant processing.
        - **Buffering**: If there is insufficient data to calculate the required indicators, the new data is
        accumulated in a buffer until enough data points are available for processing.

        Args:
        - order (OrderTV): The order object containing relevant trade details.
        - df (pd.DataFrame): The incoming DataFrame containing price data and other relevant fields.

        Returns:
        - pd.DataFrame: The updated DataFrame after processing and generating any necessary alerts.
        """
        try:
            df = pd.concat([self.buffer, df], ignore_index=True)
            if len(df) < self.event_loop_min_len:
                logger.info(
                    f"{self.w_idx}/{self.w_len}, loop buffer : {len(df)}, Required: {self.event_loop_min_len}"
                )
                self.buffer = df
                return df

            # At this point, df has enough data, so reset the buffer
            self.buffer = pd.DataFrame()

            self.w_len = len(df)
            # Initialize necessary columns if not already present
            self.calculate_initialize(df)

            # Keep a copy of the original TBOT column
            original_tbot = df["TBOT"].copy()

            def update_status(idx, status):
                df.at[idx, TBOT_COL] = TBOTStatus.add_status(
                    df.at[idx, TBOT_COL], status
                )

            for i in range(1, len(df)):
                prev_row = df.iloc[i - 1]
                prev_prev_row = (
                    df.iloc[i - 2] if i > 1 else None
                )  # Check to ensure we don't go out of bounds
                i % 10 == 0 and logger.debug(f"event_loop: {i+1}/{len(df)}")
                self.w_idx = i
                try:
                    # Process each indicator in the order defined in the child class
                    for (
                        column_name,
                        indicator_method,
                    ) in self.indicators.items():
                        if column_name in df.columns:
                            logger.debug(
                                f"Processing {indicator_method.__name__} for row {i+1}: {df.at[i, TBOT_COL]}"
                            )

                            try:
                                df.iloc[i, df.columns.get_loc(column_name)] = (
                                    indicator_method(
                                        df.iloc[: i + 1],
                                        prev_row,
                                        update_status,
                                    )
                                )

                            except Exception as e:
                                logger.error(
                                    f"Error in indicator {indicator_method.__name__} for row {i+1}: {e}"
                                )
                                continue  # Continue with other indicators even if one fails

                    # Calculate alerts
                    df.iloc[i] = self.calculate_alerts(
                        df.iloc[: i + 1], prev_row, prev_prev_row
                    )

                    # Post alert messages
                    await self.post_alert_messages(
                        order, df.iloc[: i + 1], update_status
                    )

                except Exception as e:
                    logger.error(f"Error processing row {i} of DataFrame: {e}")
                    continue

            # Find the rows where TBOT status has changed
            changed_rows = df[original_tbot != df["TBOT"]]

            if changed_rows.empty:
                logger.info(
                    "No changes detected in TBOT status. Returning an empty DataFrame."
                )
                return pd.DataFrame()

            logger.info(
                f"Returning {len(df)} -> {len(changed_rows)} rows where TBOT status changed."
            )

        except Exception as e:
            logger.error(f"Critical error in event_loop: {e}")
            raise

        return changed_rows
