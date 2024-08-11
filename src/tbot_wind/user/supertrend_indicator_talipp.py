import pandas as pd
import numpy as np
from talipp.indicators import SuperTrend
from talipp.ohlcv import OHLCV
from loguru import logger
import warnings

from ..core.indicator_base import IndicatorBase
from ..utils.objects import OrderTV
from ..utils.tbot_log import trace_function
from ..utils.tbot_status import TBOTStatus
from ..utils.constants import (
    TBOT_COL,
    ALT_BUY,
    ALT_SELL,
    TBOT_WIND_DUPLICATE_MSG_FILTER,
    TBOT_WIND_ENABLE_TBOT_COL,
    IND0_COL,
)

# Column names
SUPERT_COL = IND0_COL

warnings.simplefilter(action="ignore", category=FutureWarning)


class SupertrendIndicatorTalip(IndicatorBase):
    def __init__(self, period=10, multiplier=3.0, warm_win_loopback=84):
        super().__init__(period)
        self.warm_win_loopback = warm_win_loopback
        self.atr_length = period
        self.multiplier = multiplier
        # Registering the Supertrend indicator with the actual function reference
        self.register_indicator(SUPERT_COL, self.calculate_supertrend)
        self.inc_supert = SuperTrend(atr_period=period, mult=multiplier)

    @trace_function
    def initialize_missing_columns(self, df: pd.DataFrame) -> None:
        super().initialize_missing_columns(df)
        # Initialize Supertrend column with zeros for missing columns
        if SUPERT_COL not in df.columns:
            df[SUPERT_COL] = 0.0

    @trace_function
    def sanitize_existing_data(self, df: pd.DataFrame) -> None:
        # Ensure TBOT_COL values are correct and the dtype is float64
        super().sanitize_existing_data(df)

        # Ensure Supertrend values are correct and the dtype is float64
        df.loc[df[SUPERT_COL].isnull(), SUPERT_COL] = 0.0
        assert df[SUPERT_COL].dtype == np.float64, f"{SUPERT_COL} must be float64"

    @trace_function
    def calculate_initialize(self, df: pd.DataFrame) -> None:
        self.initialize_missing_columns(df)
        self.sanitize_existing_data(df)

    @trace_function
    def calculate_supertrend(self, df: pd.DataFrame, prev_row: pd.Series, update_status_cb=None) -> np.float64:
        """
        Calculate the Supertrend using the talipp SuperTrend indicator.
        """
        curr = df.index[-1]
        min_valid_len = self.atr_length + 1

        ohlcv = OHLCV(*df[["open", "high", "low", "close", "volume"]].iloc[-1])
        self.inc_supert.add(ohlcv)

        # Check if we have enough data for a full calculation
        if len(self.inc_supert) < min_valid_len:
            logger.info(f"Still warming up Supertrend: {len(self.inc_supert)} data points provided so far.")
            return np.nan

        # Check if the Supertrend calculation for this row has already been completed
        if TBOT_WIND_ENABLE_TBOT_COL and TBOTStatus.has_status(df.loc[curr, TBOT_COL], TBOTStatus.INDICATOR1_COMPLETED):
            logger.debug(f"Supertrend calculation already completed for row {curr}.")
            return df.loc[curr, SUPERT_COL]

        try:
            # Get the most recent SuperTrend value
            supertrend_value = self.inc_supert[-1].value
            if pd.isna(supertrend_value):
                logger.info("Supertrend contains NaN values.")
                return np.nan

            # If a status update callback is provided, mark this row as completed
            if update_status_cb:
                logger.info(f"{self.w_idx}/{self.w_len} TBOTStatus.INDICATOR1_COMPLETED")
                update_status_cb(curr, TBOTStatus.INDICATOR1_COMPLETED)

            return round(np.float64(supertrend_value), 2)

        except Exception as e:
            logger.error(f"Error generating Supertrend: {e}")
            return np.nan

    def calculate_alerts(
        self,
        df_in: pd.DataFrame,
        prev_row: pd.Series,
        prev_prev_row: pd.Series,
    ) -> pd.Series:
        """Generate buy and sell signals based on the Supertrend indicator."""
        window = self.atr_length
        # the minimum length required to pass continuously valid data
        min_valid_len = window + 1

        curr = df_in.index[-1]
        if (
            TBOT_WIND_DUPLICATE_MSG_FILTER
            and TBOT_WIND_ENABLE_TBOT_COL
            and TBOTStatus.has_status(df_in.loc[curr, TBOT_COL], TBOTStatus.ALERT_COMPLETED)
        ):
            logger.debug(f"{self.w_idx}/ Calculation already done")
            return df_in.loc[curr]

        if len(df_in) < min_valid_len:
            logger.debug(f"Skipping alert calculation len, {len(df_in)}: {self.w_idx}/{self.w_len}:.")
            return df_in.iloc[-1]

        logger.debug(f"Creating alerts view: idx={self.w_idx}, len={len(df_in)}, length={window}")

        df = df_in.iloc[-min_valid_len:].copy()
        df.reset_index(drop=True, inplace=True)
        last = df.index[-1]

        # Ensure the last two Supertrend values are not NaN
        if df[SUPERT_COL].iloc[-2:].isna().any():
            logger.debug("NaN detected in the last two Supertrend values.")
            return df.loc[last]

        # Purge old values after the indicator stabilizes
        stable_length = 200  # Define how much data you want to retain for stable calculations
        if len(self.inc_supert.input_values) > stable_length:
            purge_size = len(self.inc_supert.input_values) - stable_length
            self.inc_supert.purge_oldest(purge_size)

        try:
            # Compare the last two Supertrend values to detect a trend change
            previous_supertrend = df[SUPERT_COL].iloc[-2]
            current_supertrend = df[SUPERT_COL].iloc[-1]

            # Generate buy signal if the Supertrend changes from above to below the price (uptrend start)
            df.loc[last, ALT_BUY] = current_supertrend > previous_supertrend

            # Generate sell signal if the Supertrend changes from below to above the price (downtrend start)
            df.loc[last, ALT_SELL] = current_supertrend < previous_supertrend
            df.loc[last, TBOT_COL] = TBOTStatus.add_status(df.loc[last, TBOT_COL], TBOTStatus.ALERT_COMPLETED)
        except Exception as e:
            logger.error(f"Error generating Supertrend signals: {e}")
            return df.loc[last]

        return df.loc[last]

    @trace_function
    async def post_alert_messages(self, order: OrderTV, df_in: pd.DataFrame, update_status_cb=None):
        """
        Generate Alert Messages according to TBOT on TradingBoat's Rules
        and then send it to Redis
        """
        curr = df_in.index[-1]
        if TBOT_WIND_ENABLE_TBOT_COL and TBOTStatus.has_status(df_in.loc[curr, TBOT_COL], TBOTStatus.ALERT_GENERATED):
            logger.info(f"{self.w_idx}/ alert already sent")
            return

        window = self.atr_length
        min_valid_len = window + 1
        if len(df_in) < min_valid_len:
            logger.debug(f"Skipping sending alert len, {len(df_in)}: {self.w_idx}/{self.w_len}:.")
            return

        row = df_in.iloc[-1]
        buy_signal = row[ALT_BUY]
        sell_signal = row[ALT_SELL]

        if not buy_signal and not sell_signal:
            logger.trace("No alert messages detected")
            return

        # Ensure that ALT_BUY and ALT_SELL are of boolean type
        assert isinstance(buy_signal, np.bool_), f"Expected dtype 'np.bool_', but got {buy_signal}"
        assert isinstance(sell_signal, np.bool_), f"Expected dtype 'np.bool_', but got {sell_signal}"

        timestamp = int(pd.Timestamp(row["date"]).timestamp() * 1000)
        common_ref = "Supertrend"
        common_params = {
            "ticker": order.symbol,
            "timeframe": order.timeframe,
            "contract": order.contract,
            "currency": order.currency,
            "qty": order.qty,
            "timestamp": timestamp,
            "orderRef": common_ref,
        }

        timestamp = int(pd.Timestamp(row["date"]).timestamp() * 1000)
        readable = pd.Timestamp(row["date"]).strftime("%Y-%m-%d %H:%M:%S")

        if buy_signal:
            msg = self.make_webhook_json(
                direction="strategy.entrylong",
                entry_limit=order.entryLimit,
                entry_stop=order.entryStop,
                **common_params,
            )
            result = await super().send_alert_message(msg)
            if result:
                logger.success(
                    f"Generated buy signal at {readable} for {order.symbol} (timestamp: {timestamp}): {msg}"
                )
        elif sell_signal:
            msg = self.make_webhook_json(
                direction="strategy.close",
                **common_params,
            )
            result = await super().send_alert_message(msg)
            if result:
                logger.success(
                    f"Generated sell signal at {readable} for {order.symbol} (timestamp: {timestamp}): {msg}"
                )

        # Pass the signal data to OpenAI for analysis
        prompt_data = pd.DataFrame(
            {
                "SuperTrend": [row[SUPERT_COL]],
            }
        )
        ai_decision = await self.ai_engine.analyze_signal(prompt_data)
        ai_msg = ai_decision.get("interpretation", "No additional interpretation available.")
        logger.success(
            f"Generated AI message at {readable} for {order.symbol} (timestamp: {timestamp}): {ai_msg}"
        )

        if update_status_cb:
            logger.info(f"{self.w_idx}/{self.w_len} TBOTStatus.ALERT_GENERATED")
            update_status_cb(curr, TBOTStatus.ALERT_GENERATED)
