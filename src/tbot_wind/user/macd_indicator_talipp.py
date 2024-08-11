import pandas as pd
import numpy as np
from talipp.indicators import MACD
from loguru import logger
import warnings

from ..core.indicator_base import IndicatorBase
from ..utils.objects import OrderTV
from ..utils.ta_utils import TAUtils
from ..utils.tbot_log import trace_function
from ..utils.tbot_status import TBOTStatus
from ..utils.constants import (
    TBOT_COL,
    ALT_BUY,
    ALT_SELL,
    TBOT_WIND_DUPLICATE_MSG_FILTER,
    TBOT_WIND_ENABLE_TBOT_COL,
    IND0_COL,
    IND1_COL,
    IND2_COL,
)

# Column names
MACD_COL = IND0_COL
MACD_SIGNAL = IND1_COL
MACD_HISTOGRAM = IND2_COL
warnings.simplefilter(action="ignore", category=FutureWarning)


class MACDIndicatorTalipp(IndicatorBase):
    def __init__(self, fast_period=12, slow_period=26, signal_period=9, warm_win_loopback=130):
        super().__init__(fast_period)
        self.warm_win_loopback = warm_win_loopback
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.inc_macd = MACD(self.slow_period, self.fast_period, self.signal_period)

        self.w_idx = 0  # For debugging index
        self.w_len = 0

    @trace_function
    def initialize_missing_columns(self, df: pd.DataFrame) -> None:
        """Initialize missing MACD columns with zeros."""
        super().initialize_missing_columns(df)
        if MACD_COL not in df.columns:
            df[MACD_COL] = 0.0
        if MACD_SIGNAL not in df.columns:
            df[MACD_SIGNAL] = 0.0
        if MACD_HISTOGRAM not in df.columns:
            df[MACD_HISTOGRAM] = 0.0

    @trace_function
    def sanitize_existing_data(self, df: pd.DataFrame) -> None:
        """Ensure TBOT_COL and MACD columns are correct and clean."""
        super().sanitize_existing_data(df)
        df.loc[df[MACD_COL].isnull(), MACD_COL] = 0.0
        df.loc[df[MACD_SIGNAL].isnull(), MACD_SIGNAL] = 0.0
        df.loc[df[MACD_HISTOGRAM].isnull(), MACD_HISTOGRAM] = 0.0

        assert df[MACD_COL].dtype == np.float64, f"{MACD_COL} must be float64"
        assert df[MACD_SIGNAL].dtype == np.float64, f"{MACD_SIGNAL} must be float64"
        assert df[MACD_HISTOGRAM].dtype == np.float64, f"{MACD_HISTOGRAM} must be float64"

    @trace_function
    def calculate_initialize(self, df: pd.DataFrame) -> None:
        """Initialize and sanitize columns."""
        self.initialize_missing_columns(df)
        self.sanitize_existing_data(df)

    @trace_function
    def calculate_alerts(
        self,
        df_in: pd.DataFrame,
        prev_row: pd.Series,
        prev_prev_row: pd.Series,
    ) -> pd.Series:
        """Generate buy and sell signals and update the DataFrame in place."""

        min_valid_len = self.slow_period + self.fast_period
        curr = df_in.index[-1]
        incremental_valid_len = self.signal_period

        if len(df_in) < incremental_valid_len:
            # Not enough data yet, log the status and return the last row without calculation.
            logger.info(f"Skipping MACD calculation for {len(df_in)}: Not enough data for incremental calculations.")
            return df_in.loc[curr]
        elif incremental_valid_len <= len(df_in) < min_valid_len:
            # Feed the latest price into the MACD indicator incrementally during the warm-up phase.
            latest_price = df_in["close"].iloc[-1]
            self.inc_macd.add_input_value(latest_price)
            return df_in.loc[curr]

        if (
            TBOT_WIND_DUPLICATE_MSG_FILTER
            and TBOT_WIND_ENABLE_TBOT_COL
            and TBOTStatus.has_status(df_in.loc[curr, TBOT_COL], TBOTStatus.ALERT_COMPLETED)
        ):
            logger.debug(f"{self.w_idx}/ Calculation already done")
            return df_in.loc[curr]

        # Step 3: Full MACD calculation with sufficient data
        df = df_in.iloc[-min_valid_len:].copy()
        df.reset_index(drop=True, inplace=True)
        last = df.index[-1]

        logger.info(f"Processing row {self.w_idx}: Calculating MACD and Alerts.")

        try:
            # Feed only the latest close price into the MACD indicator
            latest_price = df["close"].iloc[-1]
            self.inc_macd.add_input_value(latest_price)

            # Log internal buffer status of macd_indicator
            logger.info(f"MACD indicator buffer size: {len(self.inc_macd)}")
            logger.debug(
                f"MACD indicator values (last 5): {self.inc_macd[-5:] if len(self.inc_macd) >= 5 else self.inc_macd[:]}"
            )

            # Get MACD values
            macd_values = self.inc_macd[-1]
            macd_value = macd_values.macd if macd_values.macd is not None else 0.0
            signal_value = macd_values.signal if macd_values.signal is not None else 0.0
            histogram_value = macd_values.histogram if macd_values.histogram is not None else 0.0

            # Invert the MACD and Signal values
            macd_value = -macd_value
            signal_value = -signal_value
            histogram_value = -histogram_value

            # Assign values to DataFrame
            df.loc[last, MACD_COL] = round(macd_value, 2)
            df.loc[last, MACD_SIGNAL] = round(signal_value, 2)
            df.loc[last, MACD_HISTOGRAM] = round(histogram_value, 4)

            crossover = TAUtils.crossover(df[MACD_COL], df[MACD_SIGNAL])
            crossunder = TAUtils.crossunder(df[MACD_COL], df[MACD_SIGNAL])

            df.loc[last, ALT_BUY] = crossover.iloc[-1]
            df.loc[last, ALT_SELL] = crossunder.iloc[-1]

            df.loc[last, TBOT_COL] = TBOTStatus.add_status(df.loc[last, TBOT_COL], TBOTStatus.ALERT_COMPLETED)
        except Exception as e:
            logger.error(f"Error calculating MACD for row {self.w_idx}: {e}")
            return df.loc[last]

        return df.loc[last]

    @trace_function
    async def post_alert_messages(self, order: OrderTV, df_in: pd.DataFrame, update_status_cb=None):
        """
        This fucntion will create the order and then send it to TBOT by Redis Pub/Sub
        """
        curr = df_in.index[-1]
        if TBOT_WIND_ENABLE_TBOT_COL:
            if TBOTStatus.has_status(df_in.loc[curr, TBOT_COL], TBOTStatus.ALERT_GENERATED):
                logger.info(f"{self.w_idx}/ alert already sent")
                return

        window = self.slow_period
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

        timestamp = int(pd.Timestamp(row["date"]).timestamp() * 1000)
        common_ref = "macd"
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

        # Pass the signal data to OpenAI for analysis
        prompt_data = pd.DataFrame(
            {
                "MACD": [row[MACD_COL]],
                "MACD_SIGNAL": [MACD_SIGNAL],
                "MACD_HISTOGRAM": [row[MACD_HISTOGRAM]],
            }
        )

        if buy_signal:
            msg = self.make_webhook_json(
                direction="strategy.entrylong",
                entry_limit=order.entryLimit,
                entry_stop=order.entryStop,
                **common_params,
            )
            result = await super().send_alert_message(msg)
            if result:
                ai_decision = await self.ai_engine.analyze_signal(prompt_data)
                ai_msg = ai_decision.get("interpretation", "No additional interpretation available.")
                logger.success(
                    f"Generated buy signal at {readable} for {order.symbol} (timestamp: {timestamp}): {msg} {ai_msg}"
                )

        if sell_signal:
            msg = self.make_webhook_json(
                direction="strategy.close",
                **common_params,
            )
            result = await super().send_alert_message(msg)
            if result:
                ai_decision = await self.ai_engine.analyze_signal(prompt_data)
                ai_msg = ai_decision.get("interpretation", "No additional interpretation available.")
                logger.success(
                    f"Generated sell signal at {readable} for {order.symbol} (timestamp: {timestamp}): {msg} {ai_msg}"
                )

        if update_status_cb:
            logger.info(f"{self.w_idx}/{self.w_len} TBOTStatus.ALERT_GENERATED")
            update_status_cb(curr, TBOTStatus.ALERT_GENERATED)
