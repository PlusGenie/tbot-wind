import pandas as pd
import numpy as np
from loguru import logger
import warnings

from talipp.indicators import RSI, ATR
from talipp.ohlcv import OHLCV

from ..core.indicator_base import IndicatorBase
from ..utils.ta_utils import TAUtils
from ..utils.objects import OrderTV
from ..utils.tbot_log import trace_function, pretty_print_df, pretty_msg_df
from ..utils.tbot_status import TBOTStatus
from ..utils.constants import (
    TBOT_COL,
    ALT_BUY,
    ALT_SELL,
    TBOT_WIND_ENABLE_TBOT_COL,
    TBOT_WIND_DUPLICATE_MSG_FILTER,
    IND0_COL,
    IND1_COL,
    IND2_COL,
    IND3_BOOL_COL,
    IND4_BOOL_COL,
    IND5_COL,
    IND6_COL,
    IND7_COL,
    IND8_COL,
    IND9_COL,
)

APH_COL = IND0_COL
ATR_COL = IND1_COL
RSI_COL = IND2_COL


warnings.simplefilter(action="ignore", category=FutureWarning)


class AlphaTrendIndicator(IndicatorBase):
    """
    Summary of Algorithm Components
    - **Indicators Used:**
        - **ATR (Average True Range):** Measures market volatility using Welles Wilder's smoothing method.
        - **RSI (Relative Strength Index):** Assesses momentum based on price data.
    - **Trend Determination:**
        - **Bullish Trend:** Identified when RSI is above 50, combined with ATR-adjusted thresholds.
        - **Bearish Trend:** Identified when RSI is below 50, combined with ATR-adjusted thresholds.
    - **Signal Logic:**
        - **Buy Signals:** Generated on upward crossovers of AlphaTrend over its lagged version,
        ensuring momentum and volatility conditions are met.
        - **Sell Signals:** Generated on downward crossovers of AlphaTrend under its lagged version,
        ensuring momentum and volatility conditions are met.

    Key Takeaways
    - **ATR Calculation Method:** Uses EMA-like smoothing, not SMA.
    - **RSI Usage:** RSI values help determine momentum for trend direction, with a 50 threshold.
    - **Dynamic Thresholds:** Adapt to changing market volatility, improving responsiveness in different conditions.
    - **Signal Confirmation:** Lagged trend lines and bar comparisons reduce false signals by confirming trends beyond
    transient crossovers.

    Note: PineScript RSI uses the rma() function, while Talipp uses SMA.
    """

    def __init__(self, coeff=1.0, period=14, warm_win_loopback=365):
        super().__init__(period)
        self.warm_win_loopback = warm_win_loopback
        self.coeff = coeff
        self.window_base = period

        self.register_indicator(ATR_COL, self.calculate_atr)
        self.register_indicator(RSI_COL, self.calculate_rsi)
        self.register_indicator(APH_COL, self.calculate_alpha)

        self.inc_atr = ATR(period=period)
        self.inc_rsi = RSI(period=period)

    @trace_function
    def initialize_missing_columns(self, df: pd.DataFrame) -> None:
        super().initialize_missing_columns(df)

        # Initialize AlphaTrend with zeros for missing column
        if APH_COL not in df.columns:
            df[APH_COL] = 0.0

        # Initialize other necessary columns with NaN for missing columns
        necessary_columns_nan = [ATR_COL, RSI_COL]
        for column in necessary_columns_nan:
            if column not in df.columns:
                df[column] = np.nan

        extra_columns_nan = [
            IND3_BOOL_COL,
            IND3_BOOL_COL,
            IND4_BOOL_COL,
            IND5_COL,
            IND6_COL,
            IND7_COL,
            IND8_COL,
            IND9_COL,
        ]
        for column in extra_columns_nan:
            if column not in df.columns:
                df[column] = np.nan

    @trace_function
    def sanitize_existing_data(self, df: pd.DataFrame) -> None:
        super().sanitize_existing_data(df)
        # Ensure AlphaTrend values are correct and the dtype is float64
        df.loc[df[APH_COL].isnull(), APH_COL] = 0.0
        assert df[APH_COL].dtype == np.float64, f"{APH_COL} must be float64"

    @trace_function
    def calculate_initialize(self, df: pd.DataFrame) -> None:
        self.initialize_missing_columns(df)
        self.sanitize_existing_data(df)

    @trace_function
    def calculate_atr(
        self, df: pd.DataFrame, prev_row: pd.Series, update_status_cb=None
    ) -> np.float64:
        """
        Calculate the ATR using the last `window + 1` rows of the DataFrame.
        This function avoids creating unnecessary copies by operating directly on the relevant data.

        Parameters:
        - df: The DataFrame containing at least `window + 1` rows of data.
        - window: The look-back period for ATR calculation (default is 12).

        Returns:
        - The ATR value for the most recent row, or NaN if there are not enough data points.
        """

        # the minimum length required to pass continuously valid data
        min_valid_len = self.window_base + 1
        curr = df.index[-1]

        ohlcv = OHLCV(*df[["open", "high", "low", "close", "volume"]].iloc[-1])
        self.inc_atr.add(ohlcv)

        if TBOT_WIND_ENABLE_TBOT_COL and TBOTStatus.has_status(
            df.loc[curr, TBOT_COL], TBOTStatus.INDICATOR1_COMPLETED
        ):
            logger.debug(
                f"ATR already completed for row {curr} at index {self.w_idx}/{self.w_len}."
            )
            return df.loc[curr, ATR_COL]

        if len(self.inc_atr) < min_valid_len:
            logger.debug(
                f"{self.w_idx}/{self.w_len}, Available: {len(df)}, Required: {min_valid_len}"
            )
            return np.nan

        logger.debug(
            f"Creating the view: idx={self.w_idx}, len={len(df)}, window={min_valid_len}"
        )
        try:
            atr_value = self.inc_atr[-1]
            if pd.isna(atr_value):
                logger.debug(
                    f"Data used for ATR calculation:\n{df[['high', 'low', 'close']]}"
                )
                return np.nan

        except Exception as e:
            logger.error(f"Error generating ATR: {e}")
            return np.nan

        if update_status_cb:
            update_status_cb(curr, TBOTStatus.INDICATOR1_COMPLETED)

        return round(atr_value, 2)

    def calculate_rsi(
        self, df: pd.DataFrame, prev_row: pd.Series, update_status_cb=None
    ) -> np.float64:
        """
        Calculate the RSI using the last `window` rows of the DataFrame.
        This function avoids creating unnecessary copies by operating directly on the relevant data.

        Parameters:
        - df: The DataFrame containing at least `window` rows of data.
        - window: The look-back period for RSI calculation (default is 14).

        Returns:
        - The RSI value for the most recent row, or NaN if there are not enough data points.
        """
        min_valid_len = self.window_base + 1
        self.inc_rsi.add(df["close"].iloc[-1])

        curr = df.index[-1]
        if TBOT_WIND_ENABLE_TBOT_COL and TBOTStatus.has_status(
            df.loc[curr, TBOT_COL], TBOTStatus.INDICATOR3_COMPLETED
        ):
            logger.debug(
                f"RSI already completed at {curr}:index {self.w_idx}/{self.w_len}."
            )
            return df.loc[curr, RSI_COL]

        if len(self.inc_rsi) < min_valid_len:
            logger.debug(
                f"{self.w_idx}/{self.w_len}, Available: {len(df)}, Required: {min_valid_len}"
            )
            return np.nan

        logger.debug(
            f"Creating the view: idx={self.w_idx}, len={len(df)}, window={self.window_base}"
        )
        try:
            rsi_value = self.inc_rsi[-1]
            if pd.isna(rsi_value):
                return np.nan
        except Exception as e:
            logger.error(f"Error generating RSI: {e}")
            return np.nan

        if update_status_cb:
            update_status_cb(curr, TBOTStatus.INDICATOR3_COMPLETED)

        return round(rsi_value, 2)

    @trace_function
    def calculate_alpha(
        self, df: pd.DataFrame, prev: pd.Series, update_status_cb=None
    ) -> float:
        curr = df.index[-1]
        min_valid_len = self.window_base + 1

        if TBOT_WIND_ENABLE_TBOT_COL and TBOTStatus.has_status(
            df.loc[curr, TBOT_COL], TBOTStatus.INDICATOR4_COMPLETED
        ):
            logger.debug(f"{self.w_idx}/ Alpha calculation already done")
            return df.loc[curr, APH_COL]

        if len(df) < min_valid_len:
            logger.debug(
                f"{self.w_idx}/{self.w_len}, Available: {len(df)}, Required: {min_valid_len}"
            )
            return 0.0

        logger.debug(
            f"Creating the view: idx={self.w_idx}, len={len(df)}, window={min_valid_len}"
        )

        if pd.isna(df[RSI_COL].iloc[-1]):
            logger.warning(f"{self.w_idx}/ The last RSI  values are NaN")
            return 0.0

        upT = (
            df["low"].iloc[-1] - df[ATR_COL].iloc[-1] * self.coeff
            if not pd.isna(df[ATR_COL].iloc[-1])
            else prev[APH_COL]
        )
        downT = (
            df["high"].iloc[-1] + df[ATR_COL].iloc[-1] * self.coeff
            if not pd.isna(df[ATR_COL].iloc[-1])
            else prev[APH_COL]
        )

        try:
            isRsiAbove50 = df[RSI_COL].iloc[-1] >= 50
            if isRsiAbove50:
                alpha = max(prev[APH_COL], upT)
            else:
                alpha = min(prev[APH_COL], downT)

            if update_status_cb:
                update_status_cb(curr, TBOTStatus.INDICATOR4_COMPLETED)

            return round(alpha, 2)

        except Exception as e:
            logger.error(f"Error calculating AlphaTrend: {e}")
            return 0.0

    @trace_function
    def calculate_alerts(
        self,
        df_in: pd.DataFrame,
        prev_row: pd.Series,
        prev_prev_row: pd.Series,
    ) -> pd.Series:
        """Generate buy and sell signals and update the DataFrame in place."""

        window = self.window_base
        # the minimum length required to pass continuously valid data
        # This is to give enough size to barsince to simulate PineScript
        min_valid_len = 64

        curr = df_in.index[-1]
        if (
            TBOT_WIND_DUPLICATE_MSG_FILTER
            and TBOT_WIND_ENABLE_TBOT_COL
            and TBOTStatus.has_status(
                df_in.loc[curr, TBOT_COL], TBOTStatus.ALERT_COMPLETED
            )
        ):
            logger.debug(f"{self.w_idx}/ Calculation already done")
            return df_in.loc[curr]

        if len(df_in) < min_valid_len:
            logger.debug(
                f"Skipping alert calculation len, {len(df_in)}: {self.w_idx}/{self.w_len}:."
            )
            return df_in.iloc[-1]

        logger.debug(
            f"Creating alerts view: idx={self.w_idx}, len={len(df_in)}, window={window}, "
            f"required={min_valid_len}, {df_in.loc[curr, TBOT_COL]}"
        )

        df = df_in.iloc[-min_valid_len:].copy()
        df.reset_index(drop=True, inplace=True)
        last = df.index[-1]

        # Check if necessary AlphaTrend values are not NaN
        if df[APH_COL].isna().any():
            return df.loc[last]

        try:
            buySignalk = TAUtils.crossover(df[APH_COL], df[APH_COL].shift(2))
            sellSignalk = TAUtils.crossunder(df[APH_COL], df[APH_COL].shift(2))
            df.loc[last, IND3_BOOL_COL] = buySignalk.iloc[-1]
            df.loc[last, IND4_BOOL_COL] = sellSignalk.iloc[-1]
        except Exception as e:
            logger.error(f"Error crossover or crossunder : {e}")
            return df.loc[last]

        try:
            # Calculate barsSince values
            prev_buy = prev_row[IND5_COL]
            prev_sell = prev_row[IND6_COL]

            pretty_msg_df(sellSignalk, "sellSignalK", "DEBUG")

            last_buy = TAUtils.barssince_scalar(buySignalk.iloc[-1], prev_buy)
            df.loc[last, IND5_COL] = 0 if pd.isna(last_buy) else last_buy

            last_sell = TAUtils.barssince_scalar(
                sellSignalk.iloc[-1], prev_sell
            )
            df.loc[last, IND6_COL] = 0 if pd.isna(last_sell) else last_sell

            # Log potential alerts
            if (
                buySignalk.iloc[-1]
                and not pd.isna(prev_buy)
                and not pd.isna(last_sell)
                and prev_buy > last_sell
            ):
                logger.success(f"Potential BUY alert detected at index {last}")
                df.loc[last, ALT_BUY] = True

            if (
                sellSignalk.iloc[-1]
                and not pd.isna(prev_sell)
                and not pd.isna(last_buy)
                and prev_sell > last_buy
            ):
                logger.success(
                    f"Potential SELL alert detected at index {last}"
                )
                df.loc[last, ALT_SELL] = True

            # if (buySignalk.iloc[-2] and not pd.isna(prev_buy) and not pd.isna(last_sell) and
            #         prev_prev_buy > last_sell):
            #     df.loc[last, ALT_BUY] = True
            #     logger.info(f"Confirmed BUY alert at index {last}.")

            # if (sellSignalk.iloc[-2] and not pd.isna(prev_sell) and not pd.isna(last_buy) and
            #         prev_prev_sell > last_buy):
            #     df.loc[last, ALT_SELL] = True
            #     logger.info(f"Confirmed SELL alert at index {last}.")

            df.loc[last, TBOT_COL] = TBOTStatus.add_status(
                df.loc[last, TBOT_COL], TBOTStatus.ALERT_COMPLETED
            )

        except Exception as e:
            logger.error(f"Error generating alert buy and sell: {e}")

        pretty_msg_df(df.tail(1), "calculate_alerts", "DEBUG")

        return df.loc[last]

    @trace_function
    async def post_alert_messages(
        self, order: OrderTV, df_in: pd.DataFrame, update_status_cb=None
    ):
        """
        Generate Alert Messages according to TBOT on TradingBoat's Rules
        and then send it to Redis
        """
        curr = df_in.index[-1]
        # The above Python code snippet is checking three conditions using an `if` statement:
        if (
            TBOT_WIND_DUPLICATE_MSG_FILTER
            and TBOT_WIND_ENABLE_TBOT_COL
            and TBOTStatus.has_status(
                df_in.loc[curr, TBOT_COL], TBOTStatus.ALERT_GENERATED
            )
        ):
            logger.debug(f"{self.w_idx}/ alert already sent")
            return

        window = self.window_base
        min_valid_len = window + 1
        if len(df_in) < min_valid_len:
            logger.debug(
                f"Skipping sending alert len, {len(df_in)}: {self.w_idx}/{self.w_len}:."
            )
            return

        row = df_in.iloc[-1]
        buy_signal = row[ALT_BUY]
        sell_signal = row[ALT_SELL]

        if not buy_signal and not sell_signal:
            logger.trace("No alert messages detected")
            return
        else:
            logger.success("Alert messages detected")
            pretty_print_df(row, "SUCCESS")

        timestamp = int(pd.Timestamp(row["date"]).timestamp() * 1000)
        common_ref = "AlphaT"
        common_params = {
            "ticker": order.symbol,
            "timeframe": order.timeframe,
            "contract": order.contract,
            "currency": order.currency,
            "qty": order.qty,
            "timestamp": timestamp,
            "orderRef": common_ref,
        }

        # Pass the signal data to OpenAI for analysis
        prompt_data = pd.DataFrame(
            {
                "APH": [row[APH_COL]],
                "RSI": [row[RSI_COL]],
                "ATR": [row[ATR_COL]],
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
                ai_msg = ai_decision.get(
                    "interpretation", "No additional interpretation available."
                )
                readable = pd.Timestamp(row["date"]).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                logger.success(
                    f"Generated buy signal {ai_msg} at {readable} for {order.symbol} (timestamp: {timestamp}): {msg}"
                )
        elif sell_signal:
            msg = self.make_webhook_json(
                direction="strategy.close",
                **common_params,
            )
            result = await super().send_alert_message(msg)
            if result:
                # Get OpenAI’s interpretation of the signal
                ai_decision = await self.ai_engine.analyze_signal(prompt_data)

                # Add OpenAI interpretation to the message
                ai_msg = ai_decision.get(
                    "interpretation", "No additional interpretation available."
                )
                readable = pd.Timestamp(row["date"]).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                logger.success(
                    f"Generated sell signal {ai_msg} at {readable} for {order.symbol} (timestamp: {timestamp}): {msg}"
                )
        else:
            logger.error(
                f"Generated invalid signal at {readable} for {order.symbol} (timestamp: {timestamp}): {msg}"
            )

        if update_status_cb:
            logger.info(
                f"{self.w_idx}/{self.w_len} TBOTStatus.ALERT_GENERATED"
            )
            update_status_cb(curr, TBOTStatus.ALERT_GENERATED)
