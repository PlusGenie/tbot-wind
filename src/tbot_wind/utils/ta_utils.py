import numpy as np
import pandas as pd
from loguru import logger


class TAUtils:
    """
    The TAUtils class provides methods for analyzing time series data, such as determining crossovers
    and crossunders, and the barssince function calculates the number of bars since a given condition
    was True, with a helper function for pretty printing DataFrames.
    """

    @staticmethod
    def check_series_type(series1, series2):
        """
        Check if the input parameters are of type pd.Series.
        Logs a critical warning if the types do not match and returns False.

        Args:
            series1: The first input to check.
            series2: The second input to check.

        Returns:
            bool: True if both inputs are pd.Series, False otherwise.
        """
        if not isinstance(series1, pd.Series):
            logger.critical(
                f"Expected series1 to be of type pd.Series but got {type(series1)} instead."
            )
            return False
        if not isinstance(series2, pd.Series):
            logger.critical(
                f"Expected series2 to be of type pd.Series but got {type(series2)} instead."
            )
            return False
        return True

    @staticmethod
    def crossover(series1: pd.Series, series2: pd.Series) -> pd.Series:
        """
        Determine where crossovers occur throughout the entire series.

        A crossover is defined as:
        - On the current bar, `series1` is greater than `series2`, and
        - On the previous bar, `series1` was less than or equal to `series2`.

        Args:
            series1 (pd.Series): First data series.
            series2 (pd.Series): Second data series.

        Returns:
            pd.Series: A boolean series indicating where crossovers occur.
        """
        if not TAUtils.check_series_type(series1, series2):
            return pd.Series([False] * len(series1), index=series1.index)

        if len(series1) < 2 or len(series2) < 2:
            logger.warning(
                "Series length is less than 2, cannot determine crossover."
            )
            return pd.Series([False] * len(series1), index=series1.index)

        # Calculate crossover for the entire series
        crossover_occurred = (series1 > series2) & (
            series1.shift(1) <= series2.shift(1)
        )
        return crossover_occurred

    @staticmethod
    def crossunder(series1: pd.Series, series2: pd.Series) -> pd.Series:
        """
        Determine where crossunders occur throughout the entire series.

        A crossunder is defined as:
        - On the current bar, `series1` is less than `series2`, and
        - On the previous bar, `series1` was greater than or equal to `series2`.

        Args:
            series1 (pd.Series): First data series.
            series2 (pd.Series): Second data series.

        Returns:
            pd.Series: A boolean series indicating where crossunders occur.
        """
        if not TAUtils.check_series_type(series1, series2):
            return pd.Series([False] * len(series1), index=series1.index)

        if len(series1) < 2 or len(series2) < 2:
            logger.warning(
                "Series length is less than 2, cannot determine crossunder."
            )
            return pd.Series([False] * len(series1), index=series1.index)

        # Calculate crossunder for the entire series
        crossunder_occurred = (series1 < series2) & (
            series1.shift(1) >= series2.shift(1)
        )
        return crossunder_occurred

    @staticmethod
    def crossover_b(series1: pd.Series, series2: pd.Series) -> bool:
        """
        Determine if a crossover occurred at the most recent data point.

        Assumption:
        - `series1` and `series2` represent time series data, such as stock prices.
        - `series1[0]` is the oldest data point and `series1[-1]` is the most recent data point.

        A crossover is defined as:
        - On the current bar (the last element in the series), `series1` is greater than `series2`, and
        - On the previous bar (second-to-last element), `series1` was less than or equal to `series2`.

        Args:
            series1 (pd.Series): First data series (e.g., fast moving average).
            series2 (pd.Series): Second data series (e.g., slow moving average).

        Returns:
            bool: True if `series1` has crossed over `series2` at the most recent data point, otherwise False.
        """
        if not TAUtils.check_series_type(series1, series2):
            return False

        if len(series1) < 2 or len(series2) < 2:
            logger.warning(
                "Series length is less than 2, cannot determine crossover."
            )
            return False

        # Check if crossover occurred on the last element
        crossover_occurred = (series1.gt(series2).iloc[-1]) and (
            series1.le(series2).iloc[-2]
        )
        logger.trace(f"Crossover occurred: {crossover_occurred}")

        return crossover_occurred

    @staticmethod
    def crossunder_b(series1: pd.Series, series2: pd.Series) -> bool:
        """
        Determine if a crossunder occurred at the most recent data point.

        Assumption:
        - `series1` and `series2` represent time series data, such as stock prices.
        - `series1[0]` is the oldest data point and `series1[-1]` is the most recent data point.

        A crossunder is defined as:
        - On the current bar (the last element in the series), `series1` is less than `series2`, and
        - On the previous bar (second-to-last element), `series1` was greater than or equal to `series2`.

        Args:
            series1 (pd.Series): First data series (e.g., fast moving average).
            series2 (pd.Series): Second data series (e.g., slow moving average).

        Returns:
            bool: True if `series1` has crossed under `series2` at the most recent data point, otherwise False.
        """
        if not TAUtils.check_series_type(series1, series2):
            return False

        if len(series1) < 2 or len(series2) < 2:
            logger.warning(
                "Series length is less than 2, cannot determine crossunder."
            )
            return False

        # Check if crossunder occurred on the last element
        crossunder_occurred = (series1.lt(series2).iloc[-1]) and (
            series1.ge(series2).iloc[-2]
        )
        logger.trace(f"Crossunder occurred: {crossunder_occurred}")

        return crossunder_occurred

    @staticmethod
    def barssince(condition: pd.Series) -> pd.Series:
        """
        Return the number of bars since a given condition was True.
        This function does not modify the original series.

        Assumption:
        - The `condition` series represents a boolean time series, where `condition[0]` is the oldest data point
        and `condition[-1]` is the most recent data point.

        Args:
            condition (pd.Series): A boolean series indicating where the condition is True.

        Returns:
            pd.Series: A series indicating the number of bars since the condition was True,
                    or a series filled with NaN if the input type is incorrect.
        """
        # Ensure NaN values are preserved, but dtype remains bool where applicable
        condition = condition.astype("boolean")

        # Initialize the result series with NaN
        result = pd.Series(np.nan, index=condition.index)
        last_true_index = None

        for i in range(len(condition)):
            if pd.isna(condition[i]):
                continue  # Leave NaN in result
            elif condition[i]:
                last_true_index = i
                result[i] = 0
            elif last_true_index is not None and not pd.isna(condition[i - 1]):
                result[i] = i - last_true_index
            else:
                result[i] = np.nan

        # Check if the result is entirely NaN
        if result.isna().all():
            logger.trace("barssince() returned a series filled with NaN.")

        return result

    @staticmethod
    def barssince_loopback(
        condition: pd.Series, loopback: float = np.nan
    ) -> pd.Series:
        """
        Return the number of bars since a given condition was True, considering an optional loopback value.

        If no loopback is provided (loopback is NaN), the function will call the existing barssince() method.

        Args:
            condition (pd.Series): A boolean series indicating where the condition is True.
            loopback (float, optional): The previous number of bars since the last True condition. Defaults to NaN.

        Returns:
            pd.Series: A series indicating the number of bars since the condition was True, continuing from `loopback`.
        """
        # If loopback is NaN, use the existing barssince() method
        if pd.isna(loopback):
            return TAUtils.barssince(condition)

        # Initialize the result series with NaN
        result = pd.Series(np.nan, index=condition.index)

        # Start the count from the loopback value
        current_count = loopback if not pd.isna(loopback) else np.nan
        last_true_index = None

        # Iterate over the series and calculate bars since last True condition
        for i in range(len(condition)):
            if pd.isna(condition[i]):
                continue  # Leave NaN in result
            elif condition[i]:
                last_true_index = i
                result[i] = 0
                current_count = 0
            else:
                if last_true_index is not None:
                    current_count += 1
                else:
                    current_count += 1  # Increment based on loopback even if no True condition was met

                result[i] = current_count

        return result

    @staticmethod
    def barssince_scalar(condition: bool, loopback: float = np.nan) -> float:
        """
        Return the number of bars since the given condition was True, considering an optional loopback value.

        If no loopback is provided (loopback is NaN), the function will return NaN.

        Args:
            condition (bool): A boolean value indicating whether the condition is True or False.
            loopback (float, optional): The previous number of bars since the last True condition. Defaults to NaN.

        Returns:
            float: The number of bars since the condition was True, continuing from `loopback`.
        """
        if pd.isna(loopback):
            # If loopback is NaN and condition is False, we can't determine bars since condition was True
            if not condition:
                return np.nan
            # If condition is True and no loopback, this is the first occurrence, so return 0
            return 0.0

        # If condition is True, reset to 0
        if condition:
            return 0.0
        # If condition is False, increment the count from the loopback value
        return loopback + 1
