from zoneinfo import ZoneInfo
import pandas as pd
from loguru import logger


def get_key_timezone(key: str) -> ZoneInfo:
    """
    Extracts and returns the timezone from a Redis key.
    """
    parts = key.split(":")
    tz_index = parts.index("tz")
    tz_str = parts[tz_index + 1]
    return ZoneInfo(tz_str)


def ensure_datetime_with_timezone(
    df: pd.DataFrame, timezone: str
) -> pd.DataFrame:
    """
    Ensure the 'date' column in the DataFrame is in datetime format with timezone information.

    Args:
        df (pd.DataFrame): The DataFrame to process.
        timezone (str): The timezone string, e.g., "US/Eastern".

    Returns:
        pd.DataFrame: The DataFrame with the 'date' column correctly formatted.
    """
    if "date" not in df.columns:
        raise ValueError("The DataFrame does not have a 'date' column.")

    try:
        # Ensure the 'date' column is in datetime format
        if not pd.api.types.is_datetime64_any_dtype(df["date"]):
            logger.warning("Converting 'date' column to datetime format.")
            df["date"] = pd.to_datetime(
                df["date"], errors="coerce"
            )  # Allow coercion of errors to NaT

        # Check if the datetime is naive or has a different timezone than expected
        if df["date"].dt.tz is None:
            # If datetime is naive, localize it to the desired timezone
            logger.warning(
                f"'date' column is naive. Localizing to timezone: {str(timezone)}."
            )
            df["date"] = df["date"].dt.tz_localize(ZoneInfo(timezone))
        elif df["date"].dt.tz != ZoneInfo(timezone):
            # If datetime has a different timezone, convert it to the desired timezone
            logger.critical(
                f"Timezone mismatch: Converting 'date' column from {df['date'].dt.tz} to {str(timezone)}."
            )
            df["date"] = df["date"].dt.tz_convert(ZoneInfo(timezone))
        else:
            logger.trace("'date' column already has the correct timezone.")
    except Exception as e:
        logger.error(f"Failed to process 'date' column. Error: {e}")
        raise  # Raise the exception to ensure it's not silently ignored

    return df


def debug_date_column(df: pd.DataFrame):
    """
    Debugs the 'date' column in a DataFrame by attempting to convert it to datetime,
    logging the dtype after conversion, and printing rows where the conversion fails.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'date' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'date' column converted to datetime.
    """

    date_column = "date"  # Hardcoded 'date' column name
    logger.info(f"Starting date column debugging for column: '{date_column}'")

    # Ensure the 'date' column exists
    if date_column not in df.columns:
        logger.error(f"'{date_column}' column is missing in the DataFrame.")
        return df

    # Ensure the 'date' column is in datetime format
    try:
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    except Exception as e:
        logger.error(
            f"Error converting '{date_column}' column to datetime: {e}"
        )
        raise

    # After conversion, check for any issues
    logger.info(f"Date column dtype after conversion: {df[date_column].dtype}")
    if df[date_column].isnull().any():
        logger.critical(
            f"Null values found in '{date_column}' column after conversion. "
            f"Rows with issues:\n{df[df[date_column].isnull()]}"
        )

    logger.info(f"Completed date column debugging for column: '{date_column}'")

    return df
