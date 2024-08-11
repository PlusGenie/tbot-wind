# -*- coding: utf-8 -*-
"""
TradingBoat © Copyright, Plusgenie Limited 2024. All Rights Reserved.
"""
__author__ = "Sangwook Lee"
__copyright__ = "Copyright (C) 2024 Plusgenie Ltd"
__license__ = "Dual-Licensing (GPL or Commercial License)"


import sys
from loguru import logger
from functools import wraps
from tabulate import tabulate
import pandas as pd
from .tbot_env import shared
from .constants import TBOT_WIND_PRETTY_PRINT_LIMIT, TBOT_WIND_PRETTY_PRINT_MIN


LOGGER_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "{extra[clientId]} | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)


def tbot_initialize_log(log_level=None):
    """Initialize log for Tbot"""
    logger.configure(extra={"clientId": shared.wind_client_id})
    logger.remove()

    log_level = (
        log_level or shared.loglevel
    )  # Use the provided log level, or fall back to the shared one

    logger.add(sys.stderr, level=log_level, format=LOGGER_FORMAT)

    # Watch out too big logfile
    logger.add(
        shared.logfile,
        level=log_level,
        format=LOGGER_FORMAT,
        rotation=shared.log_rotation,
        compression="zip",
        enqueue=True,
        retention="7 days",
        mode="w",
    )


def trace_function(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.trace(f"Entering {func.__name__}()")
        result = func(*args, **kwargs)
        logger.trace(f"Exiting {func.__name__}()")
        return result

    return wrapper


def pretty_msg_df(
    df: pd.DataFrame, message: str, level: str = "TRACE"
) -> None:
    """
    Print a message followed by the DataFrame using tabulate for better readability, up to a certain limit.

    Args:
        df (pd.DataFrame): The DataFrame or Series to print.
        message (str): The message to print before the DataFrame.
        level (str): The log level to use. Defaults to "TRACE".
    """

    # Return early if the log level is lower than the current log level
    if logger.level(level).no < logger._core.min_level:
        return

    # Check if the input is None or empty
    if df is None:
        logger.critical("The DataFrame is None.")
        return

    # If the input is not a DataFrame or Series, log an error and return
    if not isinstance(df, (pd.DataFrame, pd.Series)):
        logger.error(f"Expected DataFrame or Series, but got {type(df)}.")
        return

    # If it's a Series, convert it to a DataFrame
    if isinstance(df, pd.Series):
        df = df.to_frame().T  # Convert Series to DataFrame

    # Check if the DataFrame is too large to print
    if len(df) > TBOT_WIND_PRETTY_PRINT_LIMIT:
        logger.critical(
            f"Too big ({len(df)} rows) to print! Limiting to {TBOT_WIND_PRETTY_PRINT_MIN} rows."
        )
        limited_df = df.head(TBOT_WIND_PRETTY_PRINT_MIN)
    else:
        limited_df = df  # Use the full DataFrame if it's within the limit

    # Generate the table for printing
    try:
        table = tabulate(
            limited_df, headers="keys", tablefmt="pretty", showindex=False
        )
    except Exception as e:
        logger.error(f"Error generating table with tabulate: {e}")
        return

    # Log the message and then the DataFrame at the specified log level
    try:
        logger.log(level, f"{message}\n{table}\n")
    except Exception as e:
        logger.error(f"Error logging table at level {level}: {e}")


def pretty_print_df(df: pd.DataFrame, level: str = "TRACE") -> None:
    """
    Print the DataFrame using tabulate for better readability, up to a certain limit.

    Args:
        df (pd.DataFrame): The DataFrame or Series to print.
        level (str): The log level to use. Defaults to "TRACE".
    """

    # Return early if the log level is lower than the current log level
    if logger.level(level).no < logger._core.min_level:
        return

    # Check if the input is None or empty
    if df is None:
        logger.critical("The DataFrame is None.")
        return

    # If the input is not a DataFrame or Series, log an error and return
    if not isinstance(df, (pd.DataFrame, pd.Series)):
        logger.error(f"Expected DataFrame or Series, but got {type(df)}.")
        return

    # If it's a Series, convert it to a DataFrame
    if isinstance(df, pd.Series):
        df = df.to_frame().T  # Convert Series to DataFrame

    # Check if the DataFrame is too large to print
    if len(df) > TBOT_WIND_PRETTY_PRINT_LIMIT:
        logger.critical(
            f"Too big ({len(df)} rows) to print! Limiting to {TBOT_WIND_PRETTY_PRINT_MIN} rows."
        )
        limited_df = df.head(TBOT_WIND_PRETTY_PRINT_MIN)
    else:
        limited_df = df  # Use the full DataFrame if it's within the limit

    # Generate the table for printing
    try:
        table = tabulate(
            limited_df, headers="keys", tablefmt="pretty", showindex=False
        )
    except Exception as e:
        logger.error(f"Error generating table with tabulate: {e}")
        return

    # Log at the specified log level
    try:
        logger.log(level, "\n" + table + "\n")
    except Exception as e:
        logger.error(f"Error logging table at level {level}: {e}")
