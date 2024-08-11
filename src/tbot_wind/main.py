import asyncio
import sys
import argparse
import socket
from typing import List
from pathlib import Path

import pandas as pd
from loguru import logger

from .core.tbot_wind import TbotWind
from .core.data_initializer import DataInitializerContext
from .database.database_wrapper import DataBaseWrapper
from .utils.tbot_log import tbot_initialize_log
from .user.custom_order import create_orders_from_yaml
from .utils.objects import OrderTV

from .user.supertrend_indicator_talipp import SupertrendIndicatorTalip
from .user.macd_indicator_talipp import MACDIndicatorTalipp
from .user.alpha_trend_indicator import AlphaTrendIndicator


pd.options.mode.copy_on_write = True


def shutdown(loop, wind=None, db_wrapper=None):
    """
    Gracefully shutdown the application by closing connections and stopping the event loop.
    """
    logger.info("Shutdown initiated. Closing connections...")

    try:
        if wind:
            wind.connection_manager.disconnect()
            logger.info("Disconnected from IB Gateway.")

        if db_wrapper:
            db_wrapper.close()
            logger.info("Database connections closed.")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

    # Cancel all running tasks before stopping the loop
    if loop.is_running():
        logger.info("Stopping event loop and cancelling tasks...")
        tasks = [
            t for t in asyncio.all_tasks() if t is not asyncio.current_task()
        ]
        for task in tasks:
            task.cancel()

        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
    loop.stop()


def handle_exception(loop, context):
    """
    Handle uncaught exceptions in the asyncio event loop.
    """
    exception = context.get("exception", context["message"])
    logger.error(f"Uncaught exception: {exception}")

    # Trigger synchronous shutdown
    shutdown(loop, context.get("wind"), context.get("db_wrapper"))


async def initialize_database(wind, orders: List[OrderTV]):
    """
    Initializes the database with warm-up data for the orders and then shuts down.
    """
    logger.info("Starting database initialization...")
    data_initializer = DataInitializerContext(wind.db_wrapper, wind.fetcher)

    try:
        # Loop through all orders to initialize warm-up data
        for order in orders:
            await data_initializer.warmup_data(order)
        logger.success("Database initialized successfully.")
        if wind and wind.connection_manager:
            wind.connection_manager.disconnect()
            logger.info("Disconnected from IB Gateway.")

        if wind.db_wrapper:
            wind.db_wrapper.close()
            logger.info("Database connections closed.")
    except Exception as e:
        logger.error(f"Error during database initialization: {e}")


def parse_arguments():
    """
    Parses command-line arguments for indicator selection and database initialization.
    Returns the parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="TBOT-Wind Trading Bot",
        epilog="Available indicators:\n"
        " - macd: MACD Indicator (default)\n"
        " - supertrend: Supertrend Indicator\n"
        " - alphatrend: AlphaTrend Indicator"
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize the database with warm-up data only",
    )
    parser.add_argument(
        "--indicator",
        type=str,
        default="macd",
        help="Specify the custom indicator to use (e.g., 'macd', 'supertrend', 'alphatrend')",
    )
    return parser.parse_args()


def initialize_database_if_requested(wind, orders, init_db_flag):
    """
    If the --init-db flag is provided, initializes the database and exits the program.
    """
    if init_db_flag:
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(initialize_database(wind, orders))
        except Exception as e:
            logger.error(f"Error during database initialization: {e}")
        finally:
            loop.stop()
            sys.exit(0)  # Exit to indicate no further execution


def initialize_indicator(indicator_name):
    """
    Returns the custom indicator class based on the user's selection.
    """
    if indicator_name == "alphatrend":
        return AlphaTrendIndicator()
    elif indicator_name == "supertrend":
        return SupertrendIndicatorTalip()
    elif indicator_name == "macd":
        return MACDIndicatorTalipp()
    else:
        logger.error(
            f"Unknown indicator: {indicator_name}. Defaulting to AlphaTrendIndicator."
        )
        return AlphaTrendIndicator()


def main():
    args = parse_arguments()
    tbot_initialize_log()

    config_file_path = Path(__file__).parent / "user" / "orders_config.yaml"
    orders = create_orders_from_yaml(config_file_path)

    # Initialize custom indicator
    indicator = initialize_indicator(args.indicator)
    indicator_name = args.indicator.lower()

    logger.info(f"Initializing Custom Indicator {indicator_name} for TBOT-Wind")

    db_wrapper = DataBaseWrapper(orders=orders, indicator_name=indicator_name)

    # Initialize TbotWind with multiple orders
    wind = TbotWind(indicator=indicator, db_wrapper=db_wrapper, orders=orders)

    initialize_database_if_requested(wind, orders, args.init_db)

    # Start asyncio event loop
    loop = asyncio.get_event_loop()

    # Set up exception handler and pass wind and db_wrapper to handle_exception for proper shutdown
    loop.set_exception_handler(
        lambda l, c: handle_exception(
            l, {**c, "wind": wind, "db_wrapper": db_wrapper}
        )
    )

    logger.info("Starting main event loop...")

    try:
        loop.run_until_complete(wind.blow())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt detected, shutting down...")
        # Call shutdown directly
        shutdown(loop, wind, db_wrapper)
    except socket.error as err:
        logger.error(f"Socket error occurred: {err}")
        shutdown(loop, wind, db_wrapper)
    except Exception as err:
        logger.error(f"An unexpected error occurred: {err}")
        shutdown(loop, wind, db_wrapper)
    finally:
        logger.info("Closing main event loop...")
        loop.close()
        logger.info("Event loop closed.")


if __name__ == "__main__":
    main()
