# -*- coding: utf-8 -*-
"""
TradingBoat © Copyright, Plusgenie Limited 2024. All Rights Reserved.

This file holds NamedTuple to hold data for TBOT
"""
import os
import platform

from dataclasses import dataclass, field
from functools import partial
import tempfile


@dataclass(frozen=True)
class EnvSettings:
    """
    Create Dataclass to read Environment Values
    """

    # Function to generate platform-specific log and database paths
    @staticmethod
    def get_default_logfile():
        if platform.system() == "Windows":
            return os.path.join(tempfile.gettempdir(), "tbot_wind_log.txt")
        else:
            return "/tmp/tbot_wind_log.txt"

    @staticmethod
    def get_http_msg_hash_file():
        if platform.system() == "Windows":
            return os.path.join(
                tempfile.gettempdir(), "tbot_wind_duplicate_messages.txt"
            )
        else:
            return "/tmp/tbot_wind_duplicate_messages.txt"

    @staticmethod
    def get_default_db_office():
        if platform.system() == "Windows":
            return os.path.join(tempfile.gettempdir(), "tbot_wind_sqlite3")
        else:
            return "/home/tbot/tbot_sqlite3"

    # ---------------------------------
    # Interactive Brokers
    # ---------------------------------
    wind_client_id: str = field(
        default_factory=partial(
            os.environ.get, "TBOT_WIND_IBKR_CLIENTID", "11"
        )
    )

    tbot_client_id: str = field(
        default_factory=partial(os.environ.get, "TBOT_IBKR_CLIENTID", "3")
    )
    ibkr_port: str = field(
        default_factory=partial(os.environ.get, "TBOT_IBKR_PORT", "4002")
    )
    ibkr_addr: str = field(
        default_factory=partial(
            os.environ.get, "TBOT_IBKR_IPADDR", "127.0.0.1"
        )
    )

    http_server_addr: str = field(
        default_factory=partial(
            os.environ.get,
            "TBOT_WIND_HTTP_SERVER_ADDR",
            "http://localhost:5000/webhook",
        )
    )

    # File to store message hashes
    http_msg_hash_file: str = field(
        default_factory=partial(
            os.environ.get,
            "DUPLICATE_HTTP_MESSAGE_FILE",
            get_http_msg_hash_file(),
        )
    )

    # ---------------------------------
    # Loglevel
    # ---------------------------------
    loglevel: str = field(
        default_factory=partial(os.environ.get, "TBOT_WIND_LOGLEVEL", "TRACE")
    )
    logfile: str = field(
        default_factory=partial(
            os.environ.get, "TBOT_WIND_LOGFILE", get_default_logfile()
        )
    )
    log_rotation: str = field(
        default_factory=partial(
            os.environ.get, "TBOT_WIND_LOG_ROTATION", "128 MB"
        )
    )
    # loglovel for IB INSYNC
    ib_loglevel: str = field(
        default_factory=partial(
            os.environ.get, "TBOT_WIND_IB_LOGLEVEL", "DEBUG"
        )
    )

    # Timezone for IB INSYNC
    ib_timezone: str = field(
        default_factory=partial(
            os.environ.get, "TBOT_WIND_IB_TIMEZONE", "US/Eastern"
        )
    )

    # ---------------------------------------
    # Configuration for TBOT WIND Behavior
    # ---------------------------------------

    is_production: str = field(
        default_factory=partial(
            os.environ.get, "TBOT_WIND_PRODUCTIONT", "False"
        )
    )

    # ---------------------------------
    # Enable TBOT Profiler
    # ---------------------------------
    profiler: str = field(
        default_factory=partial(os.environ.get, "TBOT_PROFILER", "False")
    )

    # ---------------------------------
    # Redis database
    # ---------------------------------
    r_passwd: str = field(
        default_factory=partial(os.environ.get, "TBOT_REDIS_PASSWORD", "")
    )
    r_host_unix: str = field(
        default_factory=partial(
            os.environ.get, "TBOT_REDIS_UNIXDOMAIN_SOCK", ""
        )
    )
    r_host: str = field(
        default_factory=partial(os.environ.get, "TBOT_REDIS_HOST", "127.0.0.1")
    )
    r_port: str = field(
        default_factory=partial(os.environ.get, "TBOT_REDIS_PORT", "6379")
    )
    r_ttl: int = field(
        default_factory=partial(
            os.environ.get,
            "TBOT_REDIS_LAMP_TTL",
            60 * 60 * 24 * 2,  # 2 days for development
        )
    )
    r_is_stream: str = field(
        default_factory=partial(
            os.environ.get, "TBOT_USES_REDIS_STREAM", "True"
        )
    )
    r_read_timeout_ms: str = field(
        default_factory=partial(
            os.environ.get, "TBOT_REDIS_READ_TIMEOUT_MS", "40"
        )
    )

    # ---------------------------------
    # SQLite3 Database
    # ---------------------------------
    db_office: str = field(
        default_factory=partial(
            os.environ.get, "TBOT_WIND_DB_OFFICE", get_default_db_office()
        )
    )

    # ---------------------------------
    # OpenAI API Key
    # ---------------------------------
    openai_key: str = field(
        default_factory=partial(os.environ.get, "OPENAI_API_KEY", "")
    )

    openai_model: str = field(
        default_factory=partial(
            os.environ.get, "TBOT_WIND_OPENAI_MODEL", "gpt-4o-mini"
        )
    )


@dataclass
class OrderTV:
    """
    Create NamedTuple for Orders which is needed to place orders onto IB/TWS
    """

    timestamp: str
    contract: str
    symbol: str
    timeframe: str
    timezone: str
    direction: str
    qty: float
    currency: str
    entryLimit: float
    entryStop: float
    exitLimit: float
    exitStop: float
    # TradingView's close price of the bar
    price: float
    orderRef: str
    tif: str
    exchange: str
    lastTradeDateOrContractMonth: str
    multiplier: str
    outsideRth: bool


@dataclass
class OrderTVEx(OrderTV):
    """
    A subclass of OrderTV with additional fields for TBOT-Wind
    """

    duration: str
    end_date: str
    bootup_duration: str
    indicator: str
