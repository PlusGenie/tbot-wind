# -*- coding: utf-8 -*-
"""
TradingBoat © Copyright, Plusgenie Limited 2024. All Rights Reserved.
"""

###############################################################################
# User Configuration
###############################################################################
# Send the alert message to HTTP server of Redis Server
TBOT_WIND_SEND_MSG_TO_REDIS = True

# Choose Redis or SQLITE3 for storing cold or warm data
TBOT_WIND_USES_SQLITE_DATABASE = False

# Date format used in Redis for storing datetime values
TBOT_WIND_REDIS_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

TBOT_WIND_IB_VIOLATION_SLEEP_SEC = 15

# Prefix for custom indicator keys stored in Redis
TBOT_WIND_INDICATOR_PREFIX = "R12"

# Limit for pretty-printing DataFrames (number of rows to display)
TBOT_WIND_PRETTY_PRINT_LIMIT = 64
TBOT_WIND_PRETTY_PRINT_MIN = 12

# Enables or disables the check for duplicate alerts. If enabled, the system will check
# whether an alert has already been generated to avoid duplicates.
# If disabled, duplicate alerts might be generated but filtered out by Redis Pub/Sub.
TBOT_WIND_ENABLE_TBOT_COL = True


# Enables or disables the filtering of duplicate messages in Redis Pub/Sub.
# If enabled, the system will prevent sending the same message multiple times.
TBOT_WIND_DUPLICATE_MSG_FILTER = True

TBOT_WIND_CONSUMER_NUM = 2

# DataFrame Columns for TBOT-WIND signals and statuses
TBOT_COL = "TBOT"
ALT_BUY = "BUY"
ALT_SELL = "SELL"
IND0_COL = "ind0"
IND1_COL = "ind1"
IND2_COL = "ind2"
IND3_BOOL_COL = "ind3"
IND4_BOOL_COL = "ind4"
IND5_COL = "ind5"
IND6_COL = "ind6"
IND7_COL = "ind7"
IND8_COL = "ind8"
IND9_COL = "ind9"

# Redis key prefix used to store hashes of sent messages to prevent duplication
TBOT_REDIS_SENT_MSG_KEY = "TBOT_REDIS_SENT_MSGS"

# Define constants for different modes
FETCH_MODE_INCREMENTAL = "incremental"
FETCH_MODE_STREAM = "stream"
FETCH_MODE_TEST = "test"
current_fetch_mode = FETCH_MODE_INCREMENTAL

# Map modes to corresponding functions
FETCH_MODE_FUNCTION_MAP = {
    FETCH_MODE_INCREMENTAL: "fetch_incremental_data",
    FETCH_MODE_STREAM: "fetch_stream_data",
    FETCH_MODE_TEST: "fetch_test_data",
}
