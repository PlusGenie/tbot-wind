from dataclasses import dataclass
from distutils.util import strtobool
import json
from redis import Redis
from typing import Dict
from loguru import logger

from ..utils.tbot_env import shared


# This is a prefix for the Redis pub/sub channel name used
# to communicate between TBOT-Wind and TBOT on TradingBoat
REDIS_CHANNEL = "REDIS_CH_"
# This is a prefix for the Redis stream key used to
# communicate between TBOT-Wind and TBOT on TradingBoat
REDIS_STREAM_KEY = "REDIS_SKEY_"
# This is a key used in the Redis stream dictionary to identify the data from TradingBoat.
REDIS_STREAM_TB_KEY = "tradingboat"

TBOT_CLIENT_MAX_LEN = 1


@dataclass
class RedisClient:
    """Redis Client for TradingView's ClientId"""

    stream_key: str
    channel: str
    connection: Redis = None


class RedisMessagePublisher:
    """Class for handling Redis connections for message delivery.

    This class sets up a Redis connection either as a stream or as a
    pub/sub channel, based on the value of the environment variable
    `TBOT_USES_REDIS_STREAM`.
    """

    def __init__(self):
        super().__init__()
        self.client = None
        self.connection = None  # Redis
        self.is_redis_stream = strtobool(shared.r_is_stream)
        self.client_id = shared.tbot_client_id
        self.client = RedisClient(
            stream_key=REDIS_STREAM_KEY + str(self.client_id),
            channel=REDIS_CHANNEL + str(self.client_id),
        )
        self.connect_redis_host()

    def is_connected(self) -> bool:
        """Check if the Redis connection is still alive."""
        if self.connection:
            try:
                return self.connection.ping()
            except Exception as e:
                logger.critical(f"Redis connection lost: {e}")
                return False
        else:
            return False

    def connect_redis_host(self):
        """Connect to Redis via either Unix socket or TCP"""
        password = shared.r_passwd
        host = shared.r_host
        unix_sock = shared.r_host_unix

        try:
            if host:
                self.connection = Redis(
                    host=host,
                    port=int(shared.r_port),
                    password=password,
                    decode_responses=True,
                )
                logger.info(
                    f"Connected to Redis via TCP | Host: {host} | Port: {shared.r_port}"
                )
            else:
                self.connection = Redis(
                    unix_socket_path=unix_sock,
                    password=password,
                    decode_responses=True,
                )
                logger.info(
                    f"Connected to Redis via Unix socket | Socket: {unix_sock}"
                )

            if self.is_redis_stream:
                logger.success(
                    f"Connected to Redis Stream | {self.client.stream_key}:{REDIS_STREAM_TB_KEY}"
                )
            else:
                logger.success(
                    f"Connected to Redis Pub/Sub | {self.client.channel}"
                )

        except ConnectionRefusedError as err:
            logger.error(f"Connection refused to Redis: {err}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")

    def run_redis_stream(self, json_data: Dict):
        """Add data to the Redis stream"""
        if self.connection and json_data:
            try:
                stream_dict = {REDIS_STREAM_TB_KEY: json.dumps(json_data)}
                self.connection.xadd(self.client.stream_key, stream_dict)
                logger.success(
                    f"Pushed to stream | {self.client.stream_key}:{stream_dict}"
                )
            except Exception as e:
                logger.error(f"Failed to push to Redis stream: {e}")
        else:
            logger.critical(f"No connection to Redis Pub/Sub or {json_data}")

    def run_redis_pubsub(self, json_data: Dict):
        """Publish message to Redis Pub/Sub"""
        if self.connection and json_data:
            try:
                self.connection.publish(
                    self.client.channel, json.dumps(json_data)
                )
                logger.success(f"Pushed to Pub/Sub | {self.client.channel}")
            except Exception as e:
                logger.error(f"Failed to publish to Redis Pub/Sub: {e}")
        else:
            logger.critical(f"No connection to Redis Pub/Sub or {json_data}")
