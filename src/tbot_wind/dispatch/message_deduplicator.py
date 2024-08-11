import os
from datetime import datetime, timezone
from typing import Dict
from loguru import logger
import hashlib

from ..utils.constants import (
    TBOT_WIND_DUPLICATE_MSG_FILTER,
    TBOT_REDIS_SENT_MSG_KEY,
)
from ..utils.tbot_env import shared


class MessageDeduplicator:
    def __init__(self, redis_client=None):
        """
        Initialize the deduplicator with Redis client or fallback to file-based deduplication.
        """
        self.redis_client = redis_client
        self.duplicate_file = shared.http_msg_hash_file

        if self.redis_client:
            logger.info("Initialize the deduplicator with Redis client")
        else:
            logger.info(
                f"Initialize the deduplicator with file:{self.duplicate_file}"
            )

        if not TBOT_WIND_DUPLICATE_MSG_FILTER:
            logger.critical(
                "Message deduplicator - TBOT_WIND_DUPLICATE_MSG_FILTER disabled"
            )

    def _generate_msg_hash(self, json_data: Dict) -> str:
        """Generate a unique and consistent hash for the message."""
        message_id = (
            f"{json_data['ticker']}_{json_data['timeframe']}_{json_data['orderRef']}_"
            f"{json_data['direction']}_{json_data['timestamp']}"
        )
        # Use MD5 for consistent hashing
        return hashlib.md5(message_id.encode()).hexdigest()

    def _is_duplicate_in_redis(self, msg_hash: str, json_data: Dict) -> bool:
        """Check if the message hash is a duplicate in Redis."""
        if self.redis_client.connection.sismember(
            TBOT_REDIS_SENT_MSG_KEY, msg_hash
        ):
            ts = int(json_data["timestamp"])
            m_date = datetime.fromtimestamp(
                ts / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S")
            logger.warning(
                f"Duplicate message detected: {msg_hash} (Date-UTC: {m_date}) Skipping."
            )
            return True
        return False

    def _is_duplicate_in_file(self, msg_hash: str, json_data: Dict) -> bool:
        """Check if the message hash is a duplicate in the file."""
        if not os.path.exists(self.duplicate_file):
            return False
        with open(self.duplicate_file, "r") as file:
            for line in file:
                if line.strip() == msg_hash:
                    ts = int(json_data["timestamp"])
                    m_date = datetime.fromtimestamp(
                        ts / 1000, tz=timezone.utc
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    logger.warning(
                        f"Duplicate message detected: {msg_hash} (Date-UTC: {m_date}) Skipping."
                    )
                    return True

        return False

    def _store_duplicate_in_redis(self, message_hash: str) -> None:
        """Store the message hash in Redis to mark it as sent."""
        self.redis_client.connection.sadd(
            TBOT_REDIS_SENT_MSG_KEY, message_hash
        )

    def _store_duplicate_in_file(self, msg_hash: str) -> None:
        """Store the message hash in a file to mark it as sent."""

        with open(self.duplicate_file, "a") as file:
            logger.info(f"Store the message hash {msg_hash} in a file")
            file.write(msg_hash + "\n")

    def is_duplicate(self, json_data: Dict) -> bool:
        """
        Check if a message is a duplicate, either using Redis or a file-based method.
        """
        if TBOT_WIND_DUPLICATE_MSG_FILTER:
            msg_hash = self._generate_msg_hash(json_data)
            if self.redis_client:
                return self._is_duplicate_in_redis(msg_hash, json_data)
            else:
                return self._is_duplicate_in_file(msg_hash, json_data)
        else:
            return False

    def store_message_hash(self, json: Dict) -> None:
        try:
            msg_hash = self._generate_msg_hash(json)
            if self.redis_client:
                self._store_duplicate_in_redis(msg_hash)
            else:
                self._store_duplicate_in_file(msg_hash)
        except Exception as e:
            logger.error(f"Failed to store the message: {e}")
