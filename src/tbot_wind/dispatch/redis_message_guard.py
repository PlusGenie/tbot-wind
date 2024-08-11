from typing import Dict, List
from loguru import logger

from .message_sender_base import MessageSenderBase
from .redis_message_publisher import RedisMessagePublisher
from .message_deduplicator import MessageDeduplicator


class RedisMessageGuard(MessageSenderBase):
    def __init__(
        self,
        deduplicator: MessageDeduplicator,
        redis_client: RedisMessagePublisher,
    ):
        # Initialize the RedisMessagePublisher instance
        # self.cl = RedisMessagePublisher()
        self.cl = redis_client
        self.buffer: List[Dict] = (
            []
        )  # Buffer to store messages when Redis is unavailable
        self.deduplicator = deduplicator  # Deduplication handler

    def _flush_buffer(self):
        """Attempt to resend buffered messages when the connection is restored."""
        if not self.buffer:
            return
        logger.critical("Attempt to resend buffered messages")
        logger.critical(
            f"Flushing {len(self.buffer)} messages from the buffer."
        )
        for message in self.buffer.copy():
            if self._send_to_redis(message):
                self.buffer.remove(message)
            else:
                logger.error(
                    "Failed to resend buffered message, stopping flush."
                )
                break  # Stop attempting if sending fails again

    def _send_to_redis(self, json: Dict) -> bool:
        """Send data to Redis and handle errors. Returns True if successful."""
        try:
            if not json:  # Check if the json_data is None or empty
                logger.error("Attempted to send an empty or None message.")
                return False

            # If not a duplicate, send the message via Redis
            if self.cl.is_redis_stream:
                self.cl.run_redis_stream(json)
            else:
                self.cl.run_redis_pubsub(json)

            logger.debug(f"Successfully sent data to Redis: {json}")
            return True
        except Exception as e:
            logger.error(f"Error sending data to Redis: {e}")
            return False

    def send_message(self, json_data: Dict) -> bool:
        """
        Sends the JSON data to the Redis Server with duplication check.

        Args:
            json_data (Dict): A dictionary containing JSON data.
        """
        if self.deduplicator.is_duplicate(json_data):
            logger.warning(
                f"Duplicate message detected: {json_data['orderRef']} Skipping."
            )
            return False

        # Check connection status
        if not self.cl.is_connected():
            logger.critical("Redis connection lost, storing message in buffer")
            self.buffer.append(json_data)
            self.deduplicator.store_message_hash(json_data)
            self.cl.connect_redis_host()  # Attempt to reconnect
            return False

        # First attempt to flush any buffered messages
        self._flush_buffer()

        if self._send_to_redis(json_data):
            self.deduplicator.store_message_hash(json_data)
            return True
        else:
            self.deduplicator.store_message_hash(
                json_data
            )  # Prevent future duplicates
            self.buffer.append(json_data)
            return False
