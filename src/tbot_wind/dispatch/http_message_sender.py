import aiohttp
from loguru import logger
from typing import Dict, List

from .message_sender_base import MessageSenderBase
from .message_deduplicator import MessageDeduplicator

from ..utils.tbot_env import shared


class HTTPMessageSender(MessageSenderBase):
    def __init__(self, deduplicator: MessageDeduplicator):
        self.buffer: List[Dict] = (
            []
        )  # Buffer to store messages when HTTP POST fails
        self.deduplicator = deduplicator

    async def send_http_post(self, json_data: Dict) -> bool:
        """Send data via HTTP POST and handle errors. Returns True if successful."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    shared.http_server_addr, json=json_data
                ) as response:
                    if response.status == 200:
                        logger.success(
                            f"Successfully sent data via HTTP POST: {json_data}"
                        )
                        return True
                    else:
                        logger.error(
                            f"Failed to send HTTP POST. Status: {response.status}"
                        )
                        return False
        except Exception as e:
            logger.error(f"Error sending data via HTTP POST: {e}")
            return False

    async def send_message(self, json_data: Dict) -> bool:
        """Send the message if not a duplicate."""
        if self.deduplicator.is_duplicate(json_data):
            logger.warning(
                f"Duplicate message detected: {json_data['orderRef']} Skipping."
            )
            return False

        # Attempt to send the message
        if await self.send_http_post(json_data):
            self.deduplicator.store_message_hash(json_data)
        else:
            self.deduplicator.store_message_hash(json_data)
            logger.critical("Failed to send message, storing in buffer.")
            self.buffer.append(json_data)

        return True

    async def flush_buffer(self):
        """Attempt to resend buffered messages when the connection is restored."""
        if not self.buffer:
            return
        logger.critical("Attempting to resend buffered messages")
        logger.critical(
            f"Flushing {len(self.buffer)} messages from the buffer."
        )
        for message in self.buffer.copy():
            if await self.send_http_post(message):
                self.buffer.remove(message)
            else:
                logger.error(
                    "Failed to resend buffered message, stopping flush."
                )
                break  # Stop attempting if sending fails again
