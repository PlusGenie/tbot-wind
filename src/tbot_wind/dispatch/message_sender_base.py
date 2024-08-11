from abc import ABC, abstractmethod
from typing import Dict


class MessageSenderBase(ABC):
    @abstractmethod
    async def send_message(self, message: Dict) -> bool:
        """Abstract method to send a message."""
        pass
