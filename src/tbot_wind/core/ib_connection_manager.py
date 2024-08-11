import asyncio
from ib_async import IB, util
from loguru import logger

from ..utils.tbot_env import shared


class IBConnectionManager:
    def __init__(self):
        self.ib = IB()
        self.host = shared.ibkr_addr
        self.port = shared.ibkr_port
        self.client_id = shared.wind_client_id

    async def connect(self):
        """Attempt to connect to the Interactive Brokers API with retry logic."""
        if self.ib.isConnected():
            logger.debug("Already connected to IB Gateway.")
            return
        while True:
            try:
                logger.info(
                    f"Attempting to connect to IB Gateway: {self.host}:{self.port}"
                )
                await self.ib.connectAsync(
                    self.host, self.port, clientId=self.client_id
                )
                if self.ib.isConnected():
                    util.logToConsole(shared.ib_loglevel)
                    logger.info(
                        f"Connected to IB Gateway with Client ID: {self.client_id}"
                    )
                    break
                else:
                    raise Exception(
                        f"Failed to connect with Client ID: {self.client_id}"
                    )
            except asyncio.TimeoutError:
                logger.critical(
                    "API connection timed out. Retrying in 10 seconds..."
                )
                await asyncio.sleep(10)  # Wait for 10 seconds before retrying
            except KeyboardInterrupt:
                logger.info(
                    "KeyboardInterrupt received. Shutting down gracefully..."
                )
                self.disconnect()
                raise
            except Exception as e:
                logger.critical(f"API connection failed: {e}")
                logger.info("Retrying connection in 10 seconds...")
                await asyncio.sleep(10)

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()
            logger.info("Disconnected from IB Gateway.")
