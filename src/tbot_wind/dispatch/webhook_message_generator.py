from typing import Dict
import numpy as np
from loguru import logger
import os
from hashlib import md5

from ..utils.tbot_env import shared

# Load environment variables
UNIQUE_KEY = os.environ.get("TVWB_UNIQUE_KEY", "").strip()
TBOT_TVWB_EVENT = "WebhookReceived"


class WebhookMessageGenerator:
    VALID_DIRECTIONS = {
        "strategy.entrylong",
        "strategy.entryshort",
        "strategy.exitlong",
        "strategy.exitshort",
        "strategy.cancellong",
        "strategy.cancelshort",
        "strategy.cancel_all",
        "strategy.close",
        "strategy.close_all",
    }

    VALID_CONTRACT_TYPES = {"stock", "forex", "crypto", "future"}
    VALID_TIMEFRAMES = {
        "1S",
        "5S",
        "10S",
        "15S",
        "30S",
        "1",
        "5",
        "15",
        "30",
        "60",
        "120",
        "240",
        "1D",
        "1W",
        "1M",
    }

    def __init__(self):
        event_and_key = TBOT_TVWB_EVENT + UNIQUE_KEY
        hashed_value = md5(event_and_key.encode()).hexdigest()[:6]
        self.webhook_key = f"{TBOT_TVWB_EVENT}:{hashed_value}"
        self.client_id = shared.tbot_client_id

    def _metric_to_json(self, name, value):
        """Convert a metric name and value to a JSON formatted dictionary."""
        return {"name": str(name), "value": float(value)}

    def validate_value(self, value, value_name, allowed_values=None):
        """Helper function to validate a value."""
        if not value or (allowed_values and value not in allowed_values):
            raise ValueError(f"Invalid {value_name}: {value}")

    def validate_currency(self, currency):
        """Validate the currency format."""
        if (
            not isinstance(currency, str)
            or not currency.isalpha()
            or len(currency) != 3
        ):
            raise ValueError(
                f"Invalid currency: {currency}. Must be a valid ISO 4217 currency code like 'USD', 'GBP'."
            )

    def validate_direction(self, direction: str):
        """Validate the direction parameter."""
        self.validate_value(direction, "direction", self.VALID_DIRECTIONS)

    def validate_contract_type(self, contract: str):
        """Validate the contract type."""
        self.validate_value(
            contract, "contract type", self.VALID_CONTRACT_TYPES
        )

    def validate_timeframe(self, timeframe: str):
        """Validate the timeframe."""
        self.validate_value(timeframe, "timeframe", self.VALID_TIMEFRAMES)

    def validate_qty(self, qty):
        """Validate quantity is not zero."""
        if qty == 0:
            raise ValueError("Quantity (qty) should not be 0.")

    def make_webhook_json(
        self,
        timestamp,
        direction,
        ticker,
        timeframe,
        contract,
        currency,
        qty=-1e10,
        entry_limit=0.0,
        entry_stop=0.0,
        exit_limit=0.0,
        exit_stop=0.0,
        lastTradeDateOrContractMonth="",
        exchange="",
        multiplier="",
        orderRef="",
    ) -> Dict:
        """
        Create a JSON message for webhook. This method is only for development purposes.
        """
        # Perform validations
        self.validate_direction(direction)
        self.validate_contract_type(contract)
        self.validate_qty(qty)
        self.validate_value(timestamp, "timestamp", allowed_values=None)
        self.validate_value(ticker, "ticker", allowed_values=None)
        self.validate_value(orderRef, "orderRef", allowed_values=None)
        self.validate_timeframe(timeframe)
        self.validate_currency(currency)

        # Validate exit strategy conditions for exitlong and exitshort
        if direction in {"strategy.exitlong", "strategy.exitshort"}:
            conditions = np.array(
                [entry_limit, entry_stop, exit_limit, exit_stop]
            )
            vec = np.where(conditions > 0, 1, 0)
            if not (
                (vec == np.array([0, 0, 1, 1])).all()
                or (vec == np.array([0, 0, 1, 0])).all()
                or (vec == np.array([0, 0, 0, 1])).all()
            ):
                err_msg = (
                    f"entry_limit:{entry_limit}, entry_stop:{entry_stop}, "
                    f"exit_limit:{exit_limit}, exit_stop:{exit_stop}"
                )
                raise ValueError(
                    f"Invalid exit strategy parameters: {err_msg}"
                )

        # Create metrics list
        metrics = [
            self._metric_to_json("entry.stop", entry_stop),
            self._metric_to_json("entry.limit", entry_limit),
            self._metric_to_json("exit.limit", exit_limit),
            self._metric_to_json("exit.stop", exit_stop),
            self._metric_to_json("qty", qty),
        ]

        def safe_int(value, default=0):
            """Convert a value to an integer safely using base 10, with a default if conversion fails."""
            try:
                if isinstance(value, str) and not value.strip():
                    return default
                return int(value, 10)
            except (TypeError, ValueError):
                return default

        # Construct the message
        message = {
            "timestamp": timestamp,
            "ticker": ticker,
            "timeframe": timeframe,
            "key": self.webhook_key,
            "currency": currency,
            "clientId": safe_int(self.client_id),
            "orderRef": orderRef,
            "contract": contract,
            "direction": direction,
            "metrics": metrics,
            "lastTradeDateOrContractMonth": lastTradeDateOrContractMonth,
            "exchange": exchange,
            "multiplier": multiplier,
        }

        # Check all required fields are present
        required_fields = [
            "direction",
            "key",
            "metrics",
            "orderRef",
            "contract",
            "ticker",
            "timeframe",
            "currency",
            "timestamp",
        ]

        missing_fields = [
            field for field in required_fields if not message.get(field)
        ]
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")

        logger.trace(f"message: {message}")
        return message
