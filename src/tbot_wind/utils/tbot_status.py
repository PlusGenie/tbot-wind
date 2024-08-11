import pandas as pd
import numpy as np
from enum import IntFlag
from loguru import logger


class TBOTStatus(IntFlag):
    NEW = 1
    NEW_SAVED = 2
    ALERT_COMPLETED = 4
    ALERT_GENERATED = 8
    RSI_COMPLETED = 16
    MA_COMPLETED = 32
    CALCULATION_COMPLETED = 64
    ERROR_DETECTED = 128
    CROSSOVER_DETECTED = 256
    CROSSUNDER_DETECTED = 512
    DATA_ANALYSIS_COMPLETED = 1024
    FINAL_ALERT_CONFIRMED = 2048
    NAN_DETECTED = 4096

    # User-defined indicators
    INDICATOR1_COMPLETED = 8192
    INDICATOR2_COMPLETED = 16384
    INDICATOR3_COMPLETED = 32768
    INDICATOR4_COMPLETED = 65536
    INDICATOR5_COMPLETED = 131072
    INDICATOR6_COMPLETED = 262144
    # Add more as needed

    def describe(self):
        descriptions = {
            self.NEW: "The newly arrived data, not saved into the database.",
            self.NEW_SAVED: "The newly arrived data, saved into the database.",
            self.ALERT_GENERATED: "Alert signal generated.",
            self.ALERT_SAVED: "Alert signal generated and saved into the database.",
            self.RSI_COMPLETED: "RSI calculation completed.",
            self.MA_COMPLETED: "Moving average calculation completed.",
            self.CALCULATION_COMPLETED: "General calculation completed.",
            self.ERROR_DETECTED: "A generic error or anomaly was detected.",
            self.CROSSOVER_DETECTED: "A crossover detected.",
            self.CROSSUNDER_DETECTED: "A crossunder detected.",
            self.DATA_ANALYSIS_COMPLETED: "Generic data analysis completed.",
            self.FINAL_ALERT_CONFIRMED: "Final buy/sell alert confirmed.",
            self.NAN_DETECTED: "NaN values detected in critical fields.",
            self.INDICATOR1_COMPLETED: "User-defined indicator 1 calculation completed.",
            self.INDICATOR2_COMPLETED: "User-defined indicator 2 calculation completed.",
            self.INDICATOR3_COMPLETED: "User-defined indicator 3 calculation completed.",
            self.INDICATOR4_COMPLETED: "User-defined indicator 4 calculation completed.",
            self.INDICATOR5_COMPLETED: "User-defined indicator 5 calculation completed.",
            self.INDICATOR6_COMPLETED: "User-defined indicator 6 calculation completed.",
            # Add more descriptions as needed
        }
        return descriptions.get(self, "Unknown status")

    @staticmethod
    def add_status(current_status, new_status) -> np.float64:
        """Add a status to the current status using a bitwise OR."""
        if pd.isna(current_status):
            logger.critical("NaN encountered in add_status")
            int_status = 0
        else:
            int_status = int(current_status)

        updated_status = int_status | new_status
        return np.float64(updated_status)

    @staticmethod
    def remove_status(current_status, status_to_remove) -> np.float64:
        """Remove a status from the current status using a bitwise AND NOT."""
        if pd.isna(current_status):
            logger.critical("NaN encountered in remove_status")
            int_status = 0
        else:
            int_status = int(current_status)

        updated_status = int_status & ~status_to_remove
        return np.float64(updated_status)

    @staticmethod
    def has_status(current_status, status_to_check) -> bool:
        """Check if a status is present using a bitwise AND."""
        if pd.isna(current_status):
            logger.critical("NaN encountered in has_status")
            int_status = 0
        else:
            int_status = int(current_status)

        return (int_status & status_to_check) == status_to_check
