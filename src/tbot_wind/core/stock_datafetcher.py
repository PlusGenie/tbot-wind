from abc import ABC, abstractmethod
import pandas as pd

from ..utils.objects import OrderTV


class StockDataFetcher(ABC):
    @abstractmethod
    async def fetch_realtime_data(
        self, order: OrderTV, duration: str, end_date: str
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    async def retrieve_data_in_chunks(
        self, order: OrderTV, total_duration: str, end_date: str
    ) -> pd.DataFrame:
        pass
