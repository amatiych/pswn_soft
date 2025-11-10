from abc import ABC, abstractmethod
from typing import  List
from dataclasses import dataclass
from ..data_utils import df_to_dataclasses

@dataclass
class TimePoint:
    date: int # eg 20251001
    value: float

class TimeSeriesProvider(ABC):
    @abstractmethod
    def get_time_series(self,symbol : str, start_date: int, end_date: int) -> List[TimePoint]:
        pass


class YahooTimeSeriesProvider(TimeSeriesProvider):
    def get_time_series(self, symbol : str, start_date: str, end_date: str) -> List[TimePoint]:
        import yfinance as yf
        df = yf.download(symbol, start=start_date, end=end_date)
        df = df['Close']
        df['date'] = df.index.map(lambda x: x.strftime('%Y%m%d'))
        df.rename(columns={symbol:'value'},inplace=True)
        df = df[['date','value']]
        points = df_to_dataclasses(df,TimePoint)
        return points


