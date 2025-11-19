from pandas import DataFrame
from dataclasses import dataclass
from enum import Enum
from typing import TypeVar, Optional
T  = TypeVar("T",covariant=False)

class EntityType(str, Enum):
    INSTRUMENT = "instrument"
    PORTFOLIO = "portfolio",
    POSITION = "position",
    TS_MATRIX = "ts_matrix",

@dataclass
class Instrument:
    symbol: str
    security_name: str
    # asset_class: Optional[str]
    # currency: Optional[str] = "USD"
    # multiplier: Optional[float] = 1.0

@dataclass
class Position:
    ticker : str
    weight: float
    shares: Optional[float] = None
    price: Optional[float] = None

@dataclass
class TSMatrix:
    data : DataFrame

    @property
    def dates(self):
        return self.data.index

    @property
    def tickers(self):
        return self.data.columns.tolist()


# @dataclass
# class PortfolioOld:
#     name: str
#     assets: List[Position]

