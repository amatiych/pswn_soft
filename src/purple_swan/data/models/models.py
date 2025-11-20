from pandas import DataFrame
from dataclasses import dataclass
from enum import Enum
from typing import TypeVar, Optional, List
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
    cik: int
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


@dataclass
class Portfolio:

    cik: str
    name: str

    def __post_init__(self):
        self._positions: List[Position] = []
        self._ts_matrix: TSMatrix = None

    @property
    def positions(self) -> List[Position]:
        return self._positions

    @positions.setter
    def positions(self,value:List[Position]):
        self._positions = value

    @property
    def ts_matrix(self):
        return self._ts_matrix

    @ts_matrix.setter
    def ts_matrix(self,value:TSMatrix):
        self._ts_matrix = value