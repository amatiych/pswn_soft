from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Dict, Generic, List, TypeVar, Tuple
T  = TypeVar("T",covariant=False)

class EntityType(str, Enum):
    INSTRUMENT = "instrument"
    PORTFOLIO = "portfolio",
    POSITION = "position"

@dataclass
class Instrument:
    symbol: str
    security_name: str
    #asset_class: str
    #currency: str = "USD"
    #multiplier: float = 1.0

@dataclass
class Position:
    ticker : str
    weight: float
    shares: float = None
    price: float = None


# @dataclass
# class PortfolioOld:
#     name: str
#     assets: List[Position]

