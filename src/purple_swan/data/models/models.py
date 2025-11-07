from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Dict, Generic, List, TypeVar, Tuple


class EntityType(str, Enum):
    INSTRUMENT = "instrument"
    PORTFOLIO = "portfolio"

T = TypeVar("T")



@dataclass
class Instrument:
    symbol: str
    security_name: str
    #asset_class: str
    #currency: str = "USD"
    #multiplier: float = 1.0

@dataclass
class Portfolio:
    name: str
    assets: List[Tuple[Instrument, float]]

