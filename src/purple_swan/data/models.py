from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Dict, Generic, List, TypeVar, Tuple
from abc import ABC, abstractmethod
from pandas import DataFrame

from pandas import read_csv
from data_utils import df_to_dataclasses

class EntityType(str, Enum):
    INSTRUMENT = "instrument"
    PORTFOLIO = "portfolio"

T = TypeVar("T")

class DataLoader(ABC, Generic[T]):
    @property
    @abstractmethod
    def entity_type(self) -> EntityType:
        ...

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """E.g. 's3', 'dynamodb', 'sql', 'service'."""
        ...

    def filter(self, df, filters:dict=None):
        if not filters:
            return df
        for k, v in filters.items():
            if k in df.columns:
                idx = df[k] == v
                df = df[idx]
        return df


    @abstractmethod
    def load(self, flt: Dict[str, Any]) -> List[T]:
        pass

    @abstractmethod
    def write(self, data: List[T]):
        pass


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


class S3PQPortfolioDataLoader(DataLoader[Portfolio]):
    @property
    def entity_type(self) -> EntityType:
        return EntityType.PORTFOLIO

    def backend_name(self) -> str:
        return "s3"

    def load(self, filters: dict = None) -> List[Portfolio]:
        pass

    def write(self, data: List[Portfolio]):

        for p in data:
            port_name = p.name
            assets = p.assets
            url =  f"s3://pswn-test/portfolios/port_name={port_name}/portfolio.parquet"
            df = DataFrame(assets,columns=["asset","value"])
            df.to_parquet(url,compression="snappy")
            url =  f"s3://pswn-test/portfolios/port_name={port_name}/portfolio.csv"
            df.to_csv(url,index=False)

class S3StockDataLoader(DataLoader[Instrument]):
    @property
    def entity_type(self) -> EntityType:
        return EntityType.INSTRUMENT

    def backend_name(self) -> str:
        return "s3"

    def write(self, data: List[Portfolio]):
        pass

    def load(self, filters:dict=None) -> List[Instrument]:
        url = "s3://pswn-test/markat_data/import/stocks.csv"
        df = read_csv(url).rename(columns={"Symbol":"symbol","Security Name":"security_name"})
        df = self.filter(df,filters)
        instruments = df_to_dataclasses(df,Instrument)
        return instruments