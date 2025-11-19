import os
from pandas import  DataFrame
from purple_swan.data.loaders.file_source_data_loader import S3DataLoader
from purple_swan.data.models.models import Position, EntityType
from purple_swan.data.loader_registry import register_loader
from typing import List,Mapping, Any


class S3PositionDataLoader(S3DataLoader[Position]):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def entity_type(self) -> EntityType:
        return EntityType.POSITION

    def backend_name(self) -> str:
        return "s3"




    def write(self, data: List[Position]):
        for p in data:
            port_name = p.name
            assets = p.assets
            url = f"s3://{self.bucket_name}/portfolios/port_name={port_name}/portfolio.{self.file_format}"
            df = DataFrame(assets, columns=["asset", "value"])
            self.write_fun(url, df)


@register_loader("s3_positions_pq")
class S3PositionDataLoaderParquet (S3PositionDataLoader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def file_tye(self) -> str:
        return "parquet"

@register_loader("s3_positions_csv")
class S3PositionDataLoaderCsv (S3PositionDataLoader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


