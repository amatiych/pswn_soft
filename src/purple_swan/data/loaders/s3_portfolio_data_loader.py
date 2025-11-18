import os
from pandas import read_parquet, concat, DataFrame, read_csv
from purple_swan.data.loaders.file_source_data_loader import S3DataLoader
from purple_swan.data.models.models import Portfolio, EntityType, List
from purple_swan.core.aws_utils import list_s3_files
from purple_swan.data.loader_registry import register_loader
from typing import List,Mapping, Any

class S3PortfolioDataLoader(S3DataLoader[Portfolio]):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        env_name = os.environ.get('environment')
        self.bucket_name = kwargs["bucket"]
        self.key = kwargs["key"]
        self.init_funcs()

    def init_funcs(self):
        self.file_format = "parquet"
        self.read_func = read_parquet
        self.write_func = DataFrame.to_parquet

    @property
    def entity_type(self) -> EntityType:
        return EntityType.PORTFOLIO

    def backend_name(self) -> str:
        return "s3"

    # def load(self, filters: dict = None) -> List[Portfolio]:
    #     all_files = list_s3_files(self.bucket_name,prefix='portfolios')
    #     port_name_folders = [s.split('/')[1] for s in all_files]
    #     port_names = list(set([p.split("=")[-1] for p in port_name_folders]))
    #     if filters and 'portfolios' in filters:
    #         port_list = filters['portfolios'].split(',')
    #         port_names = [p for p in port_names if p in port_list ]
    #     dfs = []
    #     for p in port_names:
    #         file = f"portfolios/port_name={p}/portfolio.{self.file_format}"
    #         url = f"s3://{self.bucket_name}/{file}"
    #         print (f"reading portfolio: {file}")
    #         try:
    #             dfs.append(self.read_func(url))
    #         except Exception as e:
    #             print(f"WARNING: could not read {url}")
    #     return(concat(dfs))


    def write(self, data: List[Portfolio]):
        for p in data:
            port_name = p.name
            assets = p.assets
            url = f"s3://{self.bucket_name}/portfolios/port_name={port_name}/portfolio.{self.file_format}"
            df = DataFrame(assets, columns=["asset", "value"])
            self.write_fun(url, df)

    @classmethod
    def from_config(cls, cfg: Mapping[str, Any]) -> "S3PortfolioDataLoader":
        """
        Config example (YAML):

            instrument:
              loader: s3_instruments
              bucket: pawn-{env}
              key: instruments/latest.parquet
              region: us-east-1
        """
        return cls(
            bucket=cfg["bucket"],
            key=cfg["key"],
            region=cfg.get("region"),
        )
@register_loader("s3_portfolios")
class S3PortfolioDataLoaderParquet (S3PortfolioDataLoader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def file_tye(self) -> str:
        return "parquet"


class S3PortfolioDataLoaderCsv (S3PortfolioDataLoader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


