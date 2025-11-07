import os
from pandas import read_parquet, concat, DataFrame, read_csv
from purple_swan.data.models.data_loader import DataLoader
from purple_swan.data.models.models import Portfolio, EntityType, List, Instrument
from purple_swan.core.aws_utils import list_s3_files
from purple_swan.data.loader_registry import register_loader



@register_loader("s3_portfolios")
class S3PortfolioDataLoader(DataLoader[Portfolio]):

    def __init__(self, **kwargs):
        env_name = os.environ.get('environment')
        self.bucket_name = f"pswn-{env_name}"
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

    def load(self, filters: dict = None) -> List[Portfolio]:
        all_files = list_s3_files(self.bucket_name,prefix='portfolios')
        port_name_folders = [s.split('/')[1] for s in all_files]
        port_names = [p.split("=")[-1] for p in port_name_folders]
        if filters and 'portfolios' in filters:
            port_list = filters['portfolios'].split(',')
            port_names = [p for p in port_names if p in port_list ]
        dfs = []
        for p in port_names:
            file = f"portfolios/name={p}/portfolio.{self.file_format}"
            url = f"s3://{self.bucket_name}/{file}"
            print (f"reading {file}")
            dfs.append(self.read_fun(url))
        return(concat(dfs))


    def write(self, data: List[Portfolio]):
        for p in data:
            port_name = p.name
            assets = p.assets
            url = f"s3://{self.bucket_name}/portfolios/port_name={port_name}/portfolio.{self.file_format}"
            df = DataFrame(assets, columns=["asset", "value"])
            self.write_fun(url, df)



class S3PortfolioDataLoaderParquet (S3PortfolioDataLoader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

class S3PortfolioDataLoaderCsv (S3PortfolioDataLoader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def init_funcs(self):
        self.file_format = "csv"
        self.read_func = read_csv
        self.write_func = DataFrame.to_csv

