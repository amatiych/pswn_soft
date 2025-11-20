from typing import Mapping,Any
from typing import List
from purple_swan.data.models.models import EntityType,Portfolio
from purple_swan.data.loader_registry import register_loader
from purple_swan.data.loaders.file_source_data_loader import SingleFiledDataLoaderS3


class S3PortfolioDataLoader(SingleFiledDataLoaderS3[Portfolio]):
    """
    Loads instruments from S3 and returns List[Instrument].
    """
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    @property
    def entity_type(self) -> EntityType:
        return EntityType.PORTFOLIO

    def get_column_map(self):
         return {'company_name':'name','cik':'cik'}

    def write(self, data: List[Portfolio]) -> None:
       pass



@register_loader("s3_portfolios_csv")
class S3PortfolioDataLoaderCSV(S3PortfolioDataLoader):
    def __init__(self,**kwargs):
        kwargs['file_format'] = "csv"
        super().__init__(**kwargs)

@register_loader("s3_portfolios_pq")
class S3PortfolioDataLoaderParquet(S3PortfolioDataLoader):
    def __init__(self,**kwargs):
        kwargs['file_format'] = "parquet"
        super().__init__(**kwargs)