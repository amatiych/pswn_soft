from typing import Mapping,Any
from purple_swan.data.models.models import EntityType, List, Instrument
from purple_swan.data.loader_registry import register_loader
from purple_swan.data.loaders.file_source_data_loader import SingleFiledDataLoaderS3
@register_loader("s3_instruments")

class S3InstrumentsDataLoader(SingleFiledDataLoaderS3[Instrument]):
    """
    Loads instruments from S3 and returns List[Instrument].
    """
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    @property
    def entity_type(self) -> EntityType:
        return EntityType.INSTRUMENT

    def get_column_map(self):
         return {'Symbol':'symbol','Security Name':'security_name'}

    def write(self, data: List[Instrument]) -> None:
       pass



@register_loader("s3_instruments_csv")
class S3InstrumentsDataLoaderCSV(S3InstrumentsDataLoader):
    def __init__(self,**kwargs):
        kwargs['file_format'] = "csv"
        super().__init__(**kwargs)

@register_loader("s3_instruments_pq")
class S3InstrumentsDataLoaderParquet(S3InstrumentsDataLoader):
    def __init__(self,**kwargs):
        kwargs['file_format'] = "parquet"
        super().__init__(**kwargs)