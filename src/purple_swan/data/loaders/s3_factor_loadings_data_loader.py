from typing import List, Mapping, Any
from purple_swan.data.models.models import FactorModel, EntityType
from purple_swan.data.loader_registry import register_loader
from purple_swan.data.loaders.file_source_data_loader import SingleFiledDataLoaderS3, SingleFiledDataLoader


class S3FactorModelDataLoader(SingleFiledDataLoaderS3[FactorModel]):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def write(self, data: List[FactorModel]) -> None:
       pass

    @property
    def entity_type(self) -> EntityType:
        return EntityType.FACTOR_MODEL

@register_loader("s3_factor_model_csv")
class S3FactorModelDataLoaderCSV(S3FactorModelDataLoader):
    def __init__(self,**kwargs):
        kwargs['file_format'] = "csv"
        super().__init__(**kwargs)

@register_loader("s3_factor_model_pq")
class S3FactorModelDataLoaderPQ(S3FactorModelDataLoader):
    def __init__(self,**kwargs):
        kwargs['file_format'] = "parquet"
        super().__init__(**kwargs)



