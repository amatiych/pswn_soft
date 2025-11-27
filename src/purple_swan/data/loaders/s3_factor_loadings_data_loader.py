from typing import List, Mapping, Any, Dict
from purple_swan.data.models.models import FactorModel, EntityType
from purple_swan.data.loader_registry import register_loader
from purple_swan.data.loaders.file_source_data_loader import SingleFiledDataLoaderS3, SingleFiledDataLoader, \
    S3DataLoader


class S3FactorModelDataLoader(S3DataLoader[FactorModel]):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load(self, filters: Mapping[str, Any] | None = None) -> List[FactorModel]:
        """
        Load the entire time series matrix as a single object.
        Returns a list with one TimeSeriesMatrix element for interface compatibility.
        """
        url = f"s3://{self.bucket_name}/{self.key}.{self.file_format}"
        df = self.read_func(url)

        # Set first column as index if it's the date column
        if df.columns[0].lower() == 'ticker':
            df = df.set_index(df.columns[0])

        return [FactorModel(data=df)]

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



