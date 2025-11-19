
from purple_swan.data.models.models import EntityType, List, Instrument, TSMatrix
from purple_swan.data.loader_registry import register_loader
from purple_swan.data.loaders.file_source_data_loader import SingleFiledDataLoaderS3, S3DataLoader
from purple_swan.data.loaders.file_source_data_loader import FILE_TYPE_FUNCS
from typing import  Mapping,Any

@register_loader("ts_matrix_pq")
class S3TSMatricDataLoader(S3DataLoader[TSMatrix]):

    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.bucket_name = kwargs["bucket"]
        self.key = kwargs["key"]
        self.file_format = "parquet"
        self.read_func, self.write_func = FILE_TYPE_FUNCS["parquet"]

    @property
    def entity_type(self) -> EntityType:
        return EntityType.TS_MATRIX

    def load(self, filters: Mapping[str, Any] | None = None) -> List[TSMatrix]:
        """
        Load the entire time series matrix as a single object.
        Returns a list with one TimeSeriesMatrix element for interface compatibility.
        """
        url = f"s3://{self.bucket_name}/{self.key}.{self.file_format}"
        df = self.read_func(url)

        # Set first column as index if it's the date column
        if df.columns[0].lower() == 'date':
            df = df.set_index(df.columns[0])

        return [TSMatrix(data=df)]

    @classmethod
    def from_config(cls, cfg: Mapping[str, Any]) -> "S3TSMatricDataLoader":
        return cls(
            bucket=cfg["bucket"],
            key=cfg["key"]
        )

    def write(self, data: List[TSMatrix]) -> None:
        if not data:
            return

        ts_matrix = data[0]
        url = f"s3://{self.bucket_name}/{self.key}.{self.file_format}"
        ts_matrix.data.to_parquet(url, compression='snappy')


@register_loader("ts_matrix_csv")
class S3TSMatricDataLoaderCSV(S3TSMatricDataLoader):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        self.file_format = "csv"
        self.bucket_name = kwargs["bucket"]
        self.key = kwargs["key"]
        self.read_func, self.write_func = FILE_TYPE_FUNCS["parquet"]
