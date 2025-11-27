from os import environ
from pandas import concat,read_csv,read_parquet,DataFrame
from abc import  abstractmethod
from purple_swan.core.aws_utils import list_s3_files
from purple_swan.data.data_utils import df_to_dataclasses
from purple_swan.data.loaders.data_loader import DataLoader, T
from typing import Any, Generic, List, Mapping

from purple_swan.data.models.models import Position

FILE_TYPE_FUNCS = {
    "parquet": [read_parquet,DataFrame.to_parquet],
    "csv": [read_csv,DataFrame.to_csv]
}

class SingleFiledDataLoader(DataLoader, Generic[T]):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.file_format = kwargs.get("file_format", "csv")
        self.key = kwargs.get("key")
        self.read_func,self.write_func = FILE_TYPE_FUNCS[self.file_format]


    def post_load(self,res : List[T],data):
        pass

    def url_prefix(self):
        return "file"

    def get_url(self):
        return f"{self.url_prefix()}://{self.key}.{self.file_format}"

    def load(self,filters = {}):
        data = self.read_func(self.get_url())
        data.rename(columns=self.get_column_map(),inplace=True)
        columns = data.columns
        if filters:
            for k,v in filters.items():
                if k in columns:
                    idx = data[k].apply(str).isin(v)
                    data = data[idx]

        res =  df_to_dataclasses(data,T)
        self.post_load(res,data)
        return res

class SingleFiledDataLoaderS3(SingleFiledDataLoader, Generic[T]):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.key = kwargs["key"]
        self.bucket_name = kwargs["bucket"]

    def get_url(self):
        return f"{self.url_prefix()}://{self.bucket_name}/{self.key}.{self.file_format}"

    def url_prefix(self):
        return "s3"

    @classmethod
    def from_config(cls, cfg: Mapping[str, Any]) -> "S3PositionDataLoader":
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
            key=cfg["key"]
        )

class FileSourceDataLoader(DataLoader, Generic[T]):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.file_format = kwargs.get("file_format", "csv")
        self.read_func, self.write_func = FILE_TYPE_FUNCS[self.file_format]

    @abstractmethod
    def list_items(self, filters: Mapping[str, Any]):
        pass

    def url_prefix(self):
        return 'file'

    def get_url(self,file):
        return f"{self.url_prefix()}://{file}"

    def filter_items(self, items, filters: Mapping[str, Any]):

        if filters is None:
            return [(item,{}) for item in items]

        def get_dict(item):
            parts = item.split("/")
            d = {}
            for p in parts:
                if "=" in p:
                    k, v = p.split("=")
                    d[k] = str(v)
            return d

        def match(item_tup, filters):
            item, d = item_tup
            for k in filters:
                if k not in d:
                    return False
                if d[k] not in filters[k]:
                    return False
            return True

        item_pairs = [(i, get_dict(i)) for i in items]
        items = [(i,d) for (i, d) in item_pairs if match((i, d), filters)]
        return items

    def load(self, filters: dict = None) -> List[T]:
        all_files = self.list_items(filters)
        # list_s3_files(self.bucket_name,prefix='portfolios'))
        files = self.filter_items(all_files, filters)
        dfs = []

        for file,d in files:
            #file = f"portfolios/port_name={p}/portfolio.{self.file_format}"
            #url = f"{self.url_prefix()}://{self.bucket_name}/{file}"
            url = self.get_url(file)
            print(f"reading portfolio: {file}")
            try:
                df = self.read_func(url)
                for k,v in d.items():
                    df[k] = v
                dfs.append(df)
            except Exception as e:
                print(f"WARNING: could not read {url}")
        final_df = (concat(dfs))
        res = df_to_dataclasses(final_df, T)
        return res

class S3DataLoader(FileSourceDataLoader, Generic[T]):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.key = kwargs["key"]
        self.bucket_name = kwargs["bucket"]

    def url_prefix(self):
        return 's3'

    def get_url(self,file):
        return f"{self.url_prefix()}://{self.bucket_name}/{file}"

    def list_items(self, filters: Mapping[str, Any]):
        files = list_s3_files(self.bucket_name,prefix=self.key)
        files = [f for f in files if f.endswith(self.file_format)]
        return files

    @classmethod
    def from_config(cls, cfg: Mapping[str, Any]) -> "S3PositionDataLoader":
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
            key=cfg["key"]
        )
