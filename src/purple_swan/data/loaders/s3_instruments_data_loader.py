from pandas import read_csv
from typing import Mapping,Any
from purple_swan.data.models.models import EntityType, List, Instrument
from purple_swan.data.loader_registry import register_loader
from purple_swan.data.loaders.s3_base import S3DataLoaderBase

@register_loader("s3_instruments")
class S3InstrumentsDataLoader(S3DataLoaderBase[Instrument]):
    """
    Loads instruments from S3 and returns List[Instrument].
    """

    entity_type = EntityType.INSTRUMENT

    def __init__(self, bucket: str, key: str, region: str | None = None):
        super().__init__(region=region)
        self.bucket = self._resolve_bucket(bucket)
        self.key = key

    @classmethod
    def from_config(cls, cfg: Mapping[str, Any]) -> "S3InstrumentsDataLoader":
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

    def load(self, filters: Mapping[str, Any] | None = None) -> List[Instrument]:
        s3 = self._get_s3_client()
        obj = s3.get_object(Bucket=self.bucket, Key=self.key)
        # Adjust to your actual format: parquet, csv, etc.
        return read_csv(obj["Body"])
        # df = read_parquet(obj["Body"])
        # # You can apply filters here if you want.
        # return [
        #     Instrument(
        #         id=str(row["id"]),
        #         symbol=row["symbol"],
        #         name=row["name"],
        #         currency=row["currency"],
        #         asset_class=row["asset_class"],
        #     )
        #     for _, row in df.iterrows()
        # ]

    def write(self, data: List[Instrument]) -> None:
        # Example: convert dataclasses back to DataFrame and upload.
        s3 = self._get_s3_client()
        df = pd.DataFrame([
            {
                "id": inst.id,
                "symbol": inst.symbol,
                "name": inst.name,
                "currency": inst.currency,
                "asset_class": inst.asset_class,
            }
            for inst in data
        ])
        body = df.to_parquet(index=False)
        s3.put_object(Bucket=self.bucket, Key=self.key, Body=body)