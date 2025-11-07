from pandas import read_csv
from purple_swan.data.models.data_loader import DataLoader
from purple_swan.data.models.models import Portfolio, EntityType, List, Instrument
from purple_swan.data.data_utils import df_to_dataclasses
from purple_swan.data.loader_registry import register_loader


@register_loader("s3_instruments")
class S3InstrumentsDataLoader(DataLoader[Instrument]):
    @property
    def entity_type(self) -> EntityType:
        return EntityType.INSTRUMENT

    def backend_name(self) -> str:
        return "s3"

    def write(self, data: List[Portfolio]):
        pass

    def load(self, filters: dict = None) -> List[Instrument]:
        url = "s3://pswn-test/markat_data/import/stocks.csv"
        df = read_csv(url).rename(columns={"Symbol": "symbol", "Security Name": "security_name"})
        df = self.filter(df, filters)
        instruments = df_to_dataclasses(df, Instrument)
        return instruments