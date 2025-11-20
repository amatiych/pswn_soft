from purple_swan.data.enrichment.enrichment import DataEnricher,EnrichmentContext
from purple_swan.data.models.models  import Position,Instrument
from typing import List
from pandas import DataFrame

class PositionInstrumentEnricher(DataEnricher[Position]):

    def can_enrich(self, data_type: type) -> bool:
        return data_type == Position

    def enrich(self, positions: List[Position], context: EnrichmentContext) -> List[Position]:
        instruments = context.cache.get("instruments", [])
        inst_by_ticker = {i.symbol : i for i in instruments}
        for pos in positions:
            instrument = inst_by_ticker.get(pos.ticker)
            if instrument:
                pos.instrument = instrument

class PositionTSMatrixEnricher(DataEnricher[Position]):

    def can_enrich(self, data_type: type) -> bool:
        return data_type == Position

    def enrich(self, positions: List[Position], context: EnrichmentContext) -> List[Position]:
        ts_data = context.cache.get("ts_matrix", [])[0].date
        tickers = ts_data.columns
        dates = ts_data['date'].values
        pos_ts_matrix = DataFrame(ts_data.index)
        for pos in positions:
            ticker = pos.ticker
            if ticker not in tickers:
                pos_ts_matrix[ticker] = 0
            else:
                pos_ts_matrix[ticker] = ts_data[ticker].values







