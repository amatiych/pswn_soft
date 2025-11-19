from purple_swan.data.enrichment import DataEnricher,EnrichmentContext
from purple_swan.data.models.models  import Position,Instrument
from typing import List

class PositionEnricher(DataEnricher[Position]):

    def can_enrich(self, data_type: type) -> bool:
        return data_type == Position

    def enrich(self, positions: List[Position], context: EnrichmentContext) -> List[Position]:
        instruments = context.cache.get("instruments", [])
        inst_by_ticker = {i.symbol : i for i in instruments}
        for pos in positions:
            instrument = inst_by_ticker.get(pos.ticker)
            if instrument:
                pos.instrument = instrument


