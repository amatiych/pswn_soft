from typing import List, Dict, Type, TypeVar, Optional
from dataclasses import dataclass
import pandas as pd
from purple_swan.data.data_factory import DataFactory
from purple_swan.data.models.models import EntityType, Position, Instrument, T, Portfolio, FactorModel
from purple_swan.data.enrichment.enrichment import DataEnricher, EnrichmentContext

@dataclass
class Environment:
    """
    Assembled environment data with linked relationships.
    This is what your analytics layer gets.
    """
    portfolios: List[Portfolio]
    positions: List[Position]
    instruments: List[Instrument]
    position_df: pd.DataFrame  # Denormalized for analysis
    instrument_df: pd.DataFrame
    factor_models: Optional[List[FactorModel]]
    time_series: Optional[pd.DataFrame] = None  # Optional: daily prices


class EnvironmentRepository:
    """
    High-level repository that orchestrates multiple loaders to build
    complete portfolio datasets. Acts as a facade over DataFactory.

    Philosophy:
    - Loaders handle "what" (where/how to get data)
    - Repository handles "how to assemble" (linking & enrichment)
    - Business logic is explicit and isolated
    """

    def __init__(self, factory: DataFactory):
        self.factory = factory
        self.enrichers: Dict[type, List[DataEnricher]] = {}
        self._time_series_provider = None

    def register_enricher(self, enricher: DataEnricher, data_type: Type):
        """Register enrichers for specific data types"""
        if data_type not in self.enrichers:
            self.enrichers[data_type] = []
        self.enrichers[data_type].append(enricher)

    def set_time_series_provider(self, provider):
        """Optional: add time series capability"""
        self._time_series_provider = provider

    def load_portfolio_data(
            self,
            position_filters: Optional[Dict] = None,
            include_time_series: bool = False
    ) -> Environment:
        """
        Orchestrates loading of positions, instruments, and optional time series,
        then links them together.
        """
        # Step 1: Load core data
        positions = self.factory.get_data(EntityType.POSITION, position_filters)
        instruments = self.factory.get_data(EntityType.INSTRUMENT)
        ts_matrix = self.factory.get_data(EntityType.TS_MATRIX)
        portfolios = self.factory.get_data(EntityType.PORTFOLIO,position_filters)
        factor_models = self.factory.get_data(EntityType.FACTOR_MODEL,{})
        # Step 2: Build context (cached data for enrichers)
        context = EnrichmentContext(
            cache={
                'instruments': instruments,
                'positions': positions,
                'ts_matrix': ts_matrix,
                'portfolios': portfolios,
                'factor_models': factor_models
            }
        )

        # Step 3: Enrich positions with instrument details
        enriched_positions = self._enrich_data(
            positions,
            Position,
            context
        )

        enriched_portfolios = self._enrich_data(portfolios, Portfolio, context)

        # Step 4: Create denormalized DataFrames for analysis
        # position_df = self._build_position_dataframe(
        #     enriched_positions,
        #     instruments
        # )

        # instrument_df = pd.DataFrame([vars(i) for i in instruments])
        #
        # # Step 5: Optional: load time series
        # time_series = None
        # if include_time_series and self._time_series_provider:
        #     tickers = [i.symbol for i in instruments]
        #     time_series = self._load_time_series(tickers)
        #     context.cache['time_series'] = time_series

        return Environment(
            portfolios=enriched_portfolios,
            positions=enriched_positions,
            instruments=instruments,
             position_df=None,
             instrument_df=None,
            factor_models=factor_models,
            time_series=ts_matrix,

        )

    def _enrich_data(
            self,
            data: List[T],
            data_type: Type[T],
            context: EnrichmentContext
    ) -> List[T]:
        """Apply all registered enrichers for this data type"""
        enriched = data
        for enricher in self.enrichers.get(data_type, []):
            if enricher.can_enrich(data_type):
                enriched = enricher.enrich(enriched, context)
        return enriched

    # def _build_position_dataframe(
    #         self,
    #         positions: List[Position],
    #         instruments: List[Instrument]
    # ) -> pd.DataFrame:
    #     """Create denormalized view with position + instrument details"""
    #     pos_data = [vars(p) for p in positions]
    #     pos_df = pd.DataFrame(pos_data)
    #
    #     # Left join instruments
    #     inst_df = pd.DataFrame([vars(i) for i in instruments])
    #     inst_df = inst_df.rename(columns={'symbol': 'ticker'})
    #
    #     merged = pos_df.merge(inst_df, on='ticker', how='left')
    #     return merged

    # def _load_time_series(self, tickers: List[str]) -> pd.DataFrame:
    #     """Load price data for tickers"""
    #     if not self._time_series_provider:
    #         return None
    #     # Implementation depends on your time series provider
    #     return self._time_series_provider.get_multiple(tickers)