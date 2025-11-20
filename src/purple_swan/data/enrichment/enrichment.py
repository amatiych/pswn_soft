from dataclasses import dataclass
from typing import Dict, List, Generic, TypeVar, Callable, Any
from abc import ABC, abstractmethod


T = TypeVar("T")

@dataclass
class EnrichmentContext:
    """Shared context during multi-step enrichment"""
    cache: Dict[str, Any] = None

    def __post_init__(self):
        if self.cache is None:
            self.cache = {}


class DataEnricher(ABC, Generic[T]):
    """
    Enrichers transform data loaded by one loader using data from another.
    Examples: enriching Positions with Instrument details, adding time series.
    """

    @abstractmethod
    def enrich(self, data: List[T], context: EnrichmentContext) -> List[T]:
        """Transform data using context (which may contain other loaded data)"""
        pass

    @abstractmethod
    def can_enrich(self, data_type: type) -> bool:
        """Check if this enricher applies to this data type"""
        pass
