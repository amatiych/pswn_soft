from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, TypeVar, Tuple,Mapping
from purple_swan.data.models.models import EntityType,T

class DataLoader(ABC, Generic[T]):
    @property
    @abstractmethod
    def entity_type(self) -> EntityType:
        ...

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """E.g. 's3', 'dynamodb', 'sql', 'service'."""
        ...

    def filter(self, df, filters:dict=None):
        if not filters:
            return df
        for k, v in filters.items():
            if k in df.columns:
                idx = df[k] == v
                df = df[idx]
        return df


    @abstractmethod
    def load(self, flt: Dict[str, Any]) -> List[T]:
        pass

    @abstractmethod
    def write(self, data: List[T]):
        pass

    @classmethod
    def from_config(
            cls,
            entity_type: EntityType,
            cfg: Mapping[str, Any],
    ) -> "DataLoader":
        """
        Default implementation: assume __init__ matches cfg keys.
        Override per concrete loader if needed.
        """
        # Ignore 'loader' key if present in cfg
        kwargs = {k: v for k, v in cfg.items() if k != "loader"}
        obj = cls(**kwargs)  # type: ignore[arg-type]
        obj.entity_type = entity_type
        return obj