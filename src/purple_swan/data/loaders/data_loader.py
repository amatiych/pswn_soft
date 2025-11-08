from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, Mapping,TypeVar
from purple_swan.data.models.models import EntityType
T_co = TypeVar("T_co",covariant=True)

class DataLoader(ABC, Generic[T_co]):
    """
    Base interface for all data loaders.
    Subclasses:
      - set `entity_type` as a class attribute
      - specialize T_co (e.g. Instrument, Portfolio, Position)
    """

    entity_type: EntityType  # subclasses set this

    @classmethod
    @abstractmethod
    def from_config(cls, cfg: Mapping[str, Any]) -> "DataLoader[T_co]":
        """
        Build an instance from a config dict.
        The factory will call this using YAML config.
        """
        raise NotImplementedError

    @abstractmethod
    def load(self, filters: Mapping[str, Any] | None = None) -> List[T_co]:
        """
        Load a list of domain objects of type T_co.
        """
        raise NotImplementedError

    @abstractmethod
    def write(self, data: List[T_co]) -> None:
        """
        Persist a list of domain objects.
        """
        raise NotImplementedError
