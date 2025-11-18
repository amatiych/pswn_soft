from abc import ABC, abstractmethod
from typing import Any, Generic, List, Mapping,TypeVar
from purple_swan.data.models.models import EntityType, T

class DataLoader(ABC, Generic[T]):
    """
    Base interface for all data loaders.
    Subclasses:
      - set `entity_type` as a class attribute
      - specialize T_co (e.g. Instrument, Portfolio, Position)
    """

    def __init__(self, **kwargs):
        # ABC.__init__ accepts kwargs but doesn't use them, so this is safe
        pass

    entity_type: EntityType  # subclasses set this

    @classmethod
    @abstractmethod
    def from_config(cls, cfg: Mapping[str, Any]) -> "DataLoader[T]":
        """
        Build an instance from a config dict.
        The factory will call this using YAML config.
        """
        raise NotImplementedError


    @abstractmethod
    def load(self, filters: Mapping[str, Any] | None = None) -> List[T]:
        """
        Load a list of domain objects of type T_co.
        """
        raise NotImplementedError

    @abstractmethod
    def write(self, data: List[T]) -> None:
        """
        Persist a list of domain objects.
        """
        raise NotImplementedError


