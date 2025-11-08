# purple_swan/data/factory.py
from typing import Dict, Any
from purple_swan.data.models.models import EntityType
from purple_swan.data.loaders.data_loader import DataLoader

class DataFactory:
    def __init__(self) -> None:
        self._loaders: Dict[EntityType, DataLoader] = {}

    def register(self, loader: DataLoader) -> None:
        key = loader.entity_type
        if key in self._loaders:
            raise ValueError(f"Loader already registered for {key}")
        self._loaders[key] = loader

    def get(self, entity_type: EntityType) -> DataLoader:
        try:
            return self._loaders[entity_type]
        except KeyError:
            raise KeyError(f"No loader registered for {entity_type=}")

    def get_data(self, data_type: EntityType, filters: dict | None = None):
        loader = self._loaders.get(data_type)
        if loader is None:
            raise KeyError(f"No loader for {data_type}")
        return loader.load(filters or {})

    def write_data(self, data_type: EntityType, data: Any) -> None:
        loader = self._loaders.get(data_type)
        if loader is None:
            raise KeyError(f"No loader for {data_type}")
        loader.write(data)
