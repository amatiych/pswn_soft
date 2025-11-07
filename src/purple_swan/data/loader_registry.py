# purple_swan/data/loader_registry.py
from typing import Dict, Type, Callable, Mapping, Any
from purple_swan.data.models.models import  EntityType
from purple_swan.data.models.data_loader import DataLoader

_LOADER_REGISTRY: Dict[str, Type[DataLoader]] = {}


def register_loader(name: str) -> Callable[[Type[DataLoader]], Type[DataLoader]]:
    """
    Decorator to register a DataLoader subclass under a string key.

    Example:
        @register_loader("s3_instruments")
        class S3InstrumentLoader(DataLoader):
            ...
    """
    def decorator(cls: Type[DataLoader]) -> Type[DataLoader]:
        if name in _LOADER_REGISTRY:
            raise ValueError(f"Loader '{name}' already registered")
        _LOADER_REGISTRY[name] = cls
        return cls

    return decorator


def get_loader_cls(name: str) -> Type[DataLoader]:
    try:
        return _LOADER_REGISTRY[name]
    except KeyError:
        raise KeyError(
            f"Unknown loader '{name}'. Registered: {list(_LOADER_REGISTRY.keys())}"
        )
