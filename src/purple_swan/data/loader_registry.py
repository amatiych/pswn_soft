# purple_swan/data/loader_registry.py
from __future__ import annotations
from typing import Dict, Type, Callable, Any

from purple_swan.data.loaders.data_loader import DataLoader

_LOADER_REGISTRY: Dict[str, Type[DataLoader[Any]]] = {}


def register_loader(name: str) -> Callable[[Type[DataLoader[Any]]], Type[DataLoader[Any]]]:
    def decorator(cls: Type[DataLoader[Any]]) -> Type[DataLoader[Any]]:
        if name in _LOADER_REGISTRY:
            raise ValueError(f"Loader '{name}' already registered")
        _LOADER_REGISTRY[name] = cls
        return cls
    return decorator


def get_loader_cls(name: str) -> Type[DataLoader[Any]]:
    try:
        return _LOADER_REGISTRY[name]
    except KeyError:
        raise KeyError(
            f"Unknown loader '{name}'. Registered: {list(_LOADER_REGISTRY.keys())}"
        )
