from __future__ import annotations

import os
from string import Template
from pathlib import Path
from typing import Mapping, Any
import yaml

from purple_swan.data.data_factory import DataFactory
from purple_swan.data.models.models import EntityType
from purple_swan.data.models.data_loader import DataLoader
from purple_swan.data.loader_registry import get_loader_cls


DEFAULT_CONFIG_RELATIVE = Path("config") / "data_profiles.yaml"
ENV_VAR_NAME = "PSWN_CONFIG"


def _find_repo_root(start: Path | None = None) -> Path:
    if start is None:
        start = Path(__file__).resolve().parent

    current = start
    for _ in range(10):
        if (current / "pyproject.toml").exists() or (current / ".git").exists():
            return current
        if current.parent == current:
            break
        current = current.parent
    return Path(__file__).resolve().parents[2]


def resolve_config_path(explicit: str | Path | None = None) -> Path:
    if explicit is not None:
        p = Path(explicit).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {p}")
        return p

    env_val = os.getenv(ENV_VAR_NAME)
    if env_val:
        p = Path(env_val).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"{ENV_VAR_NAME} set to '{env_val}' not found: {p}")
        return p

    repo_root = _find_repo_root()
    default_path = repo_root / DEFAULT_CONFIG_RELATIVE
    if not default_path.exists():
        raise FileNotFoundError(f"Default config missing: {default_path}")
    return default_path


def _substitute_env_vars(obj, env_map):
    if isinstance(obj, dict):
        return {k: _substitute_env_vars(v, env_map) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_substitute_env_vars(i, env_map) for i in obj]
    elif isinstance(obj, str):
        return Template(obj).safe_substitute(env_map)
    else:
        return obj


def build_factory_from_profile(profile: str, config_path: str | Path | None = None, env_name: str | None = None) -> DataFactory:
    cfg_path = resolve_config_path(config_path)
    with open(cfg_path, "r") as f:
        full_cfg = yaml.safe_load(f)

    env_name = env_name or os.getenv("PSWN_ENV", "dev")
    envs = full_cfg.get("envs", {})
    env_cfg = envs.get(env_name, {})

    profiles = full_cfg.get("profiles")
    if not profiles or profile not in profiles:
        raise KeyError(f"Profile '{profile}' not found in {cfg_path}")

    profile_cfg = profiles[profile]
    resolved_profile = _substitute_env_vars(profile_cfg, env_cfg)

    factory = DataFactory()
    for entity_name, entity_cfg in resolved_profile.items():
        entity_type = EntityType[entity_name.upper()]
        loader_name = entity_cfg["loader"]
        loader_cls = get_loader_cls(loader_name)
        loader: DataLoader = loader_cls.from_config(entity_type, entity_cfg)
        factory.register(loader)

    return factory
