"""
Example script to test Purple Swan DataFactory end-to-end.

Usage:
    # Default: uses config/data_profiles.yaml, profile=s3, env=dev
    pdm run python -m purple_swan.run_sample_load

    # Specify a profile
    pdm run python -m purple_swan.run_sample_load --profile hybrid

    # Specify environment (overrides PSWN_ENV)
    pdm run python -m purple_swan.run_sample_load --env prod

    # Specify custom config path
    pdm run python -m purple_swan.run_sample_load --config /path/to/custom.yaml
"""

import argparse
import os
from pprint import pprint
from purple_swan.data.factory_builder import build_factory_from_profile
from purple_swan.data.models.models import EntityType


def main() -> None:
    parser = argparse.ArgumentParser(description="Run sample data load using DataFactory.")
    parser.add_argument("--profile", "-p", default="s3", help="Profile to use (s3, db, hybrid, etc.)")
    parser.add_argument("--env", "-e", default=None, help="Environment (dev, staging, prod). Overrides PSWN_ENV.")
    parser.add_argument("--config", "-c", default=None, help="Path to YAML config (optional).")
    parser.add_argument("--entity", default="portfolio", help="Entity type to load (instrument, portfolio, position).")

    args = parser.parse_args()

    # Show environment info
    print("🔧 Active Configuration")
    print("-----------------------")
    print(f"Profile:       {args.profile}")
    env_name = args.env or os.getenv("PSWN_ENV", "dev")
    print(f"Environment:   {env_name}")
    print(f"Config file:   {args.config or '(auto-discovered)'}\n")

    # Build factory using profile & env
    factory = build_factory_from_profile(
        profile=args.profile,
        config_path=args.config,
        env_name=args.env,
    )

    # Map entity argument to EntityType enum
    try:
        entity_type = EntityType[args.entity.upper()]
    except KeyError:
        raise SystemExit(f"Unknown entity type '{args.entity}'. Must match EntityType values.")

    # Perform the load
    print(f"📦 Loading data for entity: {entity_type.name}")
    filters = {'cik':['1002784','1013538']}
    data = factory.get_data(entity_type,filters=filters)

    print("\n✅ Load complete.")
    print(f"Type: {type(data)}")
    try:
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            print(f"Shape: {data.shape}")
            print(data.head())
    except Exception:
        pass

    print("\nFactory loaders registered:")
    pprint(factory._loaders)


if __name__ == "__main__":
    main()
