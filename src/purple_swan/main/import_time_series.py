import argparse
import os
from purple_swan.data.data_factory import DataFactory
import purple_swan.data.loaders.s3_instruments_data_loader as sl
from purple_swan.data.data_providers.time_series_loader import YahooTimeSeriesProvider
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
        entity_type = EntityType['INSTRUMENT']
    except KeyError:
        raise SystemExit(f"Unknown entity type '{args.entity}'. Must match EntityType values.")

    print(f"📦 Loading data for entity: {entity_type.name}")
    data = factory.get_data(entity_type)
    symbol_list = list(data['Symbol'].values)
    symbol_list = [str(s) for s in symbol_list]
    symbol_list = sorted(list(set(symbol_list)))
    from numpy import array_split
    symbol_lists = array_split(symbol_list, 10)
    yf = YahooTimeSeriesProvider()
    dfs = []
    for i, symbol_list in enumerate(symbol_lists):
        print(i)
        data = yf.get_time_series(list(symbol_list),'2023-01-01','2025-11-01')
        url = f"s3://pswn-test/yahoo_ts/ts_{i}.csv"
        data.to_csv(url,index=False,header=True)
        url = f"s3://pswn-test/yahoo_ts/ts_{i}.parquet"
        data.to_parquet(url,compression="snappy")
        dfs.append(data)

    from pandas import concat
    url = "s3://pswn-test/ts.parquet"
    data.to_parquet(url,compression="snappy")

if __name__ == "__main__":
    main()
