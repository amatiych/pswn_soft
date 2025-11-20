from purple_swan.data.factory_builder import build_factory_from_profile
from purple_swan.data.environment import EnvironmentRepository
from purple_swan.data.enrichment.portfolio_enricher import  PortfolioEnricher13F,PortfolioTSMatrixEnricher
from purple_swan.data.enrichment.position_enricher import PositionInstrumentEnricher
from purple_swan.data.data_providers.time_series_loader import YahooTimeSeriesProvider
from purple_swan.data.models.models import Position, Portfolio
import os
import argparse

def main():
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

    # Build factory as before
    # factory = build_factory_from_profile(profile="s3")

    # Create composed repository
    repo = EnvironmentRepository(factory)

    # Register enrichers
    repo.register_enricher(PositionInstrumentEnricher(), Position)
    repo.register_enricher(PortfolioEnricher13F(), Portfolio)
    repo.register_enricher(PortfolioTSMatrixEnricher(), Portfolio)
    # Optional: add time series
    repo.set_time_series_provider(YahooTimeSeriesProvider())


    env = repo.load_portfolio_data(
        position_filters = {'cik': ['1021223']},
        include_time_series=False)

    port_data = env.portfolios[0]

    print(port_data.ts_matrix.dropna())

if __name__ == "__main__":
    main()