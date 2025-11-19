from purple_swan.data.factory_builder import build_factory_from_profile
from purple_swan.data.environment import Environment , EnvironmentRepository
from purple_swan.data.loaders.position_enricher import PositionEnricher
from purple_swan.data.data_providers.time_series_loader import YahooTimeSeriesProvider
from purple_swan.data.models.models import Position
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
 #   factory = build_factory_from_profile(profile="s3")

    # Create composed repository
    repo = EnvironmentRepository(factory)

    # Register enrichers
    repo.register_enricher(PositionEnricher(), Position)


    # Optional: add time series
    repo.set_time_series_provider(YahooTimeSeriesProvider())

    # Load complete portfolio data in one call
    portfolio_data = repo.load_portfolio_data(
        position_filters = {'cik':['1002784','1013538']},
        include_time_series=False
    )

    # Now you have everything linked:
    print(portfolio_data.position_df)  # Positions with instrument details
    portfolio_data.position_df.to_csv("/tmp/portfolio.csv")
    #print(portfolio_data.time_series)  # Historical prices

    # Pass to analytics
    # var_engine = VarEngine(portfolio_data.time_series, portfolio_data.position_df['weight'])


if __name__ == "__main__":
    main()