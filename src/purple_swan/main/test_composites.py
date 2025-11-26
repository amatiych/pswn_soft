from purple_swan.data.factory_builder import build_factory_from_profile
from purple_swan.data.environment import EnvironmentRepository
from purple_swan.data.enrichment.portfolio_enricher import  PortfolioEnricher13F,PortfolioTSMatrixEnricher
from purple_swan.data.enrichment.position_enricher import PositionInstrumentEnricher
from purple_swan.data.data_providers.time_series_loader import YahooTimeSeriesProvider
from purple_swan.data.models.models import Position, Portfolio
from purple_swan.analytics.var.var_engine import VarEngine,VaR
import os
import argparse
from purple_swan.core.timer import timed

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

    #position_filters = {'cik': ['1021223']},
    env = repo.load_portfolio_data(
            position_filters = {'cik': ['1021223']},
        include_time_series=False)

    port_data = env.portfolios[0]

    default_pos = Position('N/A',0,'N/A')

    ts = port_data.ts_matrix
    tickers = sorted(port_data.positions.keys())
    tickers = [t for t in tickers if t in ts.columns]
    missing_tickers = [t for t in tickers if t not in ts.columns]

    weights = [port_data.positions[ticker].weight for ticker in tickers]
    ts = ts[tickers]

    ts.to_csv('~/data/ts_matrix.csv')
    # print(ts.head())
    # print(ts.dropna().head())
    # print("==================================")
    # print(weights)

    var_engine = VarEngine(ts,weights)
    var_engine.calc_var()

    @timed
    def run_var():
        for _ in range(10000):
            vars = var_engine.calc_var()
        return vars
    print("=====================")
    print("calculatikng var")
    var = run_var()[0]
    from pandas import DataFrame,Series
    df_margins = Series(var.incremental_var,index=tickers)
    # print(df_margins)
    # print(df_margins.sum())
    print(var.var)
    #print(df_margins)

if __name__ == "__main__":
    main()