from purple_swan.data.factory_builder import build_factory_from_profile
from purple_swan.data.environment import EnvironmentRepository
from purple_swan.data.enrichment.portfolio_enricher import  PortfolioEnricher13F,PortfolioTSMatrixEnricher,PortfolioFactorMatrixEnricher
from purple_swan.data.enrichment.position_enricher import PositionInstrumentEnricher
from purple_swan.data.data_providers.time_series_loader import YahooTimeSeriesProvider
from purple_swan.data.models.models import Position, Portfolio,FactorModel
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
    repo.register_enricher(PortfolioFactorMatrixEnricher(), Portfolio)

    # Optional: add time series
    repo.set_time_series_provider(YahooTimeSeriesProvider())

    #position_filters = {'cik': ['1021223']},
    env = repo.load_portfolio_data(
            position_filters = {'cik': ['test']},
        include_time_series=False)

    port_data = env.portfolios[0]

    default_pos = Position('N/A',0,'N/A')

    ts = port_data.ts_matrix
    tickers = sorted(port_data.positions.keys())
    tickers = [t for t in tickers if t in ts.columns]
    missing_tickers = [t for t in tickers if t not in ts.columns]

    weights = [port_data.positions[ticker].weight for ticker in tickers]
    ts = ts[tickers]

    # print(ts.head())
    # print(ts.dropna().head())
    # print("==================================")
    print(port_data.position_df())

    var_engine = VarEngine(ts,weights)
    var_engine.calc_var()

    @timed
    def run_var():
        for _ in range(1):
            vars = var_engine.calc_var()
        return vars
    print("=====================")
    print("calculatikng var")
    var = run_var()[0].__dict__

    from purple_swan.llm.var_explain import analyze_var_profile,generate_daily_risk_report,query_var_results
    from purple_swan.llm.trade_recommendations_agent import get_trade_recommendations


    analysis = analyze_var_profile(var, portfolio=port_data)
    print(analysis)

    trades = get_trade_recommendations(analysis,tickers)
    print(trades)

    # print("NEW PORTFOLIO")
    # analysis = analyze_var_profile(var2, tickers)
    # print(analysis)


    # 2. Generate report
    # print("\n2. FORMATTED DAILY REPORT")
    # print("-" * 70)
    # report = generate_daily_risk_report(var, "2024-11-26", tickers)
    # print(report)
    # print("\n2. FORMATTED DAILY REPORT 2")
    # report = generate_daily_risk_report(var2, "2024-11-26", tickers)
    # print(report)

    # # 3. Query examples
    # print("\n3. EXAMPLE QUERIES")
    # print("-" * 70)
    #
    # queries = [
    #     "What would be the best way to reduce portfolio VaR?",
    #     "Are there any concentration risks in my portfolio?",
    #     "What does the tail risk analysis tell us about extreme scenarios?",
    # ]
    #
    # for q in queries:
    #     print(f"\nQ: {q}")
    #     print(f"A: {query_var_results(var, q, tickers)[:300]}...")
    #
    # # 4. Interactive mode
    # print("\n4. INTERACTIVE MODE")
    # print("-" * 70)
    # print("Enter your questions about the portfolio (type 'quit' to exit):")
    #
    # while True:
    #     user_input = input("\nYour question: ").strip()
    #     if user_input.lower() == 'quit':
    #         break
    #     if user_input:
    #         answer = query_var_results(var, user_input, tickers)
    #         print(f"\nAnalysis: {answer}\n")

if __name__ == "__main__":
    main()