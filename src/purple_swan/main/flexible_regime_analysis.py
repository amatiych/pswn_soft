# src/purple_swan/main/flexible_regime_analysis.py

from purple_swan.analytics.regime.regime_detection import (
    GMMRegimeDetector, HMMRegimeDetector, KMeansRegimeDetector
)
from purple_swan.analytics.regime.generic_factor_anlyzer import GenericFactorAnalyzer
from purple_swan.llm.claude_regime_interpeter import ClaudeRegimeInterpreter
import pandas as pd
import numpy as np
from typing import Dict, List
from datetime import datetime

class FlexibleRegimeAnalysisPipeline:
    """
    Generic pipeline: works with ANY factors, ANY number of regimes.
    """

    def __init__(
            self,
            factor_data: pd.DataFrame,
            n_regimes: int,
            detector_type: str = 'gmm'  # 'gmm', 'hmm', 'kmeans', 'neural'
    ):
        """
        Args:
            factor_data: (T, n_factors) DataFrame, any columns
            n_regimes: Number of regimes to detect (2, 3, 5, etc.)
            detector_type: 'gmm', 'hmm', 'kmeans', or 'neural'
        """
        self.factor_data = factor_data
        self.n_regimes = n_regimes
        self.detector_type = detector_type

        # Initialize detector
        if detector_type == 'gmm':
            self.detector = GMMRegimeDetector(n_regimes)
        elif detector_type == 'hmm':
            self.detector = HMMRegimeDetector(n_regimes)
        elif detector_type == 'kmeans':
            self.detector = KMeansRegimeDetector(n_regimes)
        else:
            raise ValueError(f"Unknown detector: {detector_type}")

        self.analyzer = GenericFactorAnalyzer(factor_data)
        self.interpreter = ClaudeRegimeInterpreter()

        self.results = None

    def run(self) -> Dict:
        """Execute full pipeline"""

        print(f"🔄 Detecting {self.n_regimes} regimes using {self.detector_type.upper()}...")

        # Step 1: Detect regimes
        detection_result = self.detector.fit(self.factor_data)
        regimes = detection_result.regime_timeseries
        probs = detection_result.regime_probabilities

        print(f"✅ Regime detection complete")
        print(f"   Distribution: {np.unique(regimes, return_counts=True)}")

        # Step 2: Create regime timeline
        regime_dates = {}
        for regime_id in range(self.n_regimes):
            mask = regimes == regime_id
            regime_dates[regime_id] = self.factor_data.index[mask]

        # Step 3: Analyze factor performance
        print(f"\n📊 Analyzing factor performance by regime...")
        factor_metrics = self.analyzer.analyze_by_regime(regimes, probs)

        # Print summary
        summary_table = self.analyzer.get_regime_summary_table(factor_metrics)
        print("\nFactor Sharpe Ratios by Regime:")
        print(summary_table)

        # Step 4: Get Claude interpretation
        print(f"\n🤖 Getting Claude interpretation...")
        regime_summaries = self._build_regime_summaries(regime_dates, regimes)
        factor_performance = self._build_factor_performance(factor_metrics)


        interpretation = self.interpreter.interpret(
            regime_assignments=regimes,
            regime_probabilities=probs,
            factor_metrics=factor_metrics,
            regime_dates=regime_dates,
            factor_names=list(self.factor_data.columns),
            n_regimes=self.n_regimes
        )

        # Generate report
        report = self.interpreter.generate_report(
            interpretation, regime_summaries, factor_performance
        )

        self.results = {
            'regimes': regimes,
            'probabilities': probs,
            'factor_metrics': factor_metrics,
            'interpretation': interpretation,
            'report': report,
            'summary_table': summary_table
        }

        return self.results

    def _build_regime_summaries(self, regime_dates, regimes):
        """Helper to build regime summaries"""
        summaries = {}
        for regime_id in range(self.n_regimes):
            dates = regime_dates.get(regime_id, pd.DatetimeIndex([]))
            if dates[0].__class__ is str:
                dates = [datetime.strptime(d,"%Y%m%d") for d in dates]
            summaries[f'Regime_{regime_id}'] = {
                'num_observations': len(dates),
                'pct_of_total': float(len(dates) / len(regimes) * 100),
                'date_range': f"{dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}" if len(
                    dates) > 0 else "N/A"
            }
        return summaries

    def _build_factor_performance(self, factor_metrics):
        """Helper to build factor performance dict"""
        performance = {}
        for regime_id, metrics_dict in factor_metrics.items():
            performance[f'Regime_{regime_id}'] = {}
            for factor_name, m in metrics_dict.items():
                performance[f'Regime_{regime_id}'][factor_name] = {
                    'sharpe': f"{m.sharpe:.2f}",
                    'mean_return': f"{m.mean_return * 100:.2f}%",
                    'cumulative': f"{m.cumulative_return * 100:.2f}%"
                }
        return performance

if __name__ == "__main__":
    fn = "s3://pswn-test/market_data/factors/ff/ff_returns.csv"
    # Example 1: 3 Fama-French regimes
    ff_returns = pd.read_csv(fn, index_col=0)
    ff_returns.index = pd.to_datetime(ff_returns.index)
    ff_returns = ff_returns[['Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA']]

    pipeline = FlexibleRegimeAnalysisPipeline(
        factor_data=ff_returns,
        n_regimes=3,
        detector_type='hmm'
    )
    results = pipeline.run()
    print(results['report'])

    # Example 2: 5 regimes on your time series data
    returns = pd.read_parquet("s3://pswn-test/all_time_series.parquet")

    pipeline = FlexibleRegimeAnalysisPipeline(
        factor_data=returns,
        n_regimes=5,
        detector_type='gmm'
    )
    results = pipeline.run()

    # Example 3: Just 2 regimes (bull/bear)
    pipeline = FlexibleRegimeAnalysisPipeline(
        factor_data=returns.iloc[:, :10],  # First 10 stocks
        n_regimes=2,
        detector_type='kmeans'
    )
    results = pipeline.run()
    print(results['interpretation']['regime_labels'])

    # Example 4: Custom factors + custom regimes
    custom_factors = pd.DataFrame({
        'volatility': returns.std(axis=1).rolling(20).mean(),
        'momentum': returns.mean(axis=1).rolling(60).mean(),
        'drawdown': (returns.max() - returns.min()).rolling(30).mean()
    })

    pipeline = FlexibleRegimeAnalysisPipeline(
        factor_data=custom_factors,
        n_regimes=4,
        detector_type='gmm'
    )
    results = pipeline.run()