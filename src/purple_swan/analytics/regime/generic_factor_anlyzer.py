# src/purple_swan/analytics/regime/generic_factor_analyzer.py
from dataclasses import dataclass
from typing import Dict
import numpy as np
import pandas as pd


@dataclass
class RegimeFactorMetrics:
    """Generic factor metrics for a regime"""
    regime_id: int
    factor_name: str
    mean_return: float
    std_dev: float
    sharpe: float
    cumulative_return: float
    best_return: float
    worst_return: float
    win_rate: float
    n_observations: int


class GenericFactorAnalyzer:
    """
    Analyzes ANY factor matrix across detected regimes.
    Works with any number of regimes.
    """

    def __init__(self, factor_data: pd.DataFrame):
        """
        Args:
            factor_data: (T, n_factors) DataFrame with any column names
        """
        self.factor_data = factor_data

    def analyze_by_regime(
            self,
            regime_assignments: np.ndarray,
            regime_probabilities: np.ndarray = None
    ) -> Dict[int, Dict[str, RegimeFactorMetrics]]:
        """
        Compute factor statistics per regime.

        Args:
            regime_assignments: (T,) array of regime ids (0, 1, ..., n_regimes-1)
            regime_probabilities: Optional (T, n_regimes) soft assignments

        Returns:
            Dict[regime_id][factor_name] -> RegimeFactorMetrics
        """
        n_regimes = len(np.unique(regime_assignments))
        results = {}

        for regime_id in range(n_regimes):
            results[regime_id] = {}

            # Filter to this regime
            if regime_probabilities is not None:
                # Soft assignment: weight by probability
                mask = regime_probabilities[:, regime_id]
            else:
                # Hard assignment
                mask = (regime_assignments == regime_id).astype(float)

            if mask.sum() == 0:
                continue

            for factor_name in self.factor_data.columns:
                factor_rets = self.factor_data[factor_name]
                weighted_rets = factor_rets * mask

                # Compute statistics
                n_obs = int(mask.sum())
                mean_ret = float((weighted_rets.sum() / mask.sum() if mask.sum() > 0 else 0) * 252)
                std_ret = float(factor_rets[mask > 0].std() * np.sqrt(252)) if n_obs > 1 else 0.0
                sharpe = float(mean_ret / std_ret) if std_ret > 0 else 0.0
                cumul_ret = float((1 + factor_rets[mask > 0]).prod() - 1) if n_obs > 1 else 0.0

                results[regime_id][factor_name] = RegimeFactorMetrics(
                    regime_id=regime_id,
                    factor_name=factor_name,
                    mean_return=mean_ret,
                    std_dev=std_ret,
                    sharpe=sharpe,
                    cumulative_return=cumul_ret,
                    best_return=float(factor_rets[mask > 0].max()) if n_obs > 0 else 0.0,
                    worst_return=float(factor_rets[mask > 0].min()) if n_obs > 0 else 0.0,
                    win_rate=float((factor_rets[mask > 0] > 0).sum() / n_obs) if n_obs > 0 else 0.0,
                    n_observations=n_obs
                )

        return results

    def get_regime_summary_table(
            self,
            metrics: Dict[int, Dict[str, RegimeFactorMetrics]]
    ) -> pd.DataFrame:
        """
        Create summary table: regimes × factors with Sharpe ratios.
        """
        data = {}

        for regime_id, factor_dict in metrics.items():
            data[f'Regime {regime_id}'] = {
                name: m.sharpe for name, m in factor_dict.items()
            }

        return pd.DataFrame(data)

    def get_regime_characteristics(
            self,
            regime_assignments: np.ndarray,
            regime_probabilities: np.ndarray = None
    ) -> pd.DataFrame:
        """
        Describe each regime by its factor characteristics.
        What makes regime 0 different from regime 1?
        """
        n_regimes = len(np.unique(regime_assignments))
        characteristics = {}

        for regime_id in range(n_regimes):
            if regime_probabilities is not None:
                mask = regime_probabilities[:, regime_id]
            else:
                mask = (regime_assignments == regime_id).astype(float)

            if mask.sum() == 0:
                continue

            # Mean return per factor during this regime
            weighted_mean = (self.factor_data * mask.reshape(-1, 1)).sum(axis=0) / mask.sum()

            characteristics[f'Regime {regime_id}'] = weighted_mean

        return pd.DataFrame(characteristics)