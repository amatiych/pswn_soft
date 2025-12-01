# src/purple_swan/llm/claude_generic_regime_interpreter.py

import anthropic
import json
from typing import Dict, List
import os
import numpy as np
import pandas as pd
from datetime import datetime

class ClaudeRegimeInterpreter:
    """
    Generic Claude interpreter for ANY regimes + ANY factors.
    No hard-coded assumptions about regimes or factors.
    """

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.environ.get("CLAUDE_API_KEY")
        self.client = anthropic.Anthropic(api_key=self.api_key)

    def interpret(
            self,
            regime_assignments: np.ndarray,
            regime_probabilities: np.ndarray,
            factor_metrics: Dict[int, Dict[str, 'RegimeFactorMetrics']],
            regime_dates: Dict[int, pd.DatetimeIndex],
            factor_names: List[str],
            n_regimes: int
    ) -> Dict:
        """
        Generically interpret detected regimes using Claude.

        Args:
            regime_assignments: (T,) hard regime assignments
            regime_probabilities: (T, n_regimes) soft assignments
            factor_metrics: Dict[regime_id][factor_name] -> metrics
            regime_dates: Dict[regime_id] -> dates when regime was active
            factor_names: List of all factor names
            n_regimes: Number of regimes detected

        Returns:
            Structured interpretation from Claude
        """

        # Build comprehensive data summary
        regime_summaries = self._build_regime_summaries(
            regime_dates, regime_assignments, n_regimes
        )

        factor_performance = self._build_factor_performance(
            factor_metrics, factor_names, n_regimes
        )

        # Create prompt
        prompt = self._build_interpretation_prompt(
            n_regimes=n_regimes,
            regime_summaries=regime_summaries,
            factor_performance=factor_performance,
            factor_names=factor_names
        )

        # Call Claude
        message = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = message.content[0].text
        interpretation = self._parse_interpretation(response_text)

        return interpretation

    def _build_regime_summaries(
            self,
            regime_dates: Dict[int, pd.DatetimeIndex],
            regime_assignments: np.ndarray,
            n_regimes: int
    ) -> Dict:
        """Summarize each regime's frequency and time periods"""
        summaries = {}

        for regime_id in range(n_regimes):
            dates = regime_dates.get(regime_id, pd.DatetimeIndex([]))
            if isinstance(dates[0],str):
                dates = [datetime.strptime(d,'%Y%m%d') for d in dates]
            summaries[f'Regime_{regime_id}'] = {
                'num_observations': len(dates),
                'pct_of_total': float(len(dates) / len(regime_assignments) * 100),
                'date_range': f"{dates[0].strftime("%Y-%m-%d")} to {dates[-1].strftime("%Y-%m-%d")}" if len(
                    dates) > 0 else "N/A",
                'first_occurred': dates[0].strftime("%Y-%m-%d") if len(dates) > 0 else "N/A",
                'last_occurred': dates[-1].strftime("%Y-%m-%d") if len(dates) > 0 else "N/A"
            }

        return summaries

    def _build_factor_performance(
            self,
            factor_metrics: Dict[int, Dict[str, 'RegimeFactorMetrics']],
            factor_names: List[str],
            n_regimes: int
    ) -> Dict:
        """Format factor performance for each regime"""
        performance = {}

        for regime_id in range(n_regimes):
            performance[f'Regime_{regime_id}'] = {}

            if regime_id in factor_metrics:
                for factor_name in factor_names:
                    if factor_name in factor_metrics[regime_id]:
                        m = factor_metrics[regime_id][factor_name]
                        performance[f'Regime_{regime_id}'][factor_name] = {
                            'mean_return_annual': f"{m.mean_return * 100:.2f}%",
                            'std_dev_annual': f"{m.std_dev * 100:.2f}%",
                            'sharpe_ratio': f"{m.sharpe:.2f}",
                            'cumulative_return': f"{m.cumulative_return * 100:.2f}%",
                            'best_day': f"{m.best_return * 100:.2f}%",
                            'worst_day': f"{m.worst_return * 100:.2f}%",
                            'win_rate': f"{m.win_rate * 100:.1f}%",
                            'n_obs': m.n_observations
                        }

        return performance

    def _build_interpretation_prompt(
            self,
            n_regimes: int,
            regime_summaries: Dict,
            factor_performance: Dict,
            factor_names: List[str]
    ) -> str:
        """Build flexible prompt for any number of regimes/factors"""

        prompt = f"""You are a financial analyst specializing in market regime analysis.

I have detected {n_regimes} distinct market regimes using unsupervised machine learning on the following factors:
{', '.join(factor_names)}

REGIME OCCURRENCE:
{json.dumps(regime_summaries, indent=2)}

FACTOR PERFORMANCE BY REGIME:
{json.dumps(factor_performance, indent=2)}

Based on this data, please provide:

1. **Regime Characterization** (for each regime):
   - What is a descriptive label for this regime? (e.g., "Risk-Off", "Growth", "Inflation", "Quiet")
   - What are the defining characteristics?
   - Which factors drive returns in this regime?

2. **Factor Behavior Analysis**:
   - Which factors are consistently positive/negative across regimes?
   - Which factors act as diversifiers (positive when others are negative)?
   - Which factors are regime-dependent (behavior changes significantly)?
   - Rank factors by their usefulness in distinguishing regimes

3. **Regime Dynamics**:
   - How frequent are transitions between regimes?
   - Are there any apparent patterns in regime sequencing?
   - Do regimes cluster in specific time periods?

4. **Investment Implications**:
   - What's the optimal portfolio positioning for each regime?
   - Which factors have best risk-adjusted returns in each regime?
   - How would you construct hedges for regime transitions?

5. **Risk Management**:
   - Which regime(s) represent the highest tail risk?
   - What are the early warning signs of regime transitions?
   - How do factor correlations differ across regimes?

Return ONLY valid JSON with no markdown or preamble:

{{
    "regime_labels": {{
        "regime_0": {{"label": "...", "description": "..."}},
        "regime_1": {{"label": "...", "description": "..."}},
        ...
    }},
    "factor_analysis": {{
        "regime_drivers": {{"regime_0": [...], "regime_1": [...], ...}},
        "diversifiers": [...],
        "regime_dependent_factors": [...],
        "most_useful_for_distinction": [...]
    }},
    "regime_dynamics": {{
        "transition_frequency": "...",
        "sequencing_patterns": "...",
        "temporal_clustering": "..."
    }},
    "investment_implications": {{
        "regime_0": {{"positioning": "...", "best_factors": [...], "hedges": [...]}},
        "regime_1": {{"positioning": "...", "best_factors": [...], "hedges": [...]}},
        ...
    }},
    "risk_management": {{
        "highest_risk_regime": "...",
        "warning_signs": [...],
        "correlation_changes": "..."
    }}
}}"""

        return prompt

    def _parse_interpretation(self, response_text: str) -> Dict:
        """Extract JSON from Claude response"""
        try:
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1

            if json_start != -1 and json_end > json_start:
                json_str = response_text[json_start:json_end]
                return json.loads(json_str)
            else:
                return {'raw_response': response_text, 'parse_error': 'No JSON found'}
        except json.JSONDecodeError as e:
            return {'raw_response': response_text, 'parse_error': str(e)}

    def generate_report(
            self,
            interpretation: Dict,
            regime_summaries: Dict,
            factor_performance: Dict
    ) -> str:
        """Generate human-readable report"""

        report = "=" * 80 + "\n"
        report += "REGIME ANALYSIS REPORT\n"
        report += "=" * 80 + "\n\n"

        # Regime labels
        if 'regime_labels' in interpretation:
            report += "REGIME CHARACTERIZATION\n"
            report += "-" * 80 + "\n"
            for regime, details in interpretation['regime_labels'].items():
                freq = regime_summaries.get(regime, {}).get('pct_of_total', 0)
                report += f"\n{regime.upper()}: {details.get('label', 'Unknown')}\n"
                report += f"  Frequency: {freq:.1f}% of period\n"
                report += f"  Description: {details.get('description', 'N/A')}\n"

        # Factor analysis
        if 'factor_analysis' in interpretation:
            report += "\n\nFACTOR ANALYSIS\n"
            report += "-" * 80 + "\n"
            fa = interpretation['factor_analysis']

            if 'diversifiers' in fa:
                report += f"\nDiversifiers: {', '.join(fa['diversifiers'])}\n"

            if 'regime_dependent_factors' in fa:
                report += f"Regime-Dependent: {', '.join(fa['regime_dependent_factors'])}\n"

            if 'regime_drivers' in fa:
                report += "\nRegime Drivers:\n"
                for regime, drivers in fa['regime_drivers'].items():
                    report += f"  {regime}: {', '.join(drivers)}\n"

        # Investment implications
        if 'investment_implications' in interpretation:
            report += "\n\nINVESTMENT IMPLICATIONS\n"
            report += "-" * 80 + "\n"
            for regime, impl in interpretation['investment_implications'].items():
                report += f"\n{regime.upper()}:\n"
                report += f"  Positioning: {impl.get('positioning', 'N/A')}\n"
                report += f"  Best Factors: {', '.join(impl.get('best_factors', []))}\n"

        report += "\n" + "=" * 80 + "\n"

        return report