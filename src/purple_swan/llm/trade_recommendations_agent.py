"""
Portfolio VaR Trade Recommendation Agent

This demonstrates how to use Claude to generate structured JSON trade recommendations
based on VaR analysis. Claude uses JSON output formatting to produce trades you can
execute or feed into your trading system.

Key features:
- Analyzes VaR concentration and tail risk
- Generates specific, actionable trades
- Returns JSON that's ready for downstream processing
- Includes reasoning for each trade
"""

import json
import os
from typing import Optional, List
import anthropic

client = anthropic.Anthropic(api_key=os.environ.get("CLAUDE_API_KEY"))


def get_trade_recommendations(
        var_results: dict,
        tickers: List[str],
        objective: str = "reduce_var",
        target_reduction: float = 0.05,
        max_trades: int = 10,
        position_sizes: Optional[List[float]] = None
) -> dict:
    """
    Generate trade recommendations as structured JSON.

    Args:
        var_results: VaR calculation results
        tickers: List of ticker symbols
        objective: "reduce_var", "reduce_tail_risk", "reduce_concentration", "diversify"
        target_reduction: Target VaR reduction (e.g., 0.05 = 5%)
        max_trades: Maximum number of trades to recommend
        position_sizes: Optional list of current position sizes (in shares or notional)

    Returns:
        Dict with trades and reasoning
    """

    marginal_vars = var_results['marginal_var'][0]
    incremental_vars = var_results.get('incremental_var', [None])[0] if 'incremental_var' in var_results else None
    tail_indexes = var_results['tail_indexes']

    # Identify top risk contributors
    top_risk_indices = sorted(
        range(len(marginal_vars)),
        key=lambda i: marginal_vars[i]
    )[:max_trades * 2]  # Get more to analyze

    # Check which are in tail risk
    tail_risk_ids = set(tail_indexes)

    # Build context for Claude
    high_risk_positions = []
    for rank, idx in enumerate(top_risk_indices[:max_trades], 1):
        position = {
            "rank": rank,
            "index": idx,
            "ticker": tickers[idx] if idx < len(tickers) else f"Position_{idx}",
            "marginal_var": marginal_vars[idx],
            "in_tail_stress": idx in tail_risk_ids,
        }
        if position_sizes and idx < len(position_sizes):
            position["current_size"] = position_sizes[idx]
        high_risk_positions.append(position)

    # Build the prompt
    context = f"""
You are a portfolio risk analyst. Based on the VaR analysis below, generate trade recommendations as JSON.

Portfolio VaR Analysis:
- Current Portfolio VaR (95% CI): {var_results['var'] * 100:.4f}%
- Expected Shortfall: {var_results['es'] * 100:.4f}%
- Total Positions: {len(marginal_vars)}
- Positions in Tail Stress Scenario: {len(tail_indexes)}
- Objective: {objective}
- Target: {target_reduction * 100:.1f}% reduction

Top Risk Contributors (by Marginal VaR impact):
{json.dumps(high_risk_positions, indent=2)}

Current Position Summary:
"""

    # Add position size context if available
    if position_sizes:
        context += f"\nTotal positions with sizes: {sum(s for s in position_sizes if s)}\n"

    # Build prompt based on objective
    objective_guidance = {
        "reduce_var": "Focus on positions with highest negative marginal VaR impact. These hurt the portfolio most.",
        "reduce_tail_risk": "Prioritize positions marked as in_tail_stress=true. These are key in worst-case scenarios.",
        "reduce_concentration": "Look for large individual positions that dominate the portfolio risk.",
        "diversify": "Recommend replacing concentrated positions with diversified alternatives."
    }

    prompt = f"""{context}

{objective_guidance.get(objective, "Reduce overall portfolio risk.")}

Generate a list of specific trades to improve the portfolio. For each trade, specify:
1. ticker: the stock symbol
2. trade: "sell" (reduce position), "buy" (increase position), or "reduce" (trim size)
3. action_type: "reduce_concentration", "exit_tail_risk", "rebalance", "hedge"
4. reasoning: brief explanation (1-2 sentences)
5. expected_var_impact: your estimate of how this helps (e.g., "~0.02%" reduction)
6. priority: "high", "medium", "low"

Additional context for sizing trades:
- "sell" or "reduce" means decrease the position
- Consider the position's current size and market impact
- Recommend at most {max_trades} trades
- Ensure trades collectively move toward the objective

Respond with ONLY a valid JSON object with this structure:
{{
    "trades": [
        {{
            "ticker": "AAPL",
            "trade": "sell",
            "action_type": "reduce_concentration",
            "reasoning": "Position has highest marginal VaR impact; reducing improves portfolio",
            "expected_var_impact": "-0.03%",
            "priority": "high"
        }},
        ...
    ],
    "summary": "Brief summary of trade rationale",
    "expected_total_var_reduction": "-0.15%",
    "confidence": "high"
}}

Ensure valid JSON format. Do not include markdown code blocks."""

    message = client.messages.create(
        model="claude-opus-4-1",
        max_tokens=2048,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    # Parse the response
    response_text = message.content[0].text

    # Clean up response if it has markdown
    if response_text.startswith("```"):
        response_text = response_text.split("```")[1]
        if response_text.startswith("json"):
            response_text = response_text[4:]

    # Parse JSON
    try:
        trades_json = json.loads(response_text)
        return trades_json
    except json.JSONDecodeError as e:
        # Return error with raw text for debugging
        return {
            "error": f"Failed to parse JSON: {str(e)}",
            "raw_response": response_text,
            "trades": []
        }


def get_rebalance_recommendations(
        var_results: dict,
        tickers: List[str],
        position_sizes: List[float],
        target_var: Optional[float] = None
) -> dict:
    """
    Generate rebalancing recommendations to hit a specific VaR target.

    Args:
        var_results: Current VaR results
        tickers: List of ticker symbols
        position_sizes: Current position sizes
        target_var: Target VaR (e.g., -0.001 for 0.1%). If None, use 20% reduction.

    Returns:
        JSON with recommended position sizes
    """

    current_var = var_results['var']

    if target_var is None:
        target_var = current_var * 1.2  # More negative = worse, so 1.2 means 20% improvement

    marginal_vars = var_results['marginal_var'][0]

    # Get top positions by size
    position_info = []
    for i, (ticker, size) in enumerate(zip(tickers, position_sizes)):
        if size > 0:
            position_info.append({
                "index": i,
                "ticker": ticker,
                "current_size": size,
                "marginal_var": marginal_vars[i] if i < len(marginal_vars) else 0
            })

    # Sort by size (largest first)
    position_info.sort(key=lambda x: x['current_size'], reverse=True)

    context = f"""
You are a portfolio manager. Generate recommended position sizes for a rebalanced portfolio.

Current Portfolio:
- Current VaR: {current_var * 100:.4f}%
- Target VaR: {target_var * 100:.4f}%
- Target Improvement: {(target_var - current_var) * 100:.4f}%
- Total Positions: {len(position_sizes)}

Top Positions by Size:
{json.dumps(position_info[:20], indent=2)}

Generate recommended position sizes that achieve the target VaR while maintaining diversification.
For each position that changes, specify the new size and rationale.
"""

    prompt = f"""{context}

Respond with ONLY a valid JSON object with this structure:
{{
    "rebalancing_trades": [
        {{
            "ticker": "AAPL",
            "current_size": 1000,
            "recommended_size": 800,
            "change_percentage": "-20%",
            "rationale": "High risk concentration; reduce to diversify"
        }},
        ...
    ],
    "portfolio_summary": {{
        "total_current_value": {sum(position_sizes)},
        "total_recommended_value": <calculated>,
        "expected_new_var": "{target_var * 100:.4f}%"
    }},
    "key_changes": ["list of main portfolio shifts"],
    "implementation_notes": "How to execute this rebalance effectively"
}}

Ensure valid JSON format."""

    message = client.messages.create(
        model="claude-opus-4-1",
        max_tokens=2048,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    response_text = message.content[0].text

    # Clean markdown
    if response_text.startswith("```"):
        response_text = response_text.split("```")[1]
        if response_text.startswith("json"):
            response_text = response_text[4:]

    try:
        return json.loads(response_text)
    except json.JSONDecodeError as e:
        return {
            "error": f"Failed to parse JSON: {str(e)}",
            "raw_response": response_text
        }


def get_hedging_recommendations(
        var_results: dict,
        tickers: List[str],
        max_hedge_cost_pct: float = 0.01
) -> dict:
    """
    Generate hedging recommendations (protective instruments).

    Args:
        var_results: VaR results
        tickers: List of tickers
        max_hedge_cost_pct: Maximum cost as % of portfolio (e.g., 0.01 = 1%)

    Returns:
        JSON with hedging recommendations
    """

    marginal_vars = var_results['marginal_var'][0]
    tail_indexes = var_results['tail_indexes']

    # Get worst tail risk positions
    tail_marginal_vars = [(idx, marginal_vars[idx]) for idx in tail_indexes]
    worst_tail = sorted(tail_marginal_vars, key=lambda x: x[1])[:5]

    context = f"""
You are a risk manager. The portfolio has {len(tail_indexes)} positions exposed to tail risk.
The worst tail risk positions are:
{json.dumps([(tickers[idx] if idx < len(tickers) else f'Pos_{idx}', var) for idx, var in worst_tail], indent=2)}

Current Portfolio VaR: {var_results['var'] * 100:.4f}%
Expected Shortfall (ES): {var_results['es'] * 100:.4f}%

Generate hedge recommendations (protective strategies) to reduce tail risk.
Budget for hedges: {max_hedge_cost_pct * 100:.2f}% of portfolio notional.
"""

    prompt = f"""{context}

Recommend specific hedging instruments (puts, collars, futures, diversification).
Respond with ONLY valid JSON:
{{
    "hedges": [
        {{
            "instrument": "Put Options on QQQ",
            "target_positions": ["AAPL", "MSFT"],
            "strike_and_expiry": "5% OTM, 3-month",
            "estimated_cost": "0.25%",
            "rationale": "Tech concentration in tail risk",
            "expected_tail_reduction": "-0.04%"
        }},
        ...
    ],
    "total_hedge_cost": "0.75%",
    "expected_es_reduction": "-0.12%",
    "alternatives": "Consider rebalancing instead of hedging if cost is concern"
}}

Ensure valid JSON."""

    message = client.messages.create(
        model="claude-opus-4-1",
        max_tokens=1024,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    response_text = message.content[0].text

    if response_text.startswith("```"):
        response_text = response_text.split("```")[1]
        if response_text.startswith("json"):
            response_text = response_text[4:]

    try:
        return json.loads(response_text)
    except json.JSONDecodeError as e:
        return {
            "error": f"Failed to parse JSON: {str(e)}",
            "raw_response": response_text
        }


# Example usage and testing
if __name__ == "__main__":
    import sys

    # Load your VaR results
    var_file = "var_results.json"

    if not os.path.exists(var_file):
        print(f"Error: {var_file} not found")
        sys.exit(1)

    with open(var_file, 'r') as f:
        var_results = json.load(f)

    # Create sample tickers
    num_positions = len(var_results['marginal_var'][0])
    tickers = [f"STOCK_{i:03d}" for i in range(num_positions)]

    # Create sample position sizes (random for demo)
    import random

    position_sizes = [random.randint(0, 1000) for _ in range(num_positions)]

    print("\n" + "=" * 80)
    print("CLAUDE TRADE RECOMMENDATION ENGINE")
    print("=" * 80)

    # 1. Basic trade recommendations
    print("\n1. TRADE RECOMMENDATIONS (Reduce VaR)")
    print("-" * 80)
    trades = get_trade_recommendations(
        var_results,
        tickers,
        objective="reduce_var",
        target_reduction=0.05,
        max_trades=5,
        position_sizes=position_sizes
    )
    print(json.dumps(trades, indent=2))

    # Save to file
    with open("trade_recommendations.json", "w") as f:
        json.dump(trades, f, indent=2)
    print("\n✓ Saved to trade_recommendations.json")

    # 2. Tail risk recommendations
    print("\n2. TAIL RISK REDUCTION TRADES")
    print("-" * 80)
    tail_trades = get_trade_recommendations(
        var_results,
        tickers,
        objective="reduce_tail_risk",
        max_trades=5
    )
    print(json.dumps(tail_trades, indent=2))

    # 3. Rebalancing
    print("\n3. REBALANCING RECOMMENDATIONS")
    print("-" * 80)
    rebalance = get_rebalance_recommendations(
        var_results,
        tickers,
        position_sizes,
        target_var=var_results['var'] * 1.3  # 30% improvement
    )
    print(json.dumps(rebalance, indent=2))

    # Save rebalancing to file
    with open("rebalance_recommendations.json", "w") as f:
        json.dump(rebalance, f, indent=2)
    print("\n✓ Saved to rebalance_recommendations.json")

    # 4. Hedging
    print("\n4. HEDGING RECOMMENDATIONS")
    print("-" * 80)
    hedges = get_hedging_recommendations(
        var_results,
        tickers,
        max_hedge_cost_pct=0.01
    )
    print(json.dumps(hedges, indent=2))

    # Save hedging to file
    with open("hedge_recommendations.json", "w") as f:
        json.dump(hedges, f, indent=2)
    print("\n✓ Saved to hedge_recommendations.json")

    print("\n" + "=" * 80)
    print("Summary of Trade Recommendations")
    print("=" * 80)

    if "trades" in trades:
        print(f"\nVaR Reduction Trades: {len(trades.get('trades', []))} recommendations")
        for trade in trades.get('trades', [])[:3]:
            print(f"  - {trade.get('ticker')}: {trade.get('trade')} ({trade.get('priority')})")

    print("\nAll recommendations saved to JSON files:")
    print("  - trade_recommendations.json")
    print("  - rebalance_recommendations.json")
    print("  - hedge_recommendations.json")