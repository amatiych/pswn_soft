"""
Portfolio VaR Analysis Agent using Claude API

This demonstrates how to build an agent that:
1. Loads VaR results
2. Processes them with Claude to generate insights
3. Responds to natural language queries about portfolio risk

Prerequisites:
    pip install anthropic

Set your API key:
    export ANTHROPIC_API_KEY="your-key-here"
"""

import json
import os
from typing import Optional
import anthropic

from purple_swan.data.models.models import Portfolio,Position

# Initialize the client
client = anthropic.Anthropic(api_key=os.environ.get("CLAUDE_API_KEY"))


def load_var_results(filepath: str) -> dict:
    """Load VaR results from JSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)


def analyze_var_profile(var_results: dict,portfolio: Portfolio) -> str:
    """
    Generate a comprehensive risk profile analysis.

    Args:
        var_results: VaR calculation results dictionary
        tickers: Optional list of ticker symbols corresponding to positions

    Returns:
        Claude's analysis as string
    """

    portfolio_name = portfolio.name
    portfolio_positions = portfolio.positions
    portfolio_df = portfolio.position_df()
    tickers = list(portfolio_df.index)


    # Extract key statistics
    total_positions = len(var_results['marginal_var'][0])
    confidence_level = var_results['ci']
    var_value = var_results['var']
    es_value = var_results['es']
    tail_positions_count = len(var_results['tail_indexes'])

    # Find top risk contributors
    marginal_vars = var_results['marginal_var'][0]
    top_risk_indices = sorted(
        range(len(marginal_vars)),
        key=lambda i: marginal_vars[i]
    )[:10]

    top_risks_values = [marginal_vars[i] for i in top_risk_indices]

    # Build context for Claude
    context = f"""
    Portfolio VaR Analysis Results:
    
    Portfolio Name: {portfolio_name}
    Positions: {portfolio_df}
    
    Metrics:
    - Value at Risk (95% CI): {var_value:.6f} ({var_value * 100:.4f}%)
    - Expected Shortfall: {es_value:.6f} ({es_value * 100:.4f}%)
    - Confidence Level: {confidence_level * 100:.0f}%
    - Total Positions: {total_positions}
    - Positions in Tail Scenario: {tail_positions_count}

    Top 10 Positions by Marginal VaR Impact (most negative = highest risk):
    {list(enumerate(top_risks_values, 1))}

    Statistics on Marginal VaR:
    - Mean: {sum(marginal_vars) / len(marginal_vars):.6e}
    - Min (highest risk): {min(marginal_vars):.6e}
    - Max (lowest risk): {max(marginal_vars):.6e}
    """

    if tickers and len(tickers) >= max(top_risk_indices):
        context += f"\n    Top Risk Contributors by Ticker:\n"
        for rank, idx in enumerate(top_risk_indices, 1):
            ticker = tickers[idx] if idx < len(tickers) else f"Position_{idx}"
            context += f"    {rank}. {ticker}: {marginal_vars[idx]:.6e}\n"

    prompt = f"""{context}

    Provide a concise but insightful analysis of this portfolio's risk profile:
    1. What is the overall risk characterization? (Conservative, moderate, aggressive)
    2. Is risk concentrated in a few positions or distributed?
    3. What do the tail positions tell us about stress scenarios?
    4. What's one key risk management action to consider?
    5. Suggest positions to be sold / bought to optimize risk .  
    6. produce json that has an  list that shows trade recommendations: ticker,buy/sell, change in weight

    Keep the response to 4-5 sentences max."""

    message = client.messages.create(
        model="claude-opus-4-1",
        max_tokens=512,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    return message.content[0].text


def compare_var_scenarios(baseline: dict, scenario: dict, scenario_name: str = "Modified") -> str:
    """
    Compare two VaR results and explain the difference.

    Args:
        baseline: Original VaR results
        scenario: New VaR results after modification
        scenario_name: Name of the scenario

    Returns:
        Claude's comparison analysis
    """

    var_change = scenario['var'] - baseline['var']
    es_change = scenario['es'] - baseline['es']
    var_pct_change = (var_change / abs(baseline['var'])) * 100 if baseline['var'] != 0 else 0

    tail_change = len(scenario['tail_indexes']) - len(baseline['tail_indexes'])

    prompt = f"""
    Portfolio Risk Comparison:

    Baseline:
    - VaR: {baseline['var']:.6f}
    - Expected Shortfall: {baseline['es']:.6f}
    - Tail Positions: {len(baseline['tail_indexes'])}

    {scenario_name} Scenario:
    - VaR: {scenario['var']:.6f}
    - Expected Shortfall: {scenario['es']:.6f}
    - Tail Positions: {len(scenario['tail_indexes'])}

    Changes:
    - VaR Change: {var_change:.6e} ({var_pct_change:+.2f}%)
    - ES Change: {es_change:.6e}
    - Tail Positions Change: {tail_change:+d}

    Interpret these changes in plain English for a portfolio manager.
    Is this scenario an improvement or deterioration? Why?
    """

    message = client.messages.create(
        model="claude-opus-4-1",
        max_tokens=512,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    return message.content[0].text


def query_var_results(var_results: dict, question: str, tickers: Optional[list] = None) -> str:
    """
    Answer natural language questions about VaR results.

    Args:
        var_results: VaR results dictionary
        question: User's natural language question
        tickers: Optional list of tickers

    Returns:
        Claude's answer
    """

    # Provide Claude with relevant data summary
    marginal_vars = var_results['marginal_var'][0]
    top_10_worst = sorted(
        range(len(marginal_vars)),
        key=lambda i: marginal_vars[i]
    )[:10]

    context = f"""
    Portfolio Context:
    - Total positions: {len(marginal_vars)}
    - VaR (95% CI): {var_results['var']:.4%}
    - Expected Shortfall: {var_results['es']:.4%}
    - Tail stress positions: {len(var_results['tail_indexes'])}

    Top 10 risk contributors (marginal VaR):
    {[(i, marginal_vars[i]) for i in top_10_worst]}
    """

    if tickers:
        context += f"\n    Ticker identities of top contributors:\n"
        for rank, idx in enumerate(top_10_worst, 1):
            if idx < len(tickers):
                context += f"    {rank}. {tickers[idx]}\n"

    prompt = f"""{context}

    Manager Question: {question}

    Answer based on the VaR data provided. Be specific and concise."""

    message = client.messages.create(
        model="claude-opus-4-1",
        max_tokens=512,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    return message.content[0].text


def generate_daily_risk_report(var_results: dict, date: str = None, tickers: Optional[list] = None) -> str:
    """
    Generate a formatted daily risk report.

    Args:
        var_results: VaR results
        date: Report date
        tickers: Optional list of tickers

    Returns:
        Formatted risk report
    """

    date_str = date if date else "Latest"

    # Get the analysis
    analysis = analyze_var_profile(var_results, tickers)

    # Build the report
    report = f"""
╔══════════════════════════════════════════════════════════════╗
║              PORTFOLIO RISK REPORT - {date_str}                 ║
╚══════════════════════════════════════════════════════════════╝

RISK METRICS
───────────────────────────────────────────────────────────────
  Value at Risk (95% CI):        {var_results['var'] * 100:>8.4f}%
  Expected Shortfall (ES):       {var_results['es'] * 100:>8.4f}%
  Positions in Analysis:          {len(var_results['marginal_var'][0]):>8d}
  Stress Scenario Positions:      {len(var_results['tail_indexes']):>8d}

RISK ANALYSIS
───────────────────────────────────────────────────────────────
{analysis}

TOP RISK DRIVERS
───────────────────────────────────────────────────────────────
"""

    marginal_vars = var_results['marginal_var'][0]
    top_5 = sorted(range(len(marginal_vars)), key=lambda i: marginal_vars[i])[:5]

    for rank, idx in enumerate(top_5, 1):
        ticker = tickers[idx] if tickers and idx < len(tickers) else f"Pos_{idx}"
        report += f"  {rank}. {ticker:<8s} - Marginal VaR: {marginal_vars[idx]:.6e}\n"

    report += """
        ───────────────────────────────────────────────────────────────
        Report generated by Claude Portfolio Risk Agent
        """

    return report



# Example usage
if __name__ == "__main__":
    import sys

    # Load sample data
    var_file = "var_results.json"

    if not os.path.exists(var_file):
        print(f"Error: {var_file} not found")
        print("Please provide a var_results.json file in the current directory")
        sys.exit(1)

    var_results = load_var_results(var_file)

    # Example tickers (replace with your actual tickers)
    sample_tickers = [f"STOCK_{i}" for i in range(len(var_results['marginal_var'][0]))]

    print("\n" + "=" * 70)
    print("Portfolio VaR Analysis Agent")
    print("=" * 70)

    # 1. Generate analysis
    print("\n1. COMPREHENSIVE RISK ANALYSIS")
    print("-" * 70)
    analysis = analyze_var_profile(var_results, sample_tickers)
    print(analysis)

    # 2. Generate report
    print("\n2. FORMATTED DAILY REPORT")
    print("-" * 70)
    report = generate_daily_risk_report(var_results, "2024-11-26", sample_tickers)
    print(report)

    # 3. Query examples
    print("\n3. EXAMPLE QUERIES")
    print("-" * 70)

    queries = [
        "What would be the best way to reduce portfolio VaR?",
        "Are there any concentration risks in my portfolio?",
        "What does the tail risk analysis tell us about extreme scenarios?",
    ]

    for q in queries:
        print(f"\nQ: {q}")
        print(f"A: {query_var_results(var_results, q, sample_tickers)[:300]}...")

    # 4. Interactive mode
    print("\n4. INTERACTIVE MODE")
    print("-" * 70)
    print("Enter your questions about the portfolio (type 'quit' to exit):")

    while True:
        user_input = input("\nYour question: ").strip()
        if user_input.lower() == 'quit':
            break
        if user_input:
            answer = query_var_results(var_results, user_input, sample_tickers)
            print(f"\nAnalysis: {answer}\n")