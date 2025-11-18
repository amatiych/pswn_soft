"""
VaR Analysis Interpreter using Claude AI
=========================================
This function takes your calculated VaR metrics and uses Claude to:
1. Interpret the risk decomposition
2. Generate actionable recommendations
3. Explain hedging strategies
4. Create natural language risk reports
"""

import anthropic
import json
from typing import Dict, List
import os

API_KEY = os.environ.get("CLAUDE_API_KEY")


def interpret_var_analysis(
        portfolio_var: float,
        marginal_vars: Dict[str, float],
        incremental_vars: Dict[str, float],
        portfolio_value: float,
        holdings: List[Dict],
        api_key: str,
        confidence_level: float = 0.95
) -> Dict:
    """
    Use Claude to interpret VaR calculations and generate insights.

    Args:
        portfolio_var: Total portfolio VaR (in dollars)
        marginal_vars: Dict of {asset_name: marginal_var} showing effect of removing each position
        incremental_vars: Dict of {asset_name: incremental_var} showing contribution to total VaR
        portfolio_value: Total portfolio value
        holdings: List of holdings [{"asset": name, "value": amount, "type": type}]
        api_key: Anthropic API key
        confidence_level: VaR confidence level (default 0.95)

    Returns:
        Dictionary containing:
        - interpretation: Natural language explanation of VaR results
        - riskDrivers: Ranked list of assets driving portfolio risk
        - recommendations: Specific actions to reduce risk
        - hedgingStrategies: Concrete hedging suggestions
        - whatIfScenarios: Analysis of potential changes
        - executiveSummary: Brief summary for stakeholders
    """

    client = anthropic.Anthropic(api_key=api_key)

    # Calculate some derived metrics for context
    var_as_pct = (portfolio_var / portfolio_value) * 100

    # Rank assets by incremental VaR contribution
    sorted_incremental = sorted(
        incremental_vars.items(),
        key=lambda x: abs(x[1]),
        reverse=True
    )

    # Build comprehensive prompt
    prompt = f"""You are a senior risk manager analyzing Value at Risk (VaR) results for a portfolio.

PORTFOLIO OVERVIEW:
- Total Value: ${portfolio_value:,.2f}
- Number of Positions: {len(holdings)}
- Holdings: {json.dumps(holdings, indent=2)}

VAR METRICS ({confidence_level * 100}% confidence level):
- Portfolio VaR: ${portfolio_var:,.2f} ({var_as_pct:.2f}% of portfolio)
- This means: With {confidence_level * 100}% confidence, the portfolio will not lose more than ${portfolio_var:,.2f} in the next period

MARGINAL VAR (effect of removing each position):
{json.dumps(marginal_vars, indent=2)}

INCREMENTAL VAR (contribution of each position to total VaR):
{json.dumps(incremental_vars, indent=2)}

Note: 
- Marginal VaR shows how much total VaR would DECREASE if we removed that position
- Incremental VaR shows how much that position CONTRIBUTES to current VaR
- Negative incremental VaR means the position actually REDUCES overall portfolio risk (diversification benefit)

Provide a comprehensive analysis in JSON format ONLY (no markdown, no preamble):

{{
  "interpretation": {{
    "portfolioRiskLevel": "Low/Medium/High/Very High (based on VaR as % of portfolio)",
    "keyInsights": [
      "Insight 1 about the VaR decomposition",
      "Insight 2 about risk concentration",
      "Insight 3 about diversification effects"
    ],
    "surprisingFindings": [
      "Any unexpected results from the VaR analysis"
    ]
  }},

  "riskDrivers": [
    {{
      "asset": "asset name",
      "contribution": incremental_var_value,
      "percentOfTotalRisk": percentage,
      "explanation": "Why this asset contributes so much/little to risk",
      "concern": "Low/Medium/High"
    }}
  ],

  "recommendations": [
    {{
      "priority": "Critical/High/Medium/Low",
      "action": "Specific action to take",
      "expectedImpact": "Quantify expected VaR reduction",
      "rationale": "Why this action will help",
      "implementation": "How to implement this"
    }}
  ],

  "hedgingStrategies": [
    {{
      "strategy": "Specific hedging approach",
      "targetAssets": ["assets to hedge"],
      "instruments": ["specific instruments to use (options, futures, etc)"],
      "expectedVarReduction": "estimated reduction in dollars or percentage",
      "cost": "estimated cost consideration",
      "pros": ["advantages"],
      "cons": ["disadvantages"]
    }}
  ],

  "whatIfScenarios": [
    {{
      "scenario": "What if we reduce position X by 50%?",
      "expectedVarChange": "estimated change",
      "rationale": "why this would help/hurt"
    }}
  ],

  "diversificationAnalysis": {{
    "currentLevel": "Poor/Moderate/Good/Excellent",
    "diversificationBenefits": [
      "Which positions provide diversification (negative incremental VaR)"
    ],
    "concentrationRisks": [
      "Which positions create concentration risk"
    ],
    "suggestions": [
      "How to improve diversification"
    ]
  }},

  "executiveSummary": "2-3 sentence summary for senior management highlighting the most critical finding and recommended action"
}}

Be specific and quantitative in your recommendations. Reference actual asset names and VaR numbers."""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = message.content[0].text.strip()

        # Clean JSON
        clean_json = response_text
        if '```json' in clean_json:
            clean_json = clean_json.split('```json')[1].split('```')[0]
        elif '```' in clean_json:
            clean_json = clean_json.split('```')[1].split('```')[0]

        clean_json = clean_json.strip()

        analysis = json.loads(clean_json)
        return analysis

    except json.JSONDecodeError as e:
        print(f"JSON Parse Error: {e}")
        print(f"Response was:\n{response_text}")
        raise
    except anthropic.APIError as e:
        print(f"API Error: {e}")
        raise


def generate_risk_report(
        analysis: Dict,
        portfolio_var: float,
        portfolio_value: float,
        output_format: str = "markdown"
) -> str:
    """
    Generate a formatted risk report from the analysis.

    Args:
        analysis: Output from interpret_var_analysis()
        portfolio_var: Portfolio VaR value
        portfolio_value: Total portfolio value
        output_format: "markdown" or "text"

    Returns:
        Formatted report string
    """

    if output_format == "markdown":
        report = f"""# Portfolio Risk Analysis Report

## Executive Summary
{analysis['executiveSummary']}

## Portfolio VaR Metrics
- **Portfolio Value**: ${portfolio_value:,.2f}
- **Value at Risk (95%)**: ${portfolio_var:,.2f}
- **VaR as % of Portfolio**: {(portfolio_var / portfolio_value) * 100:.2f}%
- **Risk Level**: {analysis['interpretation']['portfolioRiskLevel']}

## Key Insights
"""
        for insight in analysis['interpretation']['keyInsights']:
            report += f"- {insight}\n"

        if analysis['interpretation']['surprisingFindings']:
            report += f"\n### Surprising Findings\n"
            for finding in analysis['interpretation']['surprisingFindings']:
                report += f"- {finding}\n"

        report += f"\n## Risk Drivers\n"
        for driver in analysis['riskDrivers'][:5]:  # Top 5
            report += f"\n### {driver['asset']}\n"
            report += f"- **Contribution to Risk**: ${driver['contribution']:,.2f} ({driver['percentOfTotalRisk']:.1f}%)\n"
            report += f"- **Concern Level**: {driver['concern']}\n"
            report += f"- **Explanation**: {driver['explanation']}\n"

        report += f"\n## Recommendations\n"
        for rec in analysis['recommendations']:
            report += f"\n### {rec['priority']} Priority: {rec['action']}\n"
            report += f"- **Expected Impact**: {rec['expectedImpact']}\n"
            report += f"- **Rationale**: {rec['rationale']}\n"
            report += f"- **Implementation**: {rec['implementation']}\n"

        report += f"\n## Hedging Strategies\n"
        for hedge in analysis['hedgingStrategies']:
            report += f"\n### {hedge['strategy']}\n"
            report += f"- **Target Assets**: {', '.join(hedge['targetAssets'])}\n"
            report += f"- **Instruments**: {', '.join(hedge['instruments'])}\n"
            report += f"- **Expected VaR Reduction**: {hedge['expectedVarReduction']}\n"
            report += f"- **Cost Consideration**: {hedge['cost']}\n"
            report += f"- **Pros**: {', '.join(hedge['pros'])}\n"
            report += f"- **Cons**: {', '.join(hedge['cons'])}\n"

        report += f"\n## Diversification Analysis\n"
        div = analysis['diversificationAnalysis']
        report += f"- **Current Level**: {div['currentLevel']}\n\n"
        report += f"**Diversification Benefits**:\n"
        for benefit in div['diversificationBenefits']:
            report += f"- {benefit}\n"
        report += f"\n**Concentration Risks**:\n"
        for risk in div['concentrationRisks']:
            report += f"- {risk}\n"

        return report

    else:  # text format
        report = "=" * 70 + "\n"
        report += "PORTFOLIO RISK ANALYSIS REPORT\n"
        report += "=" * 70 + "\n\n"
        report += f"Executive Summary:\n{analysis['executiveSummary']}\n\n"
        report += f"Portfolio VaR: ${portfolio_var:,.2f}\n"
        report += f"Risk Level: {analysis['interpretation']['portfolioRiskLevel']}\n"
        report += "=" * 70 + "\n"
        return report


# Example usage
if __name__ == "__main__":

    # Your calculated VaR metrics (these would come from your quantitative code)
    portfolio_var = 45000.0  # $45k VaR at 95% confidence
    portfolio_value = 300000.0

    # Marginal VaR: how much VaR decreases if we remove each position
    marginal_vars = {
        "Apple stock": 15000,  # Removing Apple would reduce VaR by $15k
        "Tesla": 12000,  # Removing Tesla would reduce VaR by $12k
        "S&P 500 ETF": 8000,
        "Corporate bonds": -2000,  # Removing bonds would INCREASE VaR (diversifier)
        "Bitcoin": 18000,  # Biggest marginal impact
        "REIT": 3000
    }

    # Incremental VaR: how much each position contributes to total VaR
    incremental_vars = {
        "Apple stock": 12000,  # Apple contributes $12k to total VaR
        "Tesla": 10000,
        "S&P 500 ETF": 9000,
        "Corporate bonds": -3000,  # Bonds actually REDUCE total VaR
        "Bitcoin": 20000,  # Biggest contributor
        "REIT": 2000
    }

    holdings = [
        {"asset": "Apple stock", "value": 100000, "type": "stocks"},
        {"asset": "Tesla", "value": 50000, "type": "stocks"},
        {"asset": "S&P 500 ETF", "value": 75000, "type": "stocks"},
        {"asset": "Corporate bonds", "value": 30000, "type": "bonds"},
        {"asset": "Bitcoin", "value": 20000, "type": "crypto"},
        {"asset": "REIT", "value": 25000, "type": "real_estate"}
    ]

    # Get Claude's interpretation
    api_key = API_KEY

    print("=" * 70)
    print("INTERPRETING VAR ANALYSIS WITH CLAUDE")
    print("=" * 70)

    analysis = interpret_var_analysis(
        portfolio_var=portfolio_var,
        marginal_vars=marginal_vars,
        incremental_vars=incremental_vars,
        portfolio_value=portfolio_value,
        holdings=holdings,
        api_key=api_key,
        confidence_level=0.95
    )

    # Display results
    print("\n" + "=" * 70)
    print("EXECUTIVE SUMMARY")
    print("=" * 70)
    print(analysis['executiveSummary'])

    print("\n" + "=" * 70)
    print("KEY INSIGHTS")
    print("=" * 70)
    for insight in analysis['interpretation']['keyInsights']:
        print(f"• {insight}")

    print("\n" + "=" * 70)
    print("TOP RISK DRIVERS")
    print("=" * 70)
    for driver in analysis['riskDrivers'][:3]:
        print(f"\n{driver['asset']}:")
        print(f"  Contribution: ${driver['contribution']:,.2f} ({driver['percentOfTotalRisk']:.1f}%)")
        print(f"  Concern: {driver['concern']}")
        print(f"  {driver['explanation']}")

    print("\n" + "=" * 70)
    print("CRITICAL RECOMMENDATIONS")
    print("=" * 70)
    for rec in [r for r in analysis['recommendations'] if r['priority'] in ['Critical', 'High']]:
        print(f"\n[{rec['priority']}] {rec['action']}")
        print(f"  Expected Impact: {rec['expectedImpact']}")
        print(f"  How: {rec['implementation']}")

    print("\n" + "=" * 70)
    print("HEDGING STRATEGIES")
    print("=" * 70)
    for hedge in analysis['hedgingStrategies']:
        print(f"\n{hedge['strategy']}:")
        print(f"  Instruments: {', '.join(hedge['instruments'])}")
        print(f"  Expected VaR Reduction: {hedge['expectedVarReduction']}")
        print(f"  Key Benefit: {hedge['pros'][0]}")

    # Generate full report
    print("\n" + "=" * 70)
    print("GENERATING MARKDOWN REPORT")
    print("=" * 70)

    report = generate_risk_report(
        analysis=analysis,
        portfolio_var=portfolio_var,
        portfolio_value=portfolio_value,
        output_format="markdown"
    )

    # Save report
    with open('var_analysis_report.md', 'w') as f:
        f.write(report)

    print("✅ Full report saved to 'var_analysis_report.md'")

    # Save JSON
    with open('var_analysis.json', 'w') as f:
        json.dump(analysis, f, indent=2)

    print("✅ Analysis JSON saved to 'var_analysis.json'")