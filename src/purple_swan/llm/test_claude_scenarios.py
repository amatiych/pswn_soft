"""
Portfolio Stress Scenario Generator using Claude AI
====================================================
This function generates realistic stress test scenarios for a given portfolio
and calculates the potential impact on portfolio value.
"""

import anthropic
import json
from typing import Dict, List
import os

API_KEY = os.environ.get("CLAUDE_API_KEY")


def generate_stress_scenarios(
        portfolio: Dict,
        api_key: str,
        num_scenarios: int = 5,
        scenario_types: List[str] = None
) -> Dict:
    """
    Generate stress test scenarios for a portfolio using Claude AI.

    Args:
        portfolio: Dictionary containing portfolio holdings
                   Format: {
                       "holdings": [
                           {"asset": "name", "value": amount, "type": "asset_type"}
                       ],
                       "totalValue": total
                   }
        api_key: Anthropic API key
        num_scenarios: Number of scenarios to generate (default: 5)
        scenario_types: Optional list of scenario types to focus on
                       (e.g., ["market_crash", "inflation", "geopolitical"])

    Returns:
        Dictionary containing generated scenarios with impact analysis
        Format: {
            "scenarios": [
                {
                    "name": "scenario name",
                    "description": "detailed description",
                    "probability": "Low/Medium/High",
                    "timeframe": "expected duration",
                    "impact": percentage_change,
                    "projectedValue": new_portfolio_value,
                    "affectedAssets": [
                        {"asset": "name", "impact": percentage}
                    ],
                    "indicators": ["warning signs to watch"],
                    "mitigation": ["suggested actions"]
                }
            ],
            "summary": {
                "worstCase": {"scenario": "name", "loss": amount},
                "bestCase": {"scenario": "name", "gain": amount},
                "averageImpact": percentage
            }
        }
    """

    client = anthropic.Anthropic(api_key=API_KEY)

    # Build the prompt
    scenario_focus = ""
    if scenario_types:
        scenario_focus = f"\nFocus on these scenario types: {', '.join(scenario_types)}"

    prompt = f"""You are a financial risk analyst. Generate {num_scenarios} realistic stress test scenarios for this portfolio.{scenario_focus}

Portfolio Details:
{json.dumps(portfolio, indent=2)}

For each scenario, provide:
1. A realistic market event or economic condition
2. The probability of occurrence
3. Expected timeframe/duration
4. Quantitative impact on portfolio (percentage change)
5. Which assets are most affected and by how much
6. Early warning indicators to monitor
7. Mitigation strategies

Return ONLY valid JSON with no preamble or markdown:

{{
  "scenarios": [
    {{
      "name": "Short, descriptive name",
      "description": "Detailed 2-3 sentence description of the scenario",
      "probability": "Low/Medium/High",
      "timeframe": "Duration (e.g., '3-6 months', '1-2 years')",
      "impact": -25.5,
      "projectedValue": 222750,
      "affectedAssets": [
        {{"asset": "Apple stock", "impact": -35.0}},
        {{"asset": "Tesla", "impact": -40.0}}
      ],
      "indicators": [
        "Warning sign 1",
        "Warning sign 2"
      ],
      "mitigation": [
        "Action 1",
        "Action 2"
      ]
    }}
  ],
  "summary": {{
    "worstCase": {{"scenario": "name", "loss": 50000}},
    "bestCase": {{"scenario": "name", "gain": 0}},
    "averageImpact": -15.5
  }}
}}

Make scenarios realistic and diverse (market crashes, inflation, sector-specific, geopolitical, etc.)"""

    # Call Claude API
    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}]
        )

        # Parse response
        response_text = message.content[0].text.strip()

        # More aggressive cleaning
        clean_json = response_text

        # Remove markdown code blocks
        if '```json' in clean_json:
            clean_json = clean_json.split('```json')[1]
        if '```' in clean_json:
            clean_json = clean_json.split('```')[0]

        clean_json = clean_json.strip()

        # Debug: print what we're trying to parse
        print("\n" + "=" * 70)
        print("DEBUG: Cleaned JSON to parse:")
        print("=" * 70)
        print(clean_json)
        print("=" * 70 + "\n")

        try:
            scenarios = json.loads(clean_json)
        except json.JSONDecodeError as e:
            print("=" * 70)
            print("ERROR: Failed to parse JSON response")
            print("=" * 70)
            print(f"Error at line {e.lineno}, column {e.colno}")
            print(f"Error message: {e.msg}")

            # Show the problematic area
            lines = clean_json.split('\n')
            if e.lineno <= len(lines):
                print(f"\nProblematic line {e.lineno}:")
                print(lines[e.lineno - 1])
                print(" " * (e.colno - 1) + "^")

            print(f"\nFull response:\n{clean_json}")
            print("=" * 70)
            raise

        return scenarios

    except anthropic.APIError as e:
        print(f"API Error: {e}")
        raise


# Example usage
if __name__ == "__main__":
    # Sample portfolio
    sample_portfolio = {
        "holdings": [
            {"asset": "Apple stock", "value": 100000, "type": "stocks"},
            {"asset": "Tesla", "value": 50000, "type": "stocks"},
            {"asset": "S&P 500 ETF", "value": 75000, "type": "stocks"},
            {"asset": "Corporate bonds", "value": 30000, "type": "bonds"},
            {"asset": "Bitcoin", "value": 20000, "type": "crypto"},
            {"asset": "REIT", "value": 25000, "type": "real_estate"}
        ],
        "totalValue": 300000
    }

    # Generate scenarios
    api_key = API_KEY # Replace with your actual API key

    print("=" * 70)
    print("GENERATING STRESS TEST SCENARIOS")
    print("=" * 70)

    scenarios = generate_stress_scenarios(
        portfolio=sample_portfolio,
        api_key=api_key,
        num_scenarios=5,
        scenario_types=["market_crash", "inflation", "tech_correction", "geopolitical"]
    )

    # Display results
    print("\n📊 GENERATED SCENARIOS:\n")
    for i, scenario in enumerate(scenarios['scenarios'], 1):
        print(f"\n{'=' * 70}")
        print(f"SCENARIO {i}: {scenario['name']}")
        print(f"{'=' * 70}")
        print(f"Description: {scenario['description']}")
        print(f"Probability: {scenario['probability']}")
        print(f"Timeframe: {scenario['timeframe']}")
        print(f"Overall Impact: {scenario['impact']:.2f}%")
        print(f"Projected Portfolio Value: ${scenario['projectedValue']:,.2f}")
        print(f"Potential Loss: ${sample_portfolio['totalValue'] - scenario['projectedValue']:,.2f}")

        print(f"\n📉 Most Affected Assets:")
        for asset in scenario['affectedAssets'][:3]:  # Show top 3
            print(f"   • {asset['asset']}: {asset['impact']:.2f}%")

        print(f"\n⚠️  Warning Indicators:")
        for indicator in scenario['indicators']:
            print(f"   • {indicator}")

        print(f"\n🛡️  Mitigation Strategies:")
        for mitigation in scenario['mitigation']:
            print(f"   • {mitigation}")

    # Summary
    print(f"\n\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    summary = scenarios['summary']
    print(f"Worst Case Scenario: {summary['worstCase']['scenario']}")
    print(f"   Potential Loss: ${summary['worstCase']['loss']:,.2f}")
    print(f"\nBest Case (least damage): {summary['bestCase']['scenario']}")
    if summary['bestCase']['gain'] > 0:
        print(f"   Potential Gain: ${summary['bestCase']['gain']:,.2f}")
    else:
        print(f"   Potential Loss: ${abs(summary['bestCase']['gain']):,.2f}")
    print(f"\nAverage Impact Across All Scenarios: {summary['averageImpact']:.2f}%")
    print(f"{'=' * 70}")

    # Export to JSON
    with open('stress_scenarios.json', 'w') as f:
        json.dump(scenarios, f, indent=2)

    print("\n✅ Scenarios exported to 'stress_scenarios.json'")