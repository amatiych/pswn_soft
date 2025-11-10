import anthropic
API_KEY='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'
def generate_trading_signal(ticker, portfolio_position=None):
    client = anthropic.Anthropic(api_key=API_KEY)

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2000,
        messages=[{
            "role": "user",
            "content": f"""Search for latest news on {ticker} and provide:
            1. Trading signal (BUY/SELL/HOLD) with conviction level
            2. Key catalysts from news
            3. Risk factors to monitor
            4. Suggested position sizing considerations

            {f"Current position: {portfolio_position}" if portfolio_position else ""}

            Be specific and quantitative where possible."""
        }],
        tools=[{
            "type": "web_search_20250305",
            "name": "web_search"
        }]
    )

    return message

if __name__ == "__main__":
    res = generate_trading_signal("TSLA")
    print(res)