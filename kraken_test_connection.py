import os
import ccxt

def main():
    api_key = os.environ.get("KRAKEN_API_KEY")
    api_secret = os.environ.get("KRAKEN_API_SECRET")

    if not api_key or not api_secret:
        raise Exception("API keys not found in environment variables.")

    exchange = ccxt.kraken({
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
    })

    try:
        balance = exchange.fetch_balance()
        print("Connection successful.")
        print("Balances:")
        for asset, data in balance["total"].items():
            if data and data > 0:
                print(asset, ":", data)

    except Exception as e:
        print("Connection failed.")
        print(str(e))

if __name__ == "__main__":
    main()
