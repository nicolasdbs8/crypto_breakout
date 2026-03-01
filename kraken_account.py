import os
import ccxt

def get_kraken_balance():
    api_key = os.environ.get("KRAKEN_API_KEY")
    api_secret = os.environ.get("KRAKEN_API_SECRET")

    exchange = ccxt.kraken({
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
    })

    balance = exchange.fetch_balance()
    
    usdc = balance["total"].get("USDC", 0)
    eur = balance["total"].get("EUR", 0)

    print("USDC balance:", usdc)
    print("EUR balance:", eur)

    return usdc

if __name__ == "__main__":
    get_kraken_balance()
