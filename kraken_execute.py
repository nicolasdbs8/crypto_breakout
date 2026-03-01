import os
from pathlib import Path
import pandas as pd
import ccxt

ORDERS_PATH = Path("data/outputs/orders_today.csv")

RISK_PER_TRADE = 0.0125  # 1.25%
ATR_MULT = 2.0

LIVE_TRADING = os.environ.get("LIVE_TRADING", "0").strip()


def connect():
    return ccxt.kraken({
        "apiKey": os.environ["KRAKEN_API_KEY"],
        "secret": os.environ["KRAKEN_API_SECRET"],
        "enableRateLimit": True,
    })


def main():
    if not ORDERS_PATH.exists() or ORDERS_PATH.stat().st_size == 0:
        print("[kraken_execute] no orders.")
        return

    df = pd.read_csv(ORDERS_PATH)
    if df.empty:
        print("[kraken_execute] empty orders.")
        return

    ex = connect()
    ex.load_markets()

    balance = ex.fetch_balance()
    usdc_total = float(balance["total"].get("USDC", 0))

    print(f"USDC total equity: {usdc_total}")

    for _, row in df.iterrows():
        action = row["action"]
        symbol = row["symbol"]
        pair = f"{symbol}/USDC"

        if action != "BUY":
            print(f"Skipping non-BUY: {symbol}")
            continue

        atr = row.get("atr_ref")
        if atr == "" or pd.isna(atr):
            print(f"No ATR for {symbol}, skip.")
            continue

        atr = float(atr)

        ticker = ex.fetch_ticker(pair)
        price = float(ticker["last"])

        stop = price - ATR_MULT * atr
        stop_distance = price - stop

        if stop_distance <= 0:
            print(f"Invalid stop for {symbol}")
            continue

        risk_amount = usdc_total * RISK_PER_TRADE
        qty = risk_amount / stop_distance

        qty = float(ex.amount_to_precision(pair, qty))

        print("-----")
        print(f"Symbol: {symbol}")
        print(f"Price: {price}")
        print(f"ATR: {atr}")
        print(f"Stop: {stop}")
        print(f"Risk amount: {risk_amount}")
        print(f"Qty: {qty}")

        if LIVE_TRADING != "1":
            print("DRY RUN - not sending order")
            continue

        order = ex.create_market_buy_order(pair, qty)
        print(f"Order sent: {order['id']}")

    print("Done.")


if __name__ == "__main__":
    main()
