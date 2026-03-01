import os
import time
from datetime import datetime, timezone

import ccxt
import pandas as pd

OUT_DIR = "data_coinbase_long"
OUT_PATH = os.path.join(OUT_DIR, "BTC.csv")

TIMEFRAME = "1d"
START_DATE = "2013-01-01"

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    ex = ccxt.coinbase()
    ex.load_markets()

    symbol = "BTC/USD"

    since = ex.parse8601(f"{START_DATE}T00:00:00Z")

    all_rows = []
    last_ts = None

    print(f"Fetching Coinbase {symbol} since {START_DATE} ...")

    while True:
        ohlcv = ex.fetch_ohlcv(symbol, timeframe=TIMEFRAME, since=since, limit=300)
        if not ohlcv:
            break

        all_rows.extend(ohlcv)

        new_last = ohlcv[-1][0]
        last_date = datetime.fromtimestamp(new_last / 1000, tz=timezone.utc).date()
        print("... last:", last_date, "rows_total:", len(all_rows))

        if last_ts is not None and new_last <= last_ts:
            break

        last_ts = new_last
        since = new_last + 1

        if len(ohlcv) < 300:
            break

        time.sleep(0.3)

    df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(None)
    df = df[["date", "open", "high", "low", "close", "volume"]]
    df = df.sort_values("date").drop_duplicates(subset=["date"])

    df.to_csv(OUT_PATH, index=False)

    print(f"Saved {OUT_PATH} rows={len(df)} range={df['date'].min().date()}→{df['date'].max().date()}")

if __name__ == "__main__":
    main()
