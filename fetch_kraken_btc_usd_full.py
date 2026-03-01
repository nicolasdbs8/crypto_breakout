import os
import time
from datetime import datetime, timezone

import ccxt
import pandas as pd

OUT_DIR = os.environ.get("KRAKEN_OUT_DIR", "data_kraken_long")
OUT_PATH = os.path.join(OUT_DIR, "BTC.csv")

TIMEFRAME = "1d"
SLEEP_S = 0.35
START_DT = "2013-01-01"


def pick_btc_usd_symbol(ex: ccxt.Exchange) -> str:
    """
    Kraken symbols vary: could be 'BTC/USD', 'BTC/USD:USD', etc.
    We'll pick the best available BTC-USD spot symbol.
    """
    candidates = []
    for sym, m in ex.markets.items():
        if not m.get("spot", True):
            continue
        base = m.get("base")
        quote = m.get("quote")
        if base in ("BTC", "XBT") and quote == "USD":
            candidates.append(sym)

    # Prefer simpler forms first
    preferred_order = [
        "BTC/USD",
        "XBT/USD",
        "BTC/USD:USD",
        "XBT/USD:USD",
    ]
    for p in preferred_order:
        if p in candidates:
            return p

    if candidates:
        # deterministic choice
        return sorted(candidates)[0]

    # If not found, raise with hints
    hints = [s for s in ex.symbols if ("BTC" in s or "XBT" in s) and "USD" in s][:30]
    raise RuntimeError(f"No BTC/USD spot symbol found. Example symbols seen: {hints}")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    ex = ccxt.kraken({"enableRateLimit": True})
    ex.load_markets()

    symbol = pick_btc_usd_symbol(ex)
    since = ex.parse8601(f"{START_DT}T00:00:00Z")

    all_rows = []
    last_ts = None

    print(f"Fetching Kraken {symbol} {TIMEFRAME} since {START_DT} ...")

    while True:
        ohlcv = ex.fetch_ohlcv(symbol, timeframe=TIMEFRAME, since=since, limit=1000)
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

        if len(ohlcv) < 1000:
            break

        time.sleep(SLEEP_S)

    if not all_rows:
        raise RuntimeError("No OHLCV data returned from Kraken via CCXT.")

    df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(None)
    df = df[["date", "open", "high", "low", "close", "volume"]]
    df = df.sort_values("date").drop_duplicates(subset=["date"])

    df.to_csv(OUT_PATH, index=False)

    print(f"Saved {OUT_PATH} rows={len(df)} range={df['date'].min().date()}→{df['date'].max().date()}")


if __name__ == "__main__":
    main()
