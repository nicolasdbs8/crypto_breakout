# update_data_kraken_recent.py
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import ccxt
import pandas as pd


@dataclass
class Pair:
    asset: str          # our internal name / filename e.g. "BTC"
    ccxt_symbol: str    # e.g. "XBT/USDT"


def _ms() -> int:
    return int(time.time() * 1000)


def _fetch_ohlcv_all(exchange, symbol: str, timeframe: str, since_ms: int) -> List[List[float]]:
    """
    Fetch OHLCV in a loop until now.
    Returns list of [timestamp, open, high, low, close, volume]
    """
    all_rows: List[List[float]] = []
    now = _ms()
    since = since_ms

    while True:
        rows = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=720)
        if not rows:
            break

        # append & advance
        all_rows.extend(rows)
        last_ts = rows[-1][0]

        # if no progress -> stop
        if last_ts <= since:
            break

        since = last_ts + 1

        # stop if we're basically up-to-date
        if last_ts >= now - 2 * 24 * 60 * 60 * 1000:
            break

        # be nice to API
        time.sleep(exchange.rateLimit / 1000)

    # dedupe by timestamp
    seen = set()
    dedup = []
    for r in all_rows:
        ts = r[0]
        if ts in seen:
            continue
        seen.add(ts)
        dedup.append(r)
    return dedup


def _to_df(rows: List[List[float]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["date"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.date.astype(str)
    df = df.drop(columns=["ts"])
    # reorder to match your loader expectation
    return df[["date", "open", "high", "low", "close", "volume"]]


def main():
    # config
    out_dir = Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)

    lookback_days = int(os.environ.get("KRAKEN_LOOKBACK_DAYS", "700"))  # enough for SMA200 + buffers
    timeframe = "1d"
    since_ms = _ms() - lookback_days * 24 * 60 * 60 * 1000

    pairs: List[Pair] = [
        Pair("BTC", "XBT/USDT"),
        Pair("ETH", "ETH/USDT"),
        Pair("SOL", "SOL/USDT"),
        Pair("BNB", "BNB/USDT"),
        Pair("XRP", "XRP/USDT"),
        Pair("ADA", "ADA/USDT"),
        Pair("AVAX", "AVAX/USDT"),
        Pair("LINK", "LINK/USDT"),
        Pair("DOT", "DOT/USDT"),
        Pair("LTC", "LTC/USDT"),
        Pair("BCH", "BCH/USDT"),
        Pair("ATOM", "ATOM/USDT"),
    ]

    ex = ccxt.kraken({"enableRateLimit": True})
    ex.load_markets()

    summary: List[Dict[str, str]] = []

    for p in pairs:
        asset = p.asset
        symbol = p.ccxt_symbol
        csv_path = out_dir / f"{asset}.csv"

        if symbol not in ex.markets:
            msg = f"SKIP (not on Kraken): {symbol}"
            print(f"{asset}: {msg}")
            summary.append({"asset": asset, "status": "SKIP", "detail": msg})
            continue

        try:
            rows = _fetch_ohlcv_all(ex, symbol, timeframe, since_ms)
            if not rows:
                msg = "NO DATA returned"
                print(f"{asset}: {msg}")
                summary.append({"asset": asset, "status": "ERROR", "detail": msg})
                continue

            df = _to_df(rows)
            df.to_csv(csv_path, index=False)

            msg = f"saved {csv_path} rows={len(df)} range={df['date'].iloc[0]}->{df['date'].iloc[-1]}"
            print(f"{asset}: {msg}")
            summary.append({"asset": asset, "status": "OK", "detail": msg})

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            print(f"{asset}: ERROR {msg}")
            summary.append({"asset": asset, "status": "ERROR", "detail": msg})

    print("\n=== KRAKEN RECENT UPDATE SUMMARY ===")
    print(pd.DataFrame(summary).to_string(index=False))


if __name__ == "__main__":
    main()
