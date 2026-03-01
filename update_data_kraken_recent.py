# update_data_kraken_recent.py
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Optional

import ccxt
import pandas as pd

DATA_DIR = Path("data")
TIMEFRAME = "1d"
QUOTE = "USDC"
LOOKBACK_DAYS_DEFAULT = 900

# We always write CSV filenames by asset (BTC.csv, ETH.csv, etc.)
ASSETS = ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "LINK", "DOT", "LTC", "BCH", "ATOM"]


def _kraken() -> ccxt.Exchange:
    return ccxt.kraken({"enableRateLimit": True})


def _read_existing_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists() or path.stat().st_size == 0:
        return None
    df = pd.read_csv(path)
    if "timestamp" not in df.columns:
        return None
    df = df.dropna(subset=["timestamp"])
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["timestamp"])
    df["timestamp"] = df["timestamp"].astype(int)
    df = df.sort_values("timestamp").drop_duplicates("timestamp", keep="last")
    return df


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    df = df.sort_values("timestamp").drop_duplicates("timestamp", keep="last")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _fetch_ohlcv_daily(ex: ccxt.Exchange, market: str, since_ms: int) -> pd.DataFrame:
    all_rows = []
    since = since_ms

    while True:
        rows = ex.fetch_ohlcv(market, timeframe=TIMEFRAME, since=since, limit=720)
        if not rows:
            break

        all_rows.extend(rows)
        last_ts = rows[-1][0]
        next_since = last_ts + 24 * 60 * 60 * 1000
        if next_since <= since:
            break
        since = next_since

        time.sleep(ex.rateLimit / 1000.0)

        if len(rows) < 10:
            break

    df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = df["timestamp"].astype(int)
    return df


def _candidate_markets(asset: str) -> list[str]:
    """
    Kraken/ccxt symbol naming can vary (BTC vs XBT).
    We try a small robust set of candidates.
    """
    asset = asset.upper().strip()

    if asset == "BTC":
        # Try ccxt-unified first (often BTC/USDC exists even if Kraken uses XBT internally)
        return ["BTC/USDC", "XBT/USDC", "XXBT/USDC"]
    else:
        return [f"{asset}/{QUOTE}"]


def _resolve_market(markets: dict, asset: str) -> Optional[str]:
    for m in _candidate_markets(asset):
        if m in markets:
            return m
    return None


def main() -> None:
    lookback_days = int(os.environ.get("KRAKEN_LOOKBACK_DAYS", LOOKBACK_DAYS_DEFAULT))
    now_ms = int(time.time() * 1000)
    since_ms = now_ms - lookback_days * 24 * 60 * 60 * 1000

    ex = _kraken()
    markets = ex.load_markets()

    ok = 0
    skipped = 0

    for asset in ASSETS:
        market = _resolve_market(markets, asset)
        if market is None:
            print(f"[SKIP] {asset}: no {asset}/{QUOTE} market found. Tried: {_candidate_markets(asset)}")
            skipped += 1
            continue

        out_path = DATA_DIR / f"{asset}.csv"
        existing = _read_existing_csv(out_path)

        since_use = since_ms
        if existing is not None and len(existing) > 0:
            last_ts = int(existing["timestamp"].iloc[-1])
            since_use = max(since_ms, last_ts - 5 * 24 * 60 * 60 * 1000)

        df_new = _fetch_ohlcv_daily(ex, market, since_use)
        if df_new.empty:
            print(f"[WARN] {asset}: no data returned for {market}")
            continue

        merged = df_new if existing is None else pd.concat([existing, df_new], ignore_index=True)
        _write_csv(out_path, merged)
        print(f"[OK] {asset}: wrote {out_path} rows={len(merged)} (market={market})")
        ok += 1

    print(f"DONE. ok={ok} skipped={skipped} lookback_days={lookback_days}")


if __name__ == "__main__":
    main()
