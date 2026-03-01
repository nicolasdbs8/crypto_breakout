# update_data_kraken_recent.py
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import ccxt
import pandas as pd

DATA_DIR = Path("data")          # garde ton pipeline inchangé
TIMEFRAME = "1d"
QUOTE = "USDC"

# IMPORTANT: Kraken uses XBT for BTC
SYMBOLS = {
    "BTC": "XBT/USDC",
    "ETH": "ETH/USDC",
    "SOL": "SOL/USDC",
    "BNB": "BNB/USDC",
    "XRP": "XRP/USDC",
    "ADA": "ADA/USDC",
    "AVAX": "AVAX/USDC",
    "LINK": "LINK/USDC",
    "DOT": "DOT/USDC",
    "LTC": "LTC/USDC",
    "BCH": "BCH/USDC",
    "ATOM": "ATOM/USDC",
}

# par défaut: ~2 ans. Tu peux monter si Kraken renvoie plus.
LOOKBACK_DAYS_DEFAULT = 900


def _kraken() -> ccxt.Exchange:
    ex = ccxt.kraken({"enableRateLimit": True})
    return ex


def _read_existing_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists() or path.stat().st_size == 0:
        return None
    df = pd.read_csv(path)
    if "timestamp" not in df.columns:
        return None
    df = df.dropna(subset=["timestamp"])
    df["timestamp"] = df["timestamp"].astype(int)
    df = df.sort_values("timestamp").drop_duplicates("timestamp", keep="last")
    return df


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    df = df.sort_values("timestamp").drop_duplicates("timestamp", keep="last")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _fetch_ohlcv_daily(ex: ccxt.Exchange, market: str, since_ms: int) -> pd.DataFrame:
    # Kraken pagine, on boucle
    all_rows = []
    since = since_ms
    while True:
        rows = ex.fetch_ohlcv(market, timeframe=TIMEFRAME, since=since, limit=720)  # 720 = safe
        if not rows:
            break
        all_rows.extend(rows)
        last_ts = rows[-1][0]
        # avancer d'un jour pour éviter boucle infinie
        next_since = last_ts + 24 * 60 * 60 * 1000
        if next_since <= since:
            break
        since = next_since
        # rate-limit gentle
        time.sleep(ex.rateLimit / 1000.0)

        # stop si on a déjà "today" (Kraken renvoie parfois la bougie en cours)
        # on garde tout, le dédoublonnage nettoie.

        if len(rows) < 10:
            # fin probable
            break

    df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = df["timestamp"].astype(int)
    return df


def main() -> None:
    lookback_days = int(os.environ.get("KRAKEN_LOOKBACK_DAYS", LOOKBACK_DAYS_DEFAULT))
    now_ms = int(time.time() * 1000)
    since_ms = now_ms - lookback_days * 24 * 60 * 60 * 1000

    ex = _kraken()
    markets = ex.load_markets()

    ok = 0
    skipped = 0

    for asset, market in SYMBOLS.items():
        if market not in markets:
            print(f"[SKIP] {asset}: market not found on Kraken: {market}")
            skipped += 1
            continue

        out_path = DATA_DIR / f"{asset}.csv"
        existing = _read_existing_csv(out_path)

        # incremental: repartir du dernier timestamp si présent
        since_use = since_ms
        if existing is not None and len(existing) > 0:
            last_ts = int(existing["timestamp"].iloc[-1])
            # reprendre 5 jours avant pour sécurité (révisions/exchange)
            since_use = max(since_ms, last_ts - 5 * 24 * 60 * 60 * 1000)

        df_new = _fetch_ohlcv_daily(ex, market, since_use)

        if df_new.empty:
            print(f"[WARN] {asset}: no data returned for {market}")
            continue

        if existing is None:
            merged = df_new
        else:
            merged = pd.concat([existing, df_new], ignore_index=True)

        _write_csv(out_path, merged)
        print(f"[OK] {asset}: wrote {out_path} rows={len(merged)} (market={market})")
        ok += 1

    print(f"DONE. ok={ok} skipped={skipped} lookback_days={lookback_days}")


if __name__ == "__main__":
    main()
