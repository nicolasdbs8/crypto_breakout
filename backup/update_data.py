import os
import time
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
import pandas as pd

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

# 12 majors en USDT (Binance symbols)
SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
    "BNB": "BNBUSDT",
    "XRP": "XRPUSDT",
    "ADA": "ADAUSDT",
    "AVAX": "AVAXUSDT",
    "LINK": "LINKUSDT",
    "DOT": "DOTUSDT",
    "LTC": "LTCUSDT",
    "BCH": "BCHUSDT",
    "ATOM": "ATOMUSDT",
}

REQUIRED_COLS = ["date", "open", "high", "low", "close", "volume"]


def http_get_json(url: str):
    with urllib.request.urlopen(url) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)


def fetch_klines(symbol: str, interval: str = "1d", start_ms: int | None = None, end_ms: int | None = None, limit: int = 1000):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)

    url = BINANCE_KLINES_URL + "?" + urllib.parse.urlencode(params)
    return http_get_json(url)


def ms_from_date(dt: datetime) -> int:
    # expects dt timezone-aware UTC
    return int(dt.timestamp() * 1000)


def parse_csv_date_to_utc_midnight(d: pd.Timestamp) -> datetime:
    # We store naive timestamps from Binance openTime converted to "naive" (tz removed)
    # We interpret them as UTC times and step from there.
    if isinstance(d, pd.Timestamp):
        # ensure python datetime
        d = d.to_pydatetime()
    # treat as UTC
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    else:
        d = d.astimezone(timezone.utc)
    return d


def klines_to_df(klines) -> pd.DataFrame:
    # Binance kline schema:
    # [
    #  0 openTime, 1 open, 2 high, 3 low, 4 close, 5 volume,
    #  6 closeTime, ...
    # ]
    if not klines:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    rows = []
    for k in klines:
        open_time = int(k[0])
        date = pd.to_datetime(open_time, unit="ms", utc=True).tz_convert(None)
        rows.append({
            "date": date,
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "open_time_ms": open_time,
        })

    df = pd.DataFrame(rows).sort_values("date").drop_duplicates(subset=["date"])
    df = df.set_index("date")
    return df[["open", "high", "low", "close", "volume", "open_time_ms"]]


def fetch_from_start_ms(symbol: str, start_time_ms: int, interval: str = "1d", sleep_s: float = 0.35) -> pd.DataFrame:
    """Paginate forward from start_time_ms using startTime; returns OHLCV indexed by date."""
    all_parts = []
    last_open_time = None
    start_ms = int(start_time_ms)

    while True:
        klines = fetch_klines(symbol, interval=interval, start_ms=start_ms, limit=1000)
        if not klines:
            break

        part = klines_to_df(klines)
        if part.empty:
            break

        all_parts.append(part)

        new_last = int(part["open_time_ms"].iloc[-1])
        if last_open_time is not None and new_last <= last_open_time:
            break
        last_open_time = new_last

        start_ms = new_last + 1
        time.sleep(sleep_s)

        if len(part) < 1000:
            break

    if not all_parts:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = pd.concat(all_parts).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    return df[["open", "high", "low", "close", "volume"]]


def read_existing_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{path} missing cols: {missing}. Found: {list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates(subset=["date"]).set_index("date")
    # keep only required OHLCV
    return df[["open", "high", "low", "close", "volume"]]


def write_csv(path: str, df: pd.DataFrame) -> None:
    out = df.copy()
    out = out.sort_index()
    out.reset_index().to_csv(path, index=False)


def main():
    os.makedirs("data", exist_ok=True)

    summary_rows = []

    for asset, sym in SYMBOLS.items():
        out_path = os.path.join("data", f"{asset}.csv")

        try:
            if os.path.exists(out_path):
                existing = read_existing_csv(out_path)
                last_dt = existing.index.max()
                # start at next day (UTC) after last candle
                last_dt_utc = parse_csv_date_to_utc_midnight(pd.Timestamp(last_dt))
                # refetch last 3 days to refresh partially formed candles + avoid edge cases
                start_dt = last_dt_utc - timedelta(days=3)
                start_ms = ms_from_date(start_dt)
                mode = "INCREMENTAL"
            else:
                existing = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
                # default start for initial build
                start_dt = datetime(2018, 1, 1, tzinfo=timezone.utc)
                start_ms = ms_from_date(start_dt)
                mode = "FULL"

            print(f"Updating {asset} ({sym}) [{mode}] from {start_dt.date()} ...")

            new_df = fetch_from_start_ms(sym, start_ms, interval="1d", sleep_s=0.35)
            if new_df.empty and not existing.empty:
                print(f"  no new rows (last={existing.index.max().date()})")
                summary_rows.append({
                    "asset": asset, "mode": mode,
                    "rows_added": 0,
                    "range_start": str(existing.index.min().date()),
                    "range_end": str(existing.index.max().date()),
                })
                continue

            merged = pd.concat([existing, new_df]).sort_index()
            merged = merged[~merged.index.duplicated(keep="last")]

            write_csv(out_path, merged)

            rows_added = max(0, len(merged) - len(existing))
            print(f"  saved {out_path} rows={len(merged)} added={rows_added} range={merged.index.min().date()}->{merged.index.max().date()}")

            summary_rows.append({
                "asset": asset, "mode": mode,
                "rows_added": rows_added,
                "range_start": str(merged.index.min().date()) if len(merged) else "",
                "range_end": str(merged.index.max().date()) if len(merged) else "",
            })

        except Exception as e:
            print(f"FAILED {asset} ({sym}): {e}")
            summary_rows.append({
                "asset": asset, "mode": "ERROR",
                "rows_added": 0,
                "range_start": "",
                "range_end": "",
                "error": str(e),
            })

    print("\n=== DATA UPDATE SUMMARY ===")
    df_sum = pd.DataFrame(summary_rows)
    if not df_sum.empty:
        print(df_sum.to_string(index=False))

    print("\nDone.")


if __name__ == "__main__":
    main()
