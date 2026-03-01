import os
import time
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
import pandas as pd

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

OUT_DIR = os.environ.get("BINANCE_OUT_DIR", "data_binance")
SYMBOL = "BTCUSDT"         # Binance spot
INTERVAL = "1d"
START_DT = "2013-01-01"    # BTC long history
SLEEP_S = 0.35

REQUIRED_COLS = ["date", "open", "high", "low", "close", "volume"]


def http_get_json(url: str):
    with urllib.request.urlopen(url) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)


def fetch_klines(symbol: str, interval: str, start_ms: int, limit: int = 1000):
    params = {"symbol": symbol, "interval": interval, "limit": limit, "startTime": int(start_ms)}
    url = BINANCE_KLINES_URL + "?" + urllib.parse.urlencode(params)
    return http_get_json(url)


def ms_from_dt(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def klines_to_df(klines) -> pd.DataFrame:
    if not klines:
        return pd.DataFrame(columns=REQUIRED_COLS)

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


def write_csv(path: str, df: pd.DataFrame) -> None:
    out = df.copy().sort_index()
    out.reset_index().to_csv(path, index=False)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "BTC.csv")

    start_dt = datetime.fromisoformat(START_DT).replace(tzinfo=timezone.utc)
    start_ms = ms_from_dt(start_dt)

    parts = []
    last_open_time = None

    print(f"Fetching {SYMBOL} from {START_DT} ...")

    while True:
        klines = fetch_klines(SYMBOL, INTERVAL, start_ms, limit=1000)
        if not klines:
            break

        part = klines_to_df(klines)
        if part.empty:
            break

        parts.append(part)

        new_last = int(part["open_time_ms"].iloc[-1])
        if last_open_time is not None and new_last <= last_open_time:
            break
        last_open_time = new_last

        start_ms = new_last + 1
        time.sleep(SLEEP_S)

        if len(part) < 1000:
            break

    if not parts:
        raise RuntimeError("No data returned from Binance.")

    df = pd.concat(parts).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    df = df[["open", "high", "low", "close", "volume"]]

    write_csv(out_path, df)
    print(f"Saved {out_path} rows={len(df)} range={df.index.min().date()}→{df.index.max().date()}")


if __name__ == "__main__":
    main()
