import os
import time
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
import pandas as pd

KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"

OUT_DIR = os.environ.get("KRAKEN_OUT_DIR", "data_kraken_long")
PAIR = "XBTUSD"       # Kraken uses XBT
INTERVAL = 1440       # daily
SLEEP_S = 0.35        # rate limit friendly


def http_get_json(url: str):
    with urllib.request.urlopen(url) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)


def fetch_ohlc(pair: str, interval: int, since: int | None):
    params = {"pair": pair, "interval": interval}
    if since is not None:
        params["since"] = int(since)
    url = KRAKEN_OHLC_URL + "?" + urllib.parse.urlencode(params)
    return http_get_json(url)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "BTC.csv")

    # Start from epoch (Kraken will return earliest available and paginate via "last")
    since = 0
    parts = []
    last_seen = None

    print(f"Fetching Kraken OHLC {PAIR} interval={INTERVAL} ...")

    while True:
        payload = fetch_ohlc(PAIR, INTERVAL, since)
        if payload.get("error"):
            raise RuntimeError(f"Kraken error: {payload['error']}")

        result = payload.get("result", {})
        last = int(result.get("last"))
        # Result key is not necessarily exactly PAIR; get the first non-"last" key
        series_key = next(k for k in result.keys() if k != "last")
        rows = result.get(series_key, [])

        if not rows:
            break

        df = pd.DataFrame(rows, columns=[
            "timestamp", "open", "high", "low", "close", "vwap", "volume", "count"
        ])
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype("int64")
        df["date"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.tz_convert(None)

        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df[["date", "open", "high", "low", "close", "volume"]].dropna()
        parts.append(df)

        if last_seen is not None and last <= last_seen:
            break
        last_seen = last
        since = last  # paginate

        # stop if Kraken returns same last repeatedly or tiny increments
        time.sleep(SLEEP_S)

        # safety: if last is "now-ish", next call often returns empty; loop will break
        if len(df) < 10:
            break

        # log progress occasionally
        if len(parts) % 10 == 0:
            print("... last date:", df["date"].iloc[-1].date(), "last cursor:", last)

    if not parts:
        raise RuntimeError("No data returned from Kraken.")

    all_df = pd.concat(parts, ignore_index=True)
    all_df = all_df.sort_values("date").drop_duplicates(subset=["date"])
    all_df.to_csv(out_path, index=False)

    print(f"Saved {out_path} rows={len(all_df)} range={all_df['date'].min().date()}→{all_df['date'].max().date()}")


if __name__ == "__main__":
    main()
