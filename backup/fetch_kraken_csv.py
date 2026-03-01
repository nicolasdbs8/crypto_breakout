import os
import time
import json
import urllib.parse
import urllib.request
import pandas as pd

KRAKEN_OHLC_URL = "https://api.kraken.com/0/public/OHLC"

# IMPORTANT: paires USD (souvent bien plus anciennes que USDT)
PAIRS = {
    "BTC": "XBTUSD",
    "ETH": "ETHUSD",
    "SOL": "SOLUSD",
    "BNB": "BNBUSD",
    "XRP": "XRPUSD",
    "ADA": "ADAUSD",
    "AVAX": "AVAXUSD",
    "LINK": "LINKUSD",
    "DOT": "DOTUSD",
    "LTC": "LTCUSD",
    "BCH": "BCHUSD",
    "ATOM": "ATOMUSD",
}

REQUIRED_COLS = ["open", "high", "low", "close", "volume"]

def _http_get_json(url: str) -> dict:
    with urllib.request.urlopen(url) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw)

def fetch_ohlc_page(pair: str, interval_minutes: int = 1440, since: int = 0):
    params = {"pair": pair, "interval": interval_minutes, "since": int(since)}
    url = KRAKEN_OHLC_URL + "?" + urllib.parse.urlencode(params)
    payload = _http_get_json(url)

    if payload.get("error"):
        raise RuntimeError(f"Kraken API error for {pair}: {payload['error']}")

    result = payload["result"]
    last = int(result.get("last", 0))
    ohlc_key = [k for k in result.keys() if k != "last"][0]
    rows = result[ohlc_key]
    return rows, last

def rows_to_df(rows) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["time","open","high","low","close","vwap","volume","count"])
    df["date"] = pd.to_datetime(df["time"].astype(int), unit="s", utc=True).dt.tz_convert(None)
    df = df.drop(columns=["time","vwap","count"])
    for c in REQUIRED_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["open","high","low","close"])
    df = df.sort_values("date").drop_duplicates(subset=["date"]).set_index("date")
    return df[REQUIRED_COLS]

def fetch_full_history_forward(pair: str, interval_minutes: int = 1440, sleep_s: float = 1.2, max_pages: int = 400):
    # CRUCIAL: start from 0 to get earliest available bars, then move forward using "last"
    since = 0
    all_parts = []
    last_seen = None

    for _ in range(max_pages):
        rows, last = fetch_ohlc_page(pair, interval_minutes=interval_minutes, since=since)
        if not rows:
            break

        part = rows_to_df(rows)
        all_parts.append(part)

        if last_seen is not None and last <= last_seen:
            break
        last_seen = last
        since = last

        time.sleep(sleep_s)

        # safety: if we’re already at very recent data, stop
        if len(part) < 10:
            break

    if not all_parts:
        return pd.DataFrame(columns=REQUIRED_COLS)

    df = pd.concat(all_parts).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    return df

def main():
    os.makedirs("data", exist_ok=True)

    for asset, pair in PAIRS.items():
        print(f"Fetching FULL {asset} ({pair}) ...")
        try:
            df = fetch_full_history_forward(pair, interval_minutes=1440, sleep_s=1.2, max_pages=400)
        except Exception as e:
            print(f"FAILED {asset} ({pair}): {e}")
            continue

        if df.empty:
            print(f"EMPTY {asset} ({pair})")
            continue

        out_path = os.path.join("data", f"{asset}.csv")
        df.reset_index().rename(columns={"index": "date"}).to_csv(out_path, index=False)
        print(f"Saved: {out_path} rows={len(df)} range={df.index.min().date()}→{df.index.max().date()}")

    print("Done.")

if __name__ == "__main__":
    main()