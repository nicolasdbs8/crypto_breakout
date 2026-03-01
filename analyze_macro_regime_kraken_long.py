import pandas as pd
from pathlib import Path
from strategy import prepare_indicators, macro_filter

# <<< SOURCE RESEARCH LONG TERME >>>
BTC_PATH = "data_research/BTC_USD_FULL.csv"

REQUIRED = ["open", "high", "low", "close", "volume"]


def _ensure_date_column(df: pd.DataFrame) -> pd.DataFrame:
    # Accept 'date' or 'Date' or timestamp variants
    cols = list(df.columns)
    if "date" in cols:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df
    if "Date" in cols:
        df["date"] = pd.to_datetime(df["Date"], errors="coerce")
        return df

    # timestamp (ms or s)
    for c in ["timestamp", "Timestamp", "time", "Time"]:
        if c in cols:
            ts = pd.to_numeric(df[c], errors="coerce")
            ts_max = ts.max()
            unit = "ms" if ts_max and ts_max > 10_000_000_000 else "s"
            df["date"] = pd.to_datetime(ts, unit=unit, utc=True).dt.tz_convert(None)
            return df

    raise ValueError(f"No date column found. Columns={cols}")


def _normalize_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Map possible column names to required
    mapping = {}
    lower = {c.lower(): c for c in df.columns}

    for k in ["open", "high", "low", "close", "volume"]:
        if k in df.columns:
            mapping[k] = k
        elif k in lower:
            mapping[lower[k]] = k

    df = df.rename(columns=mapping)

    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns {missing}. Columns={list(df.columns)}")

    for c in REQUIRED:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def main():
    p = Path(BTC_PATH)
    if not p.exists():
        raise FileNotFoundError(f"Missing research BTC file: {BTC_PATH}")

    df = pd.read_csv(p)
    df = _ensure_date_column(df)
    df = _normalize_ohlcv_columns(df)

    df = df.dropna(subset=["date"] + REQUIRED)
    df = df.sort_values("date").drop_duplicates(subset=["date"]).set_index("date")
    df = df[REQUIRED]

    if len(df) < 300:
        raise ValueError(f"BTC dataset too short: {len(df)} rows. Did you upload the full daily history?")

    data = {"BTC": df}
    data = prepare_indicators(data)
    btc = data["BTC"]

    macro = macro_filter(btc).astype(bool)
    macro = macro.dropna()

    total_days = len(macro)
    on_days = int(macro.sum())
    off_days = total_days - on_days

    print("----- BTC/USD MACRO SUMMARY (RESEARCH) -----")
    print(f"Date range: {macro.index.min().date()} → {macro.index.max().date()}")
    print(f"Total days: {total_days}")
    print(f"Macro ON days: {on_days} ({on_days/total_days:.2%})")
    print(f"Macro OFF days: {off_days} ({off_days/total_days:.2%})")

    # durations
    durations = []
    cur = bool(macro.iloc[0])
    length = 1
    for v in macro.iloc[1:]:
        v = bool(v)
        if v == cur:
            length += 1
        else:
            durations.append((cur, length))
            cur = v
            length = 1
    durations.append((cur, length))

    on_d = [d for s, d in durations if s]
    off_d = [d for s, d in durations if not s]

    print("\n----- REGIME DURATIONS -----")
    if on_d:
        print(f"ON avg duration: {sum(on_d)/len(on_d):.1f} days")
        print(f"ON max duration: {max(on_d)} days")
    if off_d:
        print(f"OFF avg duration: {sum(off_d)/len(off_d):.1f} days")
        print(f"OFF max duration: {max(off_d)} days")
    print("----------------------------")


if __name__ == "__main__":
    main()
