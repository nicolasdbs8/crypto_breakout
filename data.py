from pathlib import Path
import pandas as pd


def _ensure_date_column(df: pd.DataFrame) -> pd.DataFrame:
    # Accept either:
    # - date (string / datetime)
    # - timestamp (ms or seconds)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_localize(None)
        return df

    if "timestamp" in df.columns:
        ts = df["timestamp"]
        # detect ms vs seconds
        # if values look like 13 digits -> ms
        ts_max = pd.to_numeric(ts, errors="coerce").max()
        if pd.isna(ts_max):
            raise ValueError("timestamp column exists but cannot be parsed to numeric.")
        unit = "ms" if ts_max > 10_000_000_000 else "s"
        df["date"] = pd.to_datetime(pd.to_numeric(ts, errors="coerce"), unit=unit, utc=True).dt.tz_localize(None)
        return df

    raise ValueError("CSV must contain either 'date' or 'timestamp' column.")


def load_ohlcv_folder(folder: str) -> dict[str, pd.DataFrame]:
    folder_path = Path(folder)
    if not folder_path.exists():
        raise FileNotFoundError(f"Data folder not found: {folder}")

    data: dict[str, pd.DataFrame] = {}

    for file in sorted(folder_path.glob("*.csv")):
        if file.stat().st_size == 0:
        # skip empty csv files (can happen after failed fetch / partial run)
        continue
        sym = file.stem.upper()
        df = pd.read_csv(file)
        df = _ensure_date_column(df)

        # Normalize required columns
        required = ["open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"{file.name} missing columns: {missing} (got {list(df.columns)})")

        df = df[["date"] + required].copy()
        df = df.dropna(subset=["date"]).sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)

        data[sym] = df

    if not data:
        raise ValueError(f"No CSV files found in {folder}")

    return data
