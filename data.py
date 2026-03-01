from pathlib import Path
import pandas as pd


def _ensure_date_column(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    Accept either:
      - 'date' column (string/datetime)
      - 'timestamp' column (ms or seconds)
    Produces a naive (timezone-less) datetime in df['date'].
    """
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_localize(None)
        return df

    if "timestamp" in df.columns:
        ts = pd.to_numeric(df["timestamp"], errors="coerce")
        ts_max = ts.max()
        if pd.isna(ts_max):
            raise ValueError(f"{filename}: 'timestamp' exists but cannot be parsed as numeric.")
        unit = "ms" if ts_max > 10_000_000_000 else "s"
        df["date"] = pd.to_datetime(ts, unit=unit, utc=True).dt.tz_localize(None)
        return df

    raise ValueError(f"{filename}: CSV must contain either 'date' or 'timestamp' column.")


def load_ohlcv_folder(folder: str) -> dict[str, pd.DataFrame]:
    folder_path = Path(folder)
    if not folder_path.exists():
        raise FileNotFoundError(f"Data folder not found: {folder}")

    data: dict[str, pd.DataFrame] = {}

    files = sorted(folder_path.glob("*.csv"))
    if not files:
        raise ValueError(f"No CSV files found in {folder}")

    for file in files:
        # Skip empty files (0 bytes) to avoid crashes
        if file.stat().st_size == 0:
            continue

        sym = file.stem.upper()

        df = pd.read_csv(file)
        df = _ensure_date_column(df, file.name)

        required = ["open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"{file.name}: missing columns {missing}. Got: {list(df.columns)}")

        # Normalize + clean
        df = df[["date"] + required].copy()
        df = df.dropna(subset=["date"]).sort_values("date")
        df = df.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

        data[sym] = df

    if not data:
        raise ValueError(f"All CSV files in {folder} were empty (0 bytes).")

    return data
