import pandas as pd
from pathlib import Path

REQUIRED_COLS = ["open", "high", "low", "close", "volume"]

def load_ohlcv_folder(folder_path: str):
    """
    Expect one CSV per symbol:
    columns: date, open, high, low, close, volume
    date must be parseable, sorted ascending.
    """
    data = {}
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Data folder not found: {folder_path}")

    files = list(folder.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in: {folder_path}")

    for file in files:
        df = pd.read_csv(file, parse_dates=["date"])
        if "date" not in df.columns:
            raise ValueError(f"{file.name}: missing 'date' column")
        df = df.sort_values("date").set_index("date")

        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"{file.name}: missing columns {missing}")

        df = df[REQUIRED_COLS].copy()
        # Force numeric
        for c in REQUIRED_COLS:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=["open", "high", "low", "close"])
        data[file.stem.upper()] = df

    return data