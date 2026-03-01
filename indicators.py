import pandas as pd
import numpy as np

def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window).mean()

def highest(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window).max()

def lowest(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window).min()

def atr(df: pd.DataFrame, window: int = 20) -> pd.Series:
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(window).mean()

def momentum(series: pd.Series, window: int) -> pd.Series:
    return series / series.shift(window) - 1.0

def realized_vol(series: pd.Series, window: int) -> pd.Series:
    return series.pct_change().rolling(window).std()

def rolling_quantile(series: pd.Series, window: int, q: float) -> pd.Series:
    return series.rolling(window).quantile(q)

def slope_simple(series: pd.Series, window: int) -> pd.Series:
    # Simple slope proxy (difference / window)
    return series.diff(window) / float(window)

def safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    b2 = b.replace(0, np.nan)
    return a / b2