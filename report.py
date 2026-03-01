import pandas as pd
import numpy as np


def _equity_series_with_datetime_index(equity_df: pd.DataFrame) -> pd.Series:
    """
    Returns equity series with a DateTimeIndex if possible.
    Accepts equity_df with:
      - equity column
      - optional date column (preferred)
      - optional timestamp column (ms or s)
    If no date/timestamp is available, returns equity with original index.
    """
    if "equity" not in equity_df.columns:
        raise KeyError("equity_df missing required column: 'equity'")

    df = equity_df.copy()

    # Prefer explicit date column
    if "date" in df.columns:
        dt = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.tz_localize(None)
        df = df.assign(_dt=dt).dropna(subset=["_dt"]).sort_values("_dt")
        e = df.set_index("_dt")["equity"].astype(float)
        return e

    # Or timestamp column (ms or s)
    if "timestamp" in df.columns:
        ts = pd.to_numeric(df["timestamp"], errors="coerce")
        ts_max = ts.max()
        if not pd.isna(ts_max):
            unit = "ms" if ts_max > 10_000_000_000 else "s"
            dt = pd.to_datetime(ts, unit=unit, utc=True, errors="coerce").dt.tz_localize(None)
            df = df.assign(_dt=dt).dropna(subset=["_dt"]).sort_values("_dt")
            e = df.set_index("_dt")["equity"].astype(float)
            return e

    # Fallback: no datetime available
    return df["equity"].astype(float)


def performance_metrics(trades, equity_df: pd.DataFrame, initial_capital: float = 3500.0):
    tdf = pd.DataFrame(trades) if trades else pd.DataFrame(columns=["pnl", "R_multiple"])
    e = _equity_series_with_datetime_index(equity_df).dropna()

    if len(e) < 2:
        return {"error": "Not enough equity points to compute metrics."}

    # Use explicit initial capital for comparability
    start_equity = float(initial_capital)
    end_equity = float(e.iloc[-1])

    # Compute years
    if isinstance(e.index, pd.DatetimeIndex):
        days = (e.index[-1] - e.index[0]).days
    else:
        # assume daily bars if no date index
        days = int(len(e) - 1)

    years = days / 365.0 if days > 0 else 0.0

    total_return = float(end_equity / start_equity - 1.0)
    cagr = float((end_equity / start_equity) ** (1.0 / years) - 1.0) if years > 0 else np.nan

    dd = (e / e.cummax() - 1.0)
    maxdd = float(dd.min())

    if len(tdf) > 0 and "pnl" in tdf.columns:
        gross_profit = float(tdf.loc[tdf["pnl"] > 0, "pnl"].sum())
        gross_loss = float(-tdf.loc[tdf["pnl"] < 0, "pnl"].sum())
        profit_factor = float(gross_profit / gross_loss) if gross_loss > 0 else np.inf

        expectancy_R = float(tdf["R_multiple"].mean()) if "R_multiple" in tdf.columns and len(tdf) > 0 else np.nan

        top5 = float(tdf.sort_values("pnl", ascending=False).head(5)["pnl"].sum()) if len(tdf) >= 1 else 0.0
        pct_top5 = float(top5 / gross_profit) if gross_profit > 0 else np.nan
    else:
        profit_factor = np.nan
        expectancy_R = np.nan
        pct_top5 = np.nan

    trades_per_year = float(len(tdf) / years) if years > 0 else np.nan

    return {
        "StartEquity": start_equity,
        "EndEquity": end_equity,
        "TotalReturn": total_return,
        "CAGR": cagr,
        "MaxDD": maxdd,
        "ProfitFactor": profit_factor,
        "Expectancy_R": expectancy_R,
        "TradesPerYear": trades_per_year,
        "PctPnL_Top5": pct_top5,
        "NumTrades": int(len(tdf)),
    }
