import pandas as pd
import numpy as np

def performance_metrics(trades, equity_df: pd.DataFrame, initial_capital: float = 3500.0):
    tdf = pd.DataFrame(trades) if trades else pd.DataFrame(columns=["pnl", "R_multiple"])
    e = equity_df["equity"].dropna()
    if len(e) < 2:
        return {"error": "Not enough equity points to compute metrics."}

    # Use explicit initial capital for clean walk-forward comparability
    start_equity = float(initial_capital)
    end_equity = float(e.iloc[-1])

    days = (e.index[-1] - e.index[0]).days
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