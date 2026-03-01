import argparse
from dataclasses import dataclass
from pathlib import Path
import pandas as pd


@dataclass
class WindowResult:
    start: str
    end: str
    cagr: float
    maxdd: float
    trades: int


def compute_cagr(equity: pd.Series) -> float:
    if len(equity) < 2:
        return float("nan")
    start = float(equity.iloc[0])
    end = float(equity.iloc[-1])
    if start <= 0 or end <= 0:
        return float("nan")
    days = (equity.index[-1] - equity.index[0]).days
    years = days / 365.25 if days > 0 else float("nan")
    if not years or years <= 0:
        return float("nan")
    return (end / start) ** (1 / years) - 1


def compute_maxdd(equity: pd.Series) -> float:
    if len(equity) == 0:
        return float("nan")
    peak = equity.cummax()
    dd = equity / peak - 1.0
    return float(dd.min())


def count_trades_in_window(trades_df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp) -> int:
    if trades_df is None or trades_df.empty:
        return 0
    if "entry_date" not in trades_df.columns:
        return 0
    td = trades_df.copy()
    td["entry_date"] = pd.to_datetime(td["entry_date"])
    m = (td["entry_date"] >= start) & (td["entry_date"] <= end)
    return int(m.sum())


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--equity", default="equity_curve.csv", help="Path to equity_curve*.csv")
    p.add_argument("--trades", default="trade_log.csv", help="Path to trade_log*.csv (for counts)")
    p.add_argument("--out_prefix", default="", help="Prefix for output files (e.g., s2_ma_trend)")
    p.add_argument("--window_years", type=int, default=3, help="Rolling window length in years (default 3)")
    return p.parse_args()


def main():
    args = parse_args()
    equity_path = Path(args.equity)
    trades_path = Path(args.trades)

    eq = pd.read_csv(equity_path, index_col=0, parse_dates=True)
    if "equity" not in eq.columns:
        raise SystemExit(f"{equity_path} must contain column 'equity'. Columns: {list(eq.columns)}")
    eq_series = eq["equity"].dropna().copy()
    eq_series.index = pd.to_datetime(eq_series.index)

    trades_df = None
    if trades_path.exists():
        try:
            trades_df = pd.read_csv(trades_path)
        except Exception:
            trades_df = None

    # Build 6 windows like you had (calendar years stepping by 1 year),
    # but data-driven: start at first year boundary if possible.
    idx = eq_series.index
    start_year = int(idx.min().year)
    end_year = int(idx.max().year)

    win = args.window_years
    results = []

    for y in range(start_year, end_year - win + 2):
        start = pd.Timestamp(f"{y}-01-01")
        end = pd.Timestamp(f"{y+win-1}-12-31")

        sub = eq_series[(eq_series.index >= start) & (eq_series.index <= end)]
        if len(sub) < 50:
            continue

        cagr = compute_cagr(sub)
        maxdd = compute_maxdd(sub)
        ntr = count_trades_in_window(trades_df, start, end)

        results.append(WindowResult(str(start.date()), str(end.date()), float(cagr), float(maxdd), ntr))

        print(f"[{start.date()} -> {end.date()}] CAGR={cagr:.3f} MaxDD={maxdd:.3f} Trades={ntr}")

    if not results:
        raise SystemExit("No rolling windows produced (not enough data?)")

    out_prefix = args.out_prefix.strip()
    suffix = f"_{out_prefix}" if out_prefix else ""
    out_csv = Path(f"rolling_results{suffix}.csv")

    df_out = pd.DataFrame([r.__dict__ for r in results])
    df_out.to_csv(out_csv, index=False)

    # Summary
    cagrs = df_out["cagr"]
    dds = df_out["maxdd"]
    trs = df_out["trades"]

    print("\n=== SUMMARY (rolling) ===")
    print("Windows:", len(df_out))
    print("CAGR  min/median/max:", float(cagrs.min()), float(cagrs.median()), float(cagrs.max()))
    print("MaxDD min/median/max:", float(dds.min()), float(dds.median()), float(dds.max()))
    print("Trades min/median/max:", int(trs.min()), int(trs.median()), int(trs.max()))
    print(f"Saved: {out_csv}")


if __name__ == "__main__":
    main()