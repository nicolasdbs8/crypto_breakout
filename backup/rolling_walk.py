import pandas as pd

from data import load_ohlcv_folder
from strategy import prepare_indicators
from portfolio import Portfolio
import backtest as bt
from backtest import run_backtest
from report import performance_metrics
from paths import output_path_str

# Import config from main.py to guarantee consistency
from main import INITIAL_CAPITAL, RISK_PER_TRADE, FEE_TAKER, SLIPPAGE


def run_window(data, start, end):
    # backtest.py exposes WF_START / WF_END for windowing
    bt.WF_START = pd.Timestamp(start)
    bt.WF_END   = pd.Timestamp(end)

    portfolio = Portfolio(
        capital=float(INITIAL_CAPITAL),
        risk_per_trade=float(RISK_PER_TRADE),
        fee_rate_entry=float(FEE_TAKER),
        fee_rate_exit=float(FEE_TAKER),
        slippage_rate=float(SLIPPAGE),
        max_positions=3
    )

    trades, equity_df = run_backtest(data, portfolio, btc_symbol="BTC")
    metrics = performance_metrics(trades, equity_df, initial_capital=float(INITIAL_CAPITAL))

    # reset globals to avoid surprises if user runs main afterwards
    bt.WF_START = None
    bt.WF_END = None

    return metrics


def main():
    data = load_ohlcv_folder("data/")
    data = prepare_indicators(data)

    windows = [
        ("2018-01-01", "2020-12-31"),
        ("2019-01-01", "2021-12-31"),
        ("2020-01-01", "2022-12-31"),
        ("2021-01-01", "2023-12-31"),
        ("2022-01-01", "2024-12-31"),
        ("2023-01-01", "2025-12-31"),
    ]

    rows = []
    for start, end in windows:
        m = run_window(data, start, end)
        print(f"[{start} -> {end}] CAGR={m['CAGR']:.3f} MaxDD={m['MaxDD']:.3f} Trades={m['NumTrades']}")
        rows.append({
            "start": start,
            "end": end,
            **m
        })

    out = pd.DataFrame(rows)
    out.to_csv(output_path_str("rolling_results.csv"), index=False)

    print("\n=== SUMMARY (rolling 3y) ===")
    print("Windows:", len(out))
    print("CAGR  min/median/max:", float(out["CAGR"].min()), float(out["CAGR"].median()), float(out["CAGR"].max()))
    print("MaxDD min/median/max:", float(out["MaxDD"].min()), float(out["MaxDD"].median()), float(out["MaxDD"].max()))
    print("NumTrades min/median/max:", int(out["NumTrades"].min()), int(out["NumTrades"].median()), int(out["NumTrades"].max()))


if __name__ == "__main__":
    main()
