import argparse
from pathlib import Path
import pandas as pd

from data import load_ohlcv_folder
from strategy import prepare_indicators
from portfolio import Portfolio
from backtest import run_backtest
from report import performance_metrics
from paths import output_path_str

# Baseline config (keep it explicit + conservative)
INITIAL_CAPITAL = 3500.0
RISK_PER_TRADE  = 0.0150
FEE_TAKER       = 0.0026   # 0.26%
SLIPPAGE        = 0.0020   # 0.20% per side


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--strategy",
        default="s1_breakout",
        help="s1_breakout (default), s2_ma_trend, s3_tsmom",
    )
    # Overrides (optional)
    p.add_argument("--risk", type=float, default=None, help="Override risk_per_trade (e.g. 0.01)")
    p.add_argument("--fee_entry", type=float, default=None, help="Override entry fee rate (e.g. 0.0030)")
    p.add_argument("--fee_exit", type=float, default=None, help="Override exit fee rate (e.g. 0.0030)")
    p.add_argument("--slippage", type=float, default=None, help="Override slippage rate per side (e.g. 0.0025)")
    p.add_argument(
        "--out_dir",
        type=str,
        default=None,
        help="Optional output directory (e.g. data/outputs/analysis). If omitted uses paths.output_path_str().",
    )
    return p.parse_args()


def main():
    args = parse_args()

    data = load_ohlcv_folder("data/")
    data = prepare_indicators(data)

    risk = RISK_PER_TRADE if args.risk is None else float(args.risk)
    fee_entry = FEE_TAKER if args.fee_entry is None else float(args.fee_entry)
    fee_exit = FEE_TAKER if args.fee_exit is None else float(args.fee_exit)
    slippage = SLIPPAGE if args.slippage is None else float(args.slippage)

    portfolio = Portfolio(
        capital=INITIAL_CAPITAL,
        risk_per_trade=risk,
        fee_rate_entry=fee_entry,   # conservative by default: treat entries as taker too
        fee_rate_exit=fee_exit,
        slippage_rate=slippage,
        max_positions=3
    )

    # Backward-compatible filenames for baseline
    is_baseline = (args.strategy or "").lower() in ("s1_breakout", "s1", "breakout", "")

    trade_name = "trade_log.csv" if is_baseline else f"trade_log_{args.strategy}.csv"
    eq_name    = "equity_curve.csv" if is_baseline else f"equity_curve_{args.strategy}.csv"
    risk_name  = "risk_frac_daily.csv" if is_baseline else f"risk_frac_daily_{args.strategy}.csv"

    trades, equity_df = run_backtest(
        data,
        portfolio,
        btc_symbol="BTC",
        strategy_name=args.strategy,
        risk_out_name=risk_name,
    )

    # Output routing
    def out_path(name: str) -> str:
        if args.out_dir:
            d = Path(args.out_dir)
            d.mkdir(parents=True, exist_ok=True)
            return str(d / name)
        return output_path_str(name)

    pd.DataFrame(trades).to_csv(out_path(trade_name), index=False)
    equity_df.to_csv(out_path(eq_name))

    metrics = performance_metrics(trades, equity_df, initial_capital=float(INITIAL_CAPITAL))
    print(metrics)


if __name__ == "__main__":
    main()
