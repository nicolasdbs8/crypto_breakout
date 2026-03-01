import argparse
from pathlib import Path
import pandas as pd

from data import load_ohlcv_folder
from strategy import prepare_indicators, macro_filter
from portfolio import Portfolio
from backtest import run_backtest
from report import performance_metrics
from paths import output_path_str
from indicators import slope_simple

# Baseline config
INITIAL_CAPITAL = 3500.0
RISK_PER_TRADE  = 0.0125
FEE_TAKER       = 0.0026
SLIPPAGE        = 0.0020


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", default="s1_breakout")
    p.add_argument("--risk", type=float, default=None)
    p.add_argument("--fee_entry", type=float, default=None)
    p.add_argument("--fee_exit", type=float, default=None)
    p.add_argument("--slippage", type=float, default=None)
    p.add_argument("--out_dir", type=str, default=None)
    return p.parse_args()


def main():
    args = parse_args()

    data = load_ohlcv_folder("data/")
    data = prepare_indicators(data)

    # --- MACRO DEBUG ---
    btc = data.get("BTC")
    if btc is None:
        raise KeyError("BTC data missing.")

    macro_series = macro_filter(btc)
    macro_state = bool(macro_series.iloc[-1])

    btc_close = btc["close"].iloc[-1]
    btc_sma200 = btc["SMA200"].iloc[-1]
    slope_val = slope_simple(btc["SMA200"], 20).iloc[-1]

    print("----- MACRO DEBUG -----")
    print("BTC close:", btc_close)
    print("BTC SMA200:", btc_sma200)
    print("Slope SMA200(20):", slope_val)
    print("Macro ON:", macro_state)
    print("-----------------------")

    # --- Backtest config ---
    risk = RISK_PER_TRADE if args.risk is None else float(args.risk)
    fee_entry = FEE_TAKER if args.fee_entry is None else float(args.fee_entry)
    fee_exit = FEE_TAKER if args.fee_exit is None else float(args.fee_exit)
    slippage = SLIPPAGE if args.slippage is None else float(args.slippage)

    portfolio = Portfolio(
        capital=INITIAL_CAPITAL,
        risk_per_trade=risk,
        fee_rate_entry=fee_entry,
        fee_rate_exit=fee_exit,
        slippage_rate=slippage,
        max_positions=3
    )

    trades, equity_df = run_backtest(
        data,
        portfolio,
        btc_symbol="BTC",
        strategy_name=args.strategy,
    )

    # --- Output ---
    def out_path(name: str) -> str:
        if args.out_dir:
            d = Path(args.out_dir)
            d.mkdir(parents=True, exist_ok=True)
            return str(d / name)
        return output_path_str(name)

    trade_name = f"trade_log_{args.strategy}.csv"
    eq_name = f"equity_curve_{args.strategy}.csv"

    pd.DataFrame(trades).to_csv(out_path(trade_name), index=False)
    equity_df.to_csv(out_path(eq_name))

    metrics = performance_metrics(trades, equity_df, initial_capital=float(INITIAL_CAPITAL))
    print(metrics)


if __name__ == "__main__":
    main()
