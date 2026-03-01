import pandas as pd
from data import load_ohlcv_folder
from strategy import prepare_indicators
from portfolio import Portfolio
from backtest import run_backtest
from report import performance_metrics
from paths import output_path_str

# Baseline config (keep it explicit + conservative)
INITIAL_CAPITAL = 3500.0
RISK_PER_TRADE  = 0.0125
FEE_TAKER       = 0.0026   # 0.26%
SLIPPAGE        = 0.0020   # 0.20% per side

def main():
    data = load_ohlcv_folder("data/")
    data = prepare_indicators(data)

    portfolio = Portfolio(
        capital=INITIAL_CAPITAL,
        risk_per_trade=RISK_PER_TRADE,
        fee_rate_entry=FEE_TAKER,   # conservative: treat entries as taker too
        fee_rate_exit=FEE_TAKER,
        slippage_rate=SLIPPAGE,
        max_positions=3
    )

    trades, equity_df = run_backtest(data, portfolio, btc_symbol="BTC")

    pd.DataFrame(trades).to_csv(output_path_str("trade_log.csv"), index=False)
    equity_df.to_csv(output_path_str("equity_curve.csv"))

    metrics = performance_metrics(trades, equity_df, initial_capital=float(INITIAL_CAPITAL))
    print(metrics)

if __name__ == "__main__":
    main()
