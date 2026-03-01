import json
from pathlib import Path

from data import load_ohlcv_folder
from strategy import prepare_indicators
from portfolio import Portfolio
from backtest import run_backtest
from report import performance_metrics
from paths import output_path_str


INITIAL_CAPITAL = 3500.0
RISK_PER_TRADE = 0.0125
MAX_POSITIONS = 3
MAX_TOTAL_RISK_FRAC = 0.05


def run_variant(label, fee_entry, fee_exit, slippage):
    data_dict = load_ohlcv_folder("data/")
    data_dict = prepare_indicators(data_dict)

    port = Portfolio(
        capital=INITIAL_CAPITAL,
        risk_per_trade=RISK_PER_TRADE,
        fee_rate_entry=fee_entry,
        fee_rate_exit=fee_exit,
        slippage_rate=slippage,
        max_positions=MAX_POSITIONS,
        max_total_risk_frac=MAX_TOTAL_RISK_FRAC,
    )

    trades, equity = run_backtest(
        data_dict,
        port,
        btc_symbol="BTC",
        strategy_name="s2_ma_trend",
        macro_enabled=True,  # garder macro pour cohérence
        macro_override=None,
    )

    metrics = performance_metrics(trades, equity, initial_capital=INITIAL_CAPITAL)

    equity.to_csv(output_path_str(f"equity_{label}.csv"))
    return metrics


def main():
    results = {}

    # 1️⃣ Edge brut
    results["no_costs"] = run_variant(
        "no_costs",
        fee_entry=0.0,
        fee_exit=0.0,
        slippage=0.0,
    )

    # 2️⃣ Fees only
    results["fees_only"] = run_variant(
        "fees_only",
        fee_entry=0.0026,
        fee_exit=0.0026,
        slippage=0.0,
    )

    # 3️⃣ Fees + slippage (réel)
    results["fees_and_slippage"] = run_variant(
        "fees_and_slippage",
        fee_entry=0.0026,
        fee_exit=0.0026,
        slippage=0.002,
    )

    out = output_path_str("cost_compare_s2.json")
    Path(out).write_text(json.dumps(results, indent=2))

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
