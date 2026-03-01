import json
import pandas as pd
from pathlib import Path

from data import load_ohlcv_folder
from strategy import prepare_indicators, macro_filter
from portfolio import Portfolio
from backtest import run_backtest
from report import performance_metrics
from paths import output_path_str

BTC_RESEARCH_PATH = "data_research/BTC_USD_FULL.csv"


def load_btc_research_macro(target_index: pd.DatetimeIndex) -> pd.Series:
    p = Path(BTC_RESEARCH_PATH)
    if not p.exists():
        raise FileNotFoundError(f"Missing {BTC_RESEARCH_PATH} (commit it to the repo).")

    df = pd.read_csv(p)
    # accept date/Date, but your file is lowercase
    if "date" not in df.columns and "Date" in df.columns:
        df["date"] = df["Date"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"]).set_index("date")

    # volume optional -> create
    if "volume" not in df.columns:
        df["volume"] = 0.0

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df[["open", "high", "low", "close", "volume"]].dropna()

    data = {"BTC": df}
    data = prepare_indicators(data)
    btc = data["BTC"]
    m = macro_filter(btc).astype(bool)

    # align to Kraken BTC index used by backtest dates
    m = m.reindex(target_index, method="ffill").fillna(False)
    return m


def run_one(label: str, data_dict: dict, macro_enabled: bool, macro_override: pd.Series | None):
    port = Portfolio()
    trades, equity = run_backtest(
        data_dict,
        port,
        btc_symbol="BTC",
        strategy_name="s2_ma_trend",
        risk_out_name=f"risk_frac_{label}.csv",
        macro_enabled=macro_enabled,
        macro_override=macro_override,
    )

    metrics = performance_metrics(trades, equity, initial_capital=3500.0)

    # Save artifacts
    equity.to_csv(output_path_str(f"equity_{label}.csv"))
    pd.DataFrame(trades).to_csv(output_path_str(f"trades_{label}.csv"), index=False)

    return metrics


def main():
    data_dict = load_ohlcv_folder("data/")
    data_dict = prepare_indicators(data_dict)

    btc_idx = data_dict["BTC"].index

    macro_kraken = macro_filter(data_dict["BTC"]).astype(bool)
    macro_research = load_btc_research_macro(btc_idx)

    results = {}

    # 1) No macro
    results["s2_no_macro"] = run_one("s2_no_macro", data_dict, macro_enabled=False, macro_override=None)

    # 2) Macro Kraken
    results["s2_macro_kraken"] = run_one("s2_macro_kraken", data_dict, macro_enabled=True, macro_override=macro_kraken)

    # 3) Macro Research
    results["s2_macro_research"] = run_one("s2_macro_research", data_dict, macro_enabled=True, macro_override=macro_research)

    out = output_path_str("macro_compare_s2.json")
    Path(out).write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
