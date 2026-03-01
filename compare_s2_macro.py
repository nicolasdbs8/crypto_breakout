import json
from pathlib import Path

import pandas as pd

from data import load_ohlcv_folder
from strategy import prepare_indicators, macro_filter
from portfolio import Portfolio
from backtest import run_backtest
from report import performance_metrics
from paths import output_path_str

BTC_RESEARCH_PATH = "data_research/BTC_USD_FULL.csv"

# === Paramètres moteur (à garder identiques à ton daily) ===
INITIAL_CAPITAL = 3500.0
RISK_PER_TRADE = 0.0125         # 1.25%
FEE_RATE_ENTRY = 0.0026         # exemple: 0.26%
FEE_RATE_EXIT = 0.0026          # idem si tu utilises le même
SLIPPAGE_RATE = 0.0020          # exemple: 0.20%
MAX_POSITIONS = 3
MAX_TOTAL_RISK_FRAC = 0.05      # 5%


def _force_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.index, pd.DatetimeIndex):
        return df
    if "date" in df.columns:
        idx = pd.to_datetime(df["date"], errors="coerce")
        out = df.copy()
        out.index = idx
        out = out.drop(columns=["date"], errors="ignore")
        out = out[~out.index.isna()]
        return out

    idx = pd.to_datetime(df.index, errors="coerce")
    if idx.isna().all():
        idx = pd.to_datetime(df.index, unit="ms", errors="coerce")
    if idx.isna().all():
        idx = pd.to_datetime(df.index, unit="s", errors="coerce")

    out = df.copy()
    out.index = idx
    out = out[~out.index.isna()]
    return out


def load_btc_research_macro(target_index: pd.DatetimeIndex) -> pd.Series:
    p = Path(BTC_RESEARCH_PATH)
    if not p.exists():
        raise FileNotFoundError(f"Missing {BTC_RESEARCH_PATH} (commit it to the repo).")

    df = pd.read_csv(p)

    # date col
    if "date" not in df.columns and "Date" in df.columns:
        df["date"] = df["Date"]
    if "date" not in df.columns:
        raise ValueError(f"Research BTC file missing date column. Columns={list(df.columns)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"]).set_index("date")

    # volume optional
    if "volume" not in df.columns:
        df["volume"] = 0.0

    for c in ["open", "high", "low", "close", "volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df[["open", "high", "low", "close", "volume"]].dropna()

    data = {"BTC": df}
    data = prepare_indicators(data)
    btc = data["BTC"]

    m = macro_filter(btc).astype(bool)
    m = m.reindex(target_index, method="ffill").fillna(False)
    return m


def _new_portfolio() -> Portfolio:
    return Portfolio(
        capital=INITIAL_CAPITAL,
        risk_per_trade=RISK_PER_TRADE,
        fee_rate_entry=FEE_RATE_ENTRY,
        fee_rate_exit=FEE_RATE_EXIT,
        slippage_rate=SLIPPAGE_RATE,
        max_positions=MAX_POSITIONS,
        max_total_risk_frac=MAX_TOTAL_RISK_FRAC,
    )


def run_one(label: str, data_dict: dict, macro_enabled: bool, macro_override: pd.Series | None):
    port = _new_portfolio()

    trades, equity = run_backtest(
        data_dict,
        port,
        btc_symbol="BTC",
        strategy_name="s2_ma_trend",
        risk_out_name=f"risk_frac_{label}.csv",
        macro_enabled=macro_enabled,
        macro_override=macro_override,
    )

    metrics = performance_metrics(trades, equity, initial_capital=INITIAL_CAPITAL)

    equity.to_csv(output_path_str(f"equity_{label}.csv"))
    pd.DataFrame(trades).to_csv(output_path_str(f"trades_{label}.csv"), index=False)

    return metrics


def main():
    data_dict = load_ohlcv_folder("data/")
    data_dict = prepare_indicators(data_dict)

    # normalize index to datetime everywhere
    for sym in list(data_dict.keys()):
        data_dict[sym] = _force_datetime_index(data_dict[sym]).sort_index()

    if "BTC" not in data_dict:
        raise KeyError(f"BTC missing from data/. Found: {list(data_dict.keys())}")

    btc_idx = data_dict["BTC"].index
    if not isinstance(btc_idx, pd.DatetimeIndex):
        raise TypeError(f"BTC index is not datetime after normalization: {btc_idx.dtype}")

    macro_kraken = macro_filter(data_dict["BTC"]).astype(bool).reindex(btc_idx).fillna(False)
    macro_research = load_btc_research_macro(btc_idx)

    results = {}
    results["s2_no_macro"] = run_one("s2_no_macro", data_dict, macro_enabled=False, macro_override=None)
    results["s2_macro_kraken"] = run_one("s2_macro_kraken", data_dict, macro_enabled=True, macro_override=macro_kraken)
    results["s2_macro_research"] = run_one("s2_macro_research", data_dict, macro_enabled=True, macro_override=macro_research)

    out = output_path_str("macro_compare_s2.json")
    Path(out).write_text(json.dumps(results, indent=2))

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
