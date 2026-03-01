import argparse
import pandas as pd

from data import load_ohlcv_folder
from strategy import prepare_indicators, macro_filter, build_strategy
from paths import resolve_input_path_str

BTC = "BTC"

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--strategy",
        default="s1_breakout",
        help="Strategy name (s1_breakout, s2_ma_trend, s3_tsmom)",
    )
    return p.parse_args()

def _as_dt(x):
    return pd.to_datetime(x)

def main():
    args = parse_args()
    strat = build_strategy(args.strategy)

    trade_file = "trade_log.csv" if args.strategy == "s1_breakout" else f"trade_log_{args.strategy}.csv"
    df_trades = pd.read_csv(resolve_input_path_str(trade_file))

    if df_trades.empty:
        raise SystemExit("No trades found.")

    data = load_ohlcv_folder("data/")
    data = prepare_indicators(data)

    btc_df = data[BTC]
    macro = macro_filter(btc_df)

    entries_raw = {sym: strat.entry_mask(d) for sym, d in data.items()}
    scores_raw  = {sym: strat.rank_score(d) for sym, d in data.items()}
    open_exit_raw = {sym: strat.open_exit_mask(d) for sym, d in data.items()}

    df_trades["entry_date"] = _as_dt(df_trades["entry_date"])
    df_trades["exit_date"]  = _as_dt(df_trades["exit_date"])

    errors = []

    all_dates = btc_df.index

    for idx, r in df_trades.iterrows():
        sym = r["symbol"]
        ed  = r["entry_date"]
        xd  = r["exit_date"]
        reason = str(r["reason"])

        if ed not in all_dates:
            errors.append((idx, "entry_not_in_index"))
            continue

        pos = all_dates.get_loc(ed)
        if pos == 0:
            errors.append((idx, "entry_on_first_day"))
            continue

        prev = all_dates[pos - 1]

        # ---- MACRO CHECK ----
        if not bool(macro.loc[prev]):
            errors.append((idx, "macro_off_but_trade_entered"))

        # ---- ENTRY SIGNAL CHECK ----
        if sym in data and prev in data[sym].index:
            if not bool(entries_raw[sym].loc[prev]):
                errors.append((idx, "entry_signal_false_prev"))

            ranked = []
            for s2, d2 in data.items():
                if s2 == BTC or prev not in d2.index:
                    continue
                sc = scores_raw[s2].loc[prev]
                if sc == sc:
                    ranked.append((s2, float(sc)))
            ranked.sort(key=lambda x: x[1], reverse=True)
            top3 = [s for s, _ in ranked[:3]]
            if sym not in top3:
                errors.append((idx, "not_in_top3_prev"))

        # ---- EXIT CHECK ----
        if xd not in all_dates:
            errors.append((idx, "exit_not_in_index"))
            continue

        posx = all_dates.get_loc(xd)
        if posx == 0:
            errors.append((idx, "exit_on_first_day"))
            continue

        prevx = all_dates[posx - 1]

        if reason == "macro_off":
            if bool(macro.loc[prevx]):
                errors.append((idx, "macro_off_wrong"))

        if reason == "exit_open":
            if not bool(open_exit_raw[sym].loc[prevx]):
                errors.append((idx, "open_exit_condition_not_met"))

    if errors:
        print("COHERENCE FAIL")
        for e in errors[:20]:
            print(" -", e)
        raise SystemExit(f"{len(errors)} errors found")

    print("COHERENCE OK")
    print(f"Strategy: {args.strategy}")
    print(f"Trades checked: {len(df_trades)}")

if __name__ == "__main__":
    main()