import pandas as pd
from data import load_ohlcv_folder
from strategy import prepare_indicators, macro_filter, entry_signal, rank_score
from paths import resolve_input_path_str

# This script validates the *timing + pricing* assumptions against trade_log.csv.
# It is meant to catch silent lookahead + fee/slippage mistakes.

BTC = "BTC"
BTC_OVERLAY = "BTC_OVERLAY"

def _as_dt(x):
    # trade_log may store datetime-like already; normalize
    return pd.to_datetime(x)

def main():
    df_trades = pd.read_csv(resolve_input_path_str("trade_log.csv"))
    if df_trades.empty:
        raise SystemExit("No trades found in trade_log.csv (nothing to verify).")

    # Load data
    data = load_ohlcv_folder("data/")
    data = prepare_indicators(data)

    btc_df = data[BTC].copy()
    macro = macro_filter(btc_df)
    entries_raw = {sym: entry_signal(d) for sym, d in data.items()}
    scores_raw = {sym: rank_score(d) for sym, d in data.items()}

    df_trades["entry_date"] = _as_dt(df_trades["entry_date"])
    df_trades["exit_date"]  = _as_dt(df_trades["exit_date"])

    def get_open(sym, date):
        market_sym = BTC if sym == BTC_OVERLAY else sym
        d = data[market_sym]
        return float(d.loc[date, "open"])

    def get_low(sym, date):
        market_sym = BTC if sym == BTC_OVERLAY else sym
        d = data[market_sym]
        return float(d.loc[date, "low"])

    def get_close(sym, date):
        market_sym = BTC if sym == BTC_OVERLAY else sym
        d = data[market_sym]
        return float(d.loc[date, "close"])

    def get_col(sym, date, col):
        market_sym = BTC if sym == BTC_OVERLAY else sym
        d = data[market_sym]
        return d.loc[date, col] if col in d.columns else None

    # Estimate slippage from entries
    ratios = []
    for _, r in df_trades.iterrows():
        sym = r["symbol"]
        ed = r["entry_date"]
        market_sym = BTC if sym == BTC_OVERLAY else sym
        if market_sym not in data or ed not in data[market_sym].index:
            continue
        o = float(data[market_sym].loc[ed, "open"])
        entry = float(r["entry"])
        if o > 0:
            ratios.append(entry / o - 1.0)
    slip_est = float(pd.Series(ratios).median()) if ratios else 0.0

    # Estimate fee rates from log
    fee_entry_rates = []
    fee_exit_rates = []
    for _, r in df_trades.iterrows():
        qty = float(r["qty"])
        entry = float(r["entry"])
        exit_px = float(r["exit"])
        fee_entry = float(r.get("fee_entry", 0.0))
        fee_exit  = float(r.get("fee_exit", 0.0))
        if qty > 0 and entry > 0:
            fee_entry_rates.append(fee_entry / (qty * entry))
        if qty > 0 and exit_px > 0:
            fee_exit_rates.append(fee_exit / (qty * exit_px))

    fee_entry_est = float(pd.Series(fee_entry_rates).median()) if fee_entry_rates else 0.0
    fee_exit_est  = float(pd.Series(fee_exit_rates).median()) if fee_exit_rates else 0.0

    errors = []

    for idx, r in df_trades.iterrows():
        sym = r["symbol"]
        ed = r["entry_date"]
        xd = r["exit_date"]
        reason = str(r["reason"])

        all_dates = data[BTC].index
        if ed not in all_dates:
            errors.append((idx, "entry_date_not_in_btc_index", sym, ed))
            continue
        pos = all_dates.get_loc(ed)
        if pos == 0:
            errors.append((idx, "entry_on_first_day_impossible", sym, ed))
            continue
        prev = all_dates[pos - 1]

        if not bool(macro.loc[prev]):
            errors.append((idx, "macro_prev_off_but_trade_entered", sym, ed))

        if sym == BTC_OVERLAY:
            sma100_prev = get_col(BTC, prev, "SMA100")
            close_prev = get_close(BTC, prev)
            if sma100_prev is None or sma100_prev != sma100_prev or close_prev < float(sma100_prev):
                errors.append((idx, "overlay_entry_trend_fail_prev", sym, ed))
        else:
            if ed not in data[sym].index or prev not in data[sym].index:
                errors.append((idx, "alt_missing_dates", sym, ed))
            else:
                if not bool(entries_raw[sym].loc[prev]):
                    errors.append((idx, "alt_entry_signal_false_prev", sym, ed))

                ranked = []
                for s2, d2 in data.items():
                    if s2 == BTC:
                        continue
                    if prev not in d2.index:
                        continue
                    sc = scores_raw[s2].loc[prev]
                    if sc == sc:
                        ranked.append((s2, float(sc)))
                ranked.sort(key=lambda x: x[1], reverse=True)
                top3 = [s for s, _ in ranked[:3]]
                if sym not in top3:
                    errors.append((idx, "alt_not_in_top3_prev", sym, ed))

        if xd not in all_dates:
            errors.append((idx, "exit_date_not_in_btc_index", sym, xd))
            continue
        posx = all_dates.get_loc(xd)
        if posx == 0:
            errors.append((idx, "exit_on_first_day_impossible", sym, xd))
            continue
        prevx = all_dates[posx - 1]

        if reason == "macro_off":
            if bool(macro.loc[prevx]):
                errors.append((idx, "macro_off_but_macro_prev_true", sym, xd))

        elif reason == "exit_SMA100":
            market_sym = BTC if sym == BTC_OVERLAY else sym
            sma100_prev = get_col(market_sym, prevx, "SMA100")
            close_prev = get_close(market_sym, prevx)
            if sma100_prev is None or sma100_prev != sma100_prev or close_prev >= float(sma100_prev):
                errors.append((idx, "exit_SMA100_condition_not_met_prev", sym, xd))

        elif reason == "stop_ATR":
            stop_level = float(r["stop"])
            low_today = get_low(sym, xd)
            if low_today > stop_level + 1e-9:
                errors.append((idx, "stop_ATR_but_low_above_stop", sym, xd))
            exit_fill = float(r["exit"])
            exp = stop_level * (1.0 - slip_est)
            if abs(exit_fill - exp) / max(exp, 1e-9) > 0.02:
                errors.append((idx, "stop_ATR_exit_fill_not_consistent", sym, xd))

        elif reason == "stop_LL50":
            ll50_prev = get_col(sym, prevx, "LL50")
            if ll50_prev is None or ll50_prev != ll50_prev:
                errors.append((idx, "stop_LL50_missing_ll50_prev", sym, xd))
            else:
                low_today = get_low(sym, xd)
                if low_today > float(ll50_prev) + 1e-9:
                    errors.append((idx, "stop_LL50_but_low_above_ll50_prev", sym, xd))
                exit_fill = float(r["exit"])
                exp = float(ll50_prev) * (1.0 - slip_est)
                if abs(exit_fill - exp) / max(exp, 1e-9) > 0.02:
                    errors.append((idx, "stop_LL50_exit_fill_not_consistent", sym, xd))

    for idx, r in df_trades.iterrows():
        qty = float(r["qty"])
        entry = float(r["entry"])
        exit_px = float(r["exit"])
        fee_entry = float(r.get("fee_entry", 0.0))
        fee_exit  = float(r.get("fee_exit", 0.0))

        exp_entry = qty * entry * fee_entry_est
        exp_exit  = qty * exit_px * fee_exit_est
        if exp_entry > 0 and abs(fee_entry - exp_entry) / exp_entry > 0.02:
            errors.append((idx, "fee_entry_inconsistent", r["symbol"], fee_entry, exp_entry))
        if exp_exit > 0 and abs(fee_exit - exp_exit) / exp_exit > 0.02:
            errors.append((idx, "fee_exit_inconsistent", r["symbol"], fee_exit, exp_exit))

    if errors:
        print("COHERENCE FAIL")
        for e in errors[:50]:
            print(" -", e)
        if len(errors) > 50:
            print(f"... {len(errors)-50} more")
        raise SystemExit(f"{len(errors)} coherence errors")

    print("COHERENCE OK")
    print(f"Estimated slippage_rate ~ {slip_est:.6f} (median entry/open - 1)")
    print(f"Estimated fee_entry_rate ~ {fee_entry_est:.6f}, fee_exit_rate ~ {fee_exit_est:.6f}")
    print(f"Trades checked: {len(df_trades)}")

if __name__ == '__main__':
    main()
