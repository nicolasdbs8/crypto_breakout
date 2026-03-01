# make_orders.py
import json
from pathlib import Path
import pandas as pd

from live_config import LiveConfig
from data import load_ohlcv_folder
from strategy import prepare_indicators, macro_filter, entry_signal, rank_score


ORDER_COLS = ["date", "action", "symbol", "qty", "reason", "ref_date"]


def _load_state(cfg: LiveConfig) -> dict:
    p = Path(cfg.state_path)
    if not p.exists():
        return {"cash": cfg.initial_capital, "positions": {}, "last_date": None}
    return json.loads(p.read_text(encoding="utf-8"))


def _save_state(cfg: LiveConfig, state: dict) -> None:
    Path(cfg.state_path).write_text(json.dumps(state, indent=2), encoding="utf-8")


def _write_orders_csv(cfg: LiveConfig, orders: list[dict]) -> None:
    df = pd.DataFrame(orders, columns=ORDER_COLS)  # ensures headers even if empty
    df.to_csv(cfg.orders_path, index=False)


def _pick_asof_date(data: dict, btc_symbol: str) -> pd.Timestamp:
    btc_df = data[btc_symbol]
    if len(btc_df.index) < 3:
        raise ValueError("Not enough BTC rows.")
    # last available close date used as ref_date (signals on ref close)
    return pd.Timestamp(btc_df.index[-1])


def main():
    cfg = LiveConfig()
    state = _load_state(cfg)

    data = load_ohlcv_folder(cfg.data_dir)
    data = prepare_indicators(data)

    if cfg.btc_symbol not in data:
        raise KeyError(f"Missing {cfg.btc_symbol} in data folder.")

    btc_df = data[cfg.btc_symbol]
    macro = macro_filter(btc_df)

    entries_raw = {sym: entry_signal(df) for sym, df in data.items()}
    scores_raw = {sym: rank_score(df) for sym, df in data.items()}

    asof = _pick_asof_date(data, cfg.btc_symbol)         # ref close date
    next_day = asof + pd.Timedelta(days=1)               # intended execution date (next open proxy)

    if asof not in macro.index:
        raise ValueError("asof date missing in macro series.")

    macro_on = bool(macro.loc[asof])

    positions = state.get("positions", {}) or {}
    orders: list[dict] = []

    # A) Macro OFF -> exit all positions at next open
    if not macro_on:
        for sym, pos in positions.items():
            orders.append({
                "date": str(next_day.date()),
                "action": "SELL",
                "symbol": sym,
                "qty": float(pos.get("qty", 0.0)),
                "reason": "macro_off",
                "ref_date": str(asof.date()),
            })

        _write_orders_csv(cfg, orders)
        print(f"[make_orders] macro OFF asof={asof.date()} -> exits={len(orders)} -> {cfg.orders_path}")

        state["last_date"] = str(asof.date())
        _save_state(cfg, state)
        return

    # B) SMA100 exits at next open
    for sym, pos in list(positions.items()):
        if sym not in data or asof not in data[sym].index:
            continue
        df = data[sym]
        sma100 = df.loc[asof, "SMA100"] if "SMA100" in df.columns else None
        close = float(df.loc[asof, "close"])
        if sma100 is not None and sma100 == sma100 and close < float(sma100):
            orders.append({
                "date": str(next_day.date()),
                "action": "SELL",
                "symbol": sym,
                "qty": float(pos.get("qty", 0.0)),
                "reason": "exit_SMA100",
                "ref_date": str(asof.date()),
            })

    # C) Rank top3 alts at asof
    ranked = []
    for sym, df in data.items():
        if sym == cfg.btc_symbol:
            continue
        if asof not in df.index:
            continue
        sc = scores_raw[sym].loc[asof]
        if sc == sc:
            ranked.append((sym, float(sc)))
    ranked.sort(key=lambda x: x[1], reverse=True)
    top3 = [s for s, _ in ranked[:3]]

    held = set(positions.keys())
    slots_free = max(0, cfg.max_positions - len(held))

    # D) Entries at next open for signals true at asof close
    if slots_free > 0:
        for sym in top3:
            if sym in held:
                continue
            if sym not in data or asof not in data[sym].index:
                continue
            if not bool(entries_raw[sym].loc[asof]):
                continue
            orders.append({
                "date": str(next_day.date()),
                "action": "BUY",
                "symbol": sym,
                "qty": "",  # computed by execution layer (paper_sim / ccxt_exec)
                "reason": "entry",
                "ref_date": str(asof.date()),
            })
            slots_free -= 1
            if slots_free <= 0:
                break

    _write_orders_csv(cfg, orders)

    state["last_date"] = str(asof.date())
    _save_state(cfg, state)

    print(f"[make_orders] asof={asof.date()} macro=ON top3={top3} orders={len(orders)} -> {cfg.orders_path}")
    if orders:
        print(pd.DataFrame(orders, columns=ORDER_COLS).to_string(index=False))


if __name__ == "__main__":
    main()