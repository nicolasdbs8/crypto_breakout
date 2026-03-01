# paper_sim.py
import json
from pathlib import Path
import pandas as pd

from live_config import LiveConfig
from data import load_ohlcv_folder
from strategy import prepare_indicators


def _load_state(cfg: LiveConfig) -> dict:
    p = Path(cfg.state_path)
    if not p.exists():
        return {"cash": cfg.initial_capital, "positions": {}, "last_date": None}
    return json.loads(p.read_text(encoding="utf-8"))


def _save_state(cfg: LiveConfig, state: dict) -> None:
    Path(cfg.state_path).write_text(json.dumps(state, indent=2), encoding="utf-8")


def _append_journal(cfg: LiveConfig, row: dict) -> None:
    p = Path(cfg.journal_path)
    df = pd.DataFrame([row])
    if p.exists():
        df.to_csv(p, mode="a", header=False, index=False)
    else:
        df.to_csv(p, index=False)


def _read_orders_safe(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    # if file is empty bytes, return empty df
    if p.stat().st_size == 0:
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    return df


def main():
    cfg = LiveConfig()
    state = _load_state(cfg)

    orders = _read_orders_safe(cfg.orders_path)
    if orders.empty:
        print("[paper_sim] No orders (empty orders_today.csv). Exiting cleanly.")
        return

    # Basic schema guard
    required = {"date", "action", "symbol", "reason", "ref_date"}
    if not required.issubset(set(orders.columns)):
        print(f"[paper_sim] orders schema invalid. cols={list(orders.columns)} required={sorted(required)}")
        return

    data = load_ohlcv_folder(cfg.data_dir)
    data = prepare_indicators(data)

    exec_date = pd.Timestamp(orders.iloc[0]["date"])
    ref_date = pd.Timestamp(orders.iloc[0]["ref_date"])

    cash = float(state.get("cash", cfg.initial_capital))
    positions = state.get("positions", {}) or {}

    def open_px(sym):
        return float(data[sym].loc[exec_date, "open"])

    def low_px(sym):
        return float(data[sym].loc[exec_date, "low"])

    def close_px(sym):
        return float(data[sym].loc[exec_date, "close"])

    def atr_ref(sym):
        return float(data[sym].loc[ref_date, "ATR20"])

    # Execute SELLs then BUYs at OPEN
    sells = orders[orders["action"] == "SELL"]
    buys = orders[orders["action"] == "BUY"]

    # SELL
    for _, r in sells.iterrows():
        sym = str(r["symbol"])
        if sym not in positions:
            continue
        if exec_date not in data[sym].index:
            continue

        qty = float(positions[sym]["qty"])
        px = open_px(sym)
        fill = px * (1.0 - cfg.slippage_rate)
        proceeds = qty * fill
        fee = proceeds * cfg.fee_rate_exit
        cash += (proceeds - fee)

        del positions[sym]

        _append_journal(cfg, {
            "date": str(exec_date.date()),
            "event": "EXIT_OPEN",
            "symbol": sym,
            "qty": qty,
            "price": fill,
            "fee": fee,
            "reason": str(r.get("reason", "exit")),
            "cash_after": cash
        })

    # BUY
    for _, r in buys.iterrows():
        sym = str(r["symbol"])
        if sym in positions:
            continue
        if exec_date not in data[sym].index or ref_date not in data[sym].index:
            continue

        px = open_px(sym)
        fill = px * (1.0 + cfg.slippage_rate)

        atr = atr_ref(sym)
        stop = fill - 2.0 * atr
        risk_per_unit = max(fill - stop, 1e-12)

        eq_open = cash
        for s2, p2 in positions.items():
            eq_open += float(p2["qty"]) * open_px(s2)

        risk_cash = eq_open * cfg.risk_per_trade
        qty = risk_cash / risk_per_unit

        cost = qty * fill
        fee = cost * cfg.fee_rate_entry
        total = cost + fee

        if total > cash:
            _append_journal(cfg, {
                "date": str(exec_date.date()),
                "event": "SKIP_BUY_NO_CASH",
                "symbol": sym,
                "qty": qty,
                "price": fill,
                "fee": fee,
                "reason": "no_cash",
                "cash_after": cash
            })
            continue

        cash -= total
        positions[sym] = {
            "qty": qty,
            "entry": fill,
            "stop": stop,
            "entry_date": str(exec_date.date()),
            "ref_date": str(ref_date.date())
        }

        _append_journal(cfg, {
            "date": str(exec_date.date()),
            "event": "ENTRY_OPEN",
            "symbol": sym,
            "qty": qty,
            "price": fill,
            "fee": fee,
            "reason": "entry",
            "stop": stop,
            "cash_after": cash
        })

    # Intraday stops (ATR + LL50 based on ref_date)
    for sym in list(positions.keys()):
        if exec_date not in data[sym].index or ref_date not in data[sym].index:
            continue

        low = low_px(sym)
        stop_atr = float(positions[sym]["stop"])
        ll50 = float(data[sym].loc[ref_date, "LL50"]) if "LL50" in data[sym].columns else None

        stop_hits = []
        if low <= stop_atr:
            stop_hits.append(("stop_ATR", stop_atr))
        if ll50 is not None and ll50 == ll50 and low <= float(ll50):
            stop_hits.append(("stop_LL50", float(ll50)))

        if stop_hits:
            reason, level = sorted(stop_hits, key=lambda x: x[1])[0]
            qty = float(positions[sym]["qty"])
            fill = float(level) * (1.0 - cfg.slippage_rate)
            proceeds = qty * fill
            fee = proceeds * cfg.fee_rate_exit
            cash += (proceeds - fee)

            del positions[sym]

            _append_journal(cfg, {
                "date": str(exec_date.date()),
                "event": "EXIT_STOP",
                "symbol": sym,
                "qty": qty,
                "price": fill,
                "fee": fee,
                "reason": reason,
                "cash_after": cash
            })

    # EOD MTM
    equity_close = cash
    for sym, pos in positions.items():
        if exec_date in data[sym].index:
            equity_close += float(pos["qty"]) * close_px(sym)

    state["cash"] = cash
    state["positions"] = positions
    state["last_date"] = str(exec_date.date())
    _save_state(cfg, state)

    _append_journal(cfg, {
        "date": str(exec_date.date()),
        "event": "EOD",
        "symbol": "",
        "qty": "",
        "price": "",
        "fee": "",
        "reason": "",
        "cash_after": cash,
        "equity_close": equity_close,
        "n_positions": len(positions)
    })

    print(f"[paper_sim] done for {exec_date.date()} | equity_close={equity_close:.2f} | cash={cash:.2f} | positions={len(positions)}")


if __name__ == "__main__":
    main()