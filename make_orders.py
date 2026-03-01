# make_orders.py
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from data import load_ohlcv_folder
from live_config import load_live_config
from strategy import build_strategy, macro_filter, prepare_indicators


def _latest_common_date(dfs: Dict[str, pd.DataFrame]) -> str:
    """
    Daily bars. Use BTC as anchor (macro filter depends on BTC).
    If BTC missing, fall back to the minimum of last available dates.
    """
    if "BTC" in dfs:
        return str(dfs["BTC"]["date"].iloc[-1])

    last_dates = [str(df["date"].iloc[-1]) for df in dfs.values() if len(df)]
    if not last_dates:
        raise ValueError("No data loaded.")
    return min(last_dates)


def _load_live_state(path: Path) -> dict:
    if not path.exists():
        return {"positions": {}, "cash": 0.0}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        # corrupted -> start fresh (paper_sim will rebuild)
        return {"positions": {}, "cash": 0.0}


def _write_orders(path: Path, rows: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        # create empty file (paper_sim handles "no orders" cleanly)
        path.write_text("", encoding="utf-8")
        return
    pd.DataFrame(rows).to_csv(path, index=False)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--strategy",
        default="s2_ma_trend",
        help="strategy key: s1_breakout / s2_ma_trend / s3_tsmom",
    )
    ap.add_argument("--max_positions", type=int, default=None, help="override config max_positions")
    ap.add_argument("--top_n", type=int, default=3, help="universe: keep top N by rank_score")
    args = ap.parse_args()

    cfg = load_live_config()
    strat = build_strategy(args.strategy)

    # load daily OHLCV CSVs
    data = load_ohlcv_folder("data/")
    # indicators (adds SMA/ATR/mom/vol, etc.)
    ind = prepare_indicators(data)

    asof = _latest_common_date(ind)
    btc = ind.get("BTC")
    if btc is None:
        raise KeyError("BTC data missing (needed for macro filter).")

    macro_on = bool(macro_filter(btc).iloc[-1])
    state = _load_live_state(Path(cfg.state_path))
    positions: Dict[str, dict] = state.get("positions", {}) or {}

    def atr_ref(sym: str, date: str) -> float | None:
        """
        Provide ATR reference to paper_sim so it can size + set initial stop.
        Column name is 'ATR20' in your indicator pipeline.
        """
        df = ind.get(sym)
        if df is None:
            return None
        row = df.loc[df["date"] == date]
        if row.empty:
            return None
        if "ATR20" not in row.columns:
            return None
        try:
            return float(row["ATR20"].iloc[0])
        except Exception:
            return None

    orders: List[dict] = []

    # 1) Exits first
    if not macro_on:
        for sym in sorted(positions.keys()):
            orders.append(
                {
                    "date": dt.date.today().isoformat(),
                    "action": "SELL",
                    "symbol": sym,
                    "ref_date": asof,
                    "reason": "macro_off",
                }
            )
        _write_orders(Path(cfg.orders_path), orders)
        print(f"[make_orders] macro OFF asof={asof} -> exits={len(orders)} -> {cfg.orders_path}")
        return

    # macro ON -> strategy exits for open positions
    for sym in sorted(positions.keys()):
        df = ind.get(sym)
        if df is None:
            continue
        exit_mask = strat.open_exit_mask(df)
        if bool(exit_mask.iloc[-1]):
            orders.append(
                {
                    "date": dt.date.today().isoformat(),
                    "action": "SELL",
                    "symbol": sym,
                    "ref_date": asof,
                    "reason": "exit_open",
                }
            )

    # 2) Entries if slots remain
    max_pos = args.max_positions if args.max_positions is not None else cfg.max_positions
    open_syms = set(positions.keys())

    exits_count = sum(o["action"] == "SELL" for o in orders)
    effective_open = max(0, len(open_syms) - exits_count)
    slots = max(0, int(max_pos) - effective_open)

    if slots <= 0:
        _write_orders(Path(cfg.orders_path), orders)
        print(f"[make_orders] no entry slots (max_positions={max_pos}). Wrote exits={exits_count} -> {cfg.orders_path}")
        return

    # candidates: all assets except BTC
    candidates: List[Tuple[str, float]] = []
    for sym, df in ind.items():
        if sym == "BTC":
            continue
        if sym in open_syms:
            continue  # no pyramiding for now

        entry_mask = strat.entry_mask(df)
        if not bool(entry_mask.iloc[-1]):
            continue

        score = float(strat.rank_score(df).iloc[-1])
        candidates.append((sym, score))

    candidates.sort(key=lambda x: x[1], reverse=True)
    picks = candidates[: max(0, min(args.top_n, len(candidates)))]

    for sym, score in picks[:slots]:
        ar = atr_ref(sym, asof)
        orders.append(
            {
                "date": dt.date.today().isoformat(),
                "action": "BUY",
                "symbol": sym,
                "ref_date": asof,
                "atr_ref": ar if ar is not None else "",
                "reason": f"entry_{args.strategy}",
                "score": score,
            }
        )

    _write_orders(Path(cfg.orders_path), orders)
    print(
        f"[make_orders] macro ON asof={asof} -> exits={exits_count} entries={sum(o['action']=='BUY' for o in orders)} -> {cfg.orders_path}"
    )


if __name__ == "__main__":
    main()
