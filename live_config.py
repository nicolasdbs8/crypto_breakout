# live_config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from paths import output_path_str


@dataclass(frozen=True)
class LiveConfig:
    # Strategy / portfolio
    initial_capital: float = 3500.0
    risk_per_trade: float = 0.0125
    max_positions: int = 3

    # Costs (conservative taker)
    fee_rate_entry: float = 0.0026
    fee_rate_exit: float = 0.0026
    slippage_rate: float = 0.0020

    # Universe / paths
    data_dir: str = "data"
    state_path: str = "live_state.json"
    orders_path: str = output_path_str("orders_today.csv")
    journal_path: str = output_path_str("live_journal.csv")

    # Symbol conventions
    btc_symbol: str = "BTC"


def _env_float(name: str, default: float) -> float:
    v = os.environ.get(name, "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _env_str(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v if v else default


def load_live_config() -> LiveConfig:
    """
    Backward/forward compatible loader.

    Some versions of make_orders.py import `load_live_config`.
    Other versions import LiveConfig directly.
    This function makes both work.
    """
    return LiveConfig(
        initial_capital=_env_float("LIVE_INITIAL_CAPITAL", LiveConfig.initial_capital),
        risk_per_trade=_env_float("LIVE_RISK_PER_TRADE", LiveConfig.risk_per_trade),
        max_positions=_env_int("LIVE_MAX_POSITIONS", LiveConfig.max_positions),
        fee_rate_entry=_env_float("LIVE_FEE_ENTRY", LiveConfig.fee_rate_entry),
        fee_rate_exit=_env_float("LIVE_FEE_EXIT", LiveConfig.fee_rate_exit),
        slippage_rate=_env_float("LIVE_SLIPPAGE", LiveConfig.slippage_rate),
        data_dir=_env_str("LIVE_DATA_DIR", LiveConfig.data_dir),
        state_path=_env_str("LIVE_STATE_PATH", LiveConfig.state_path),
        orders_path=_env_str("LIVE_ORDERS_PATH", LiveConfig.orders_path),
        journal_path=_env_str("LIVE_JOURNAL_PATH", LiveConfig.journal_path),
        btc_symbol=_env_str("LIVE_BTC_SYMBOL", LiveConfig.btc_symbol),
    )
