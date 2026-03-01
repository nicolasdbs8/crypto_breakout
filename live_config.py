# live_config.py
from dataclasses import dataclass
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
