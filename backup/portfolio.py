# portfolio.py
# Portfolio engine with:
# - Entry at OPEN with slippage and entry fee
# - Exit at price (OPEN or stop level) with slippage and exit fee
# - Separate fees: fee_rate_entry (maker-like) and fee_rate_exit (taker-like)
# - ATR stop sizing: stop = entry_fill - 2*ATR20
# - Total risk cap: max_total_risk_frac
# - Logs pnl net (includes both fees) + R_multiple net

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Optional, Any


@dataclass
class Position:
    symbol: str
    qty: float
    entry: float
    entry_date: Any
    stop: float
    risk_per_unit: float            # entry - stop (should be > 0)
    count_in_limit: bool = True
    fee_entry: float = 0.0          # stored to compute net pnl at exit


class Portfolio:
    def __init__(
        self,
        capital: float,
        risk_per_trade: float,
        fee_rate_entry: float,
        fee_rate_exit: float,
        slippage_rate: float,
        max_positions: int = 3,
        max_total_risk_frac: float = 0.05,
    ):
        self.cash: float = float(capital)
        self.positions: Dict[str, Position] = {}

        self.risk_per_trade: float = float(risk_per_trade)

        # Separate entry/exit fees
        self.fee_rate_entry: float = float(fee_rate_entry)
        self.fee_rate_exit: float = float(fee_rate_exit)

        self.slippage_rate: float = float(slippage_rate)
        self.max_positions: int = int(max_positions)
        self.max_total_risk_frac: float = float(max_total_risk_frac)

        self.trade_log = []

    # ---------- Helpers ----------

    def _counted_positions(self) -> int:
        return sum(1 for p in self.positions.values() if p.count_in_limit)

    def _risk_on_book(self) -> float:
        """Σ (entry - stop) * qty across open positions."""
        total = 0.0
        for p in self.positions.values():
            rpu = float(p.risk_per_unit)
            if rpu > 0:
                total += rpu * float(p.qty)
        return total

    def equity(self, prices: Dict[str, float]) -> float:
        """Mark-to-market equity using provided prices (typically CLOSE)."""
        eq = float(self.cash)
        for sym, pos in self.positions.items():
            px = prices.get(sym)
            if px is not None:
                eq += float(pos.qty) * float(px)
        return eq

    def size_position(self, equity: float, entry_price: float, stop_price: float, risk_pct: float):
        """
        qty = (equity * risk_pct) / (entry - stop)
        returns (qty, risk_per_unit)
        """
        equity = float(equity)
        entry_price = float(entry_price)
        stop_price = float(stop_price)
        risk_pct = float(risk_pct)

        risk_amount = equity * risk_pct
        risk_per_unit = entry_price - stop_price
        if risk_per_unit <= 0:
            return 0.0, 0.0
        qty = risk_amount / risk_per_unit
        if qty <= 0:
            return 0.0, 0.0
        return qty, risk_per_unit

    # ---------- Trading operations ----------

    def enter(
        self,
        date,
        symbol: str,
        open_price: float,
        atr20: float,
        equity_now: float,
        *,
        count_in_limit: bool = True,
        risk_override: Optional[float] = None,
        qty_multiplier: float = 1.0,
    ) -> None:
        """
        Enter LONG at OPEN with slippage and entry fee (fee_rate_entry).
        """
        if symbol in self.positions:
            return
        if count_in_limit and self._counted_positions() >= self.max_positions:
            return
        if atr20 is None:
            return
        try:
            atr20_f = float(atr20)
        except Exception:
            return
        if atr20_f != atr20_f:  # NaN
            return
        if equity_now is None:
            return

        risk_pct = self.risk_per_trade if (risk_override is None) else float(risk_override)

        fill_price = float(open_price) * (1.0 + self.slippage_rate)
        stop_price = fill_price - 2.0 * atr20_f

        qty, risk_per_unit = self.size_position(float(equity_now), fill_price, stop_price, risk_pct)
        if qty <= 0:
            return

        qty = qty * float(qty_multiplier)
        if qty <= 0:
            return

        # Total risk cap
        new_risk = max(risk_per_unit, 0.0) * qty
        total_risk_after = self._risk_on_book() + new_risk
        risk_cap = float(equity_now) * self.max_total_risk_frac
        if total_risk_after > risk_cap:
            return

        gross_cost = qty * fill_price
        fee_entry = gross_cost * self.fee_rate_entry
        total_cost = gross_cost + fee_entry
        if total_cost > self.cash:
            return

        self.cash -= total_cost
        self.positions[symbol] = Position(
            symbol=symbol,
            qty=float(qty),
            entry=float(fill_price),
            entry_date=date,
            stop=float(stop_price),
            risk_per_unit=float(risk_per_unit),
            count_in_limit=bool(count_in_limit),
            fee_entry=float(fee_entry),
        )

    def exit(self, date, symbol: str, price: float, reason: str = "signal") -> None:
        """
        Exit LONG with slippage and exit fee (fee_rate_exit).
        PnL and R are NET (include entry+exit fees).
        """
        pos = self.positions.get(symbol)
        if pos is None:
            return
        pos = self.positions.pop(symbol)

        fill_price = float(price) * (1.0 - self.slippage_rate)

        gross_proceeds = float(pos.qty) * fill_price
        fee_exit = gross_proceeds * self.fee_rate_exit
        net_proceeds = gross_proceeds - fee_exit
        self.cash += net_proceeds

        pnl_gross = (fill_price - float(pos.entry)) * float(pos.qty)
        pnl_net = pnl_gross - float(pos.fee_entry) - float(fee_exit)

        denom = float(pos.risk_per_unit) * float(pos.qty)
        R = (pnl_net / denom) if denom > 0 else 0.0

        self.trade_log.append(
            {
                "entry_date": pos.entry_date,
                "exit_date": date,
                "symbol": symbol,
                "qty": float(pos.qty),
                "entry": float(pos.entry),
                "exit": float(fill_price),
                "stop": float(pos.stop),
                "fee_entry": float(pos.fee_entry),
                "fee_exit": float(fee_exit),
                "pnl_gross": float(pnl_gross),
                "pnl": float(pnl_net),
                "R_multiple": float(R),
                "reason": str(reason),
                "count_in_limit": bool(pos.count_in_limit),
            }
        )