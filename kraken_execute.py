import os
from pathlib import Path
import pandas as pd
import ccxt

ORDERS_PATH = Path("data/outputs/orders_today.csv")
QUOTE = "USDC"

# Hard safety defaults (override via GitHub Secrets/Env if you want)
MAX_NOTIONAL_PER_ORDER = float(os.environ.get("MAX_NOTIONAL_PER_ORDER_USDC", "100"))
MAX_NOTIONAL_PER_DAY = float(os.environ.get("MAX_NOTIONAL_PER_DAY_USDC", "200"))

LIVE_TRADING = os.environ.get("LIVE_TRADING", "0").strip()  # must be "1" to send orders


def _connect():
    api_key = os.environ.get("KRAKEN_API_KEY")
    api_secret = os.environ.get("KRAKEN_API_SECRET")
    if not api_key or not api_secret:
        raise SystemExit("Missing KRAKEN_API_KEY / KRAKEN_API_SECRET in env.")

    ex = ccxt.kraken({
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
    })
    ex.load_markets()
    return ex


def _read_orders(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()

    df = pd.read_csv(path)
    # Accept the engine schema: date,action,symbol,ref_date,reason,(optional atr_ref,score)
    if "action" not in df.columns or "symbol" not in df.columns:
        raise SystemExit(f"orders_today.csv missing required columns. Got: {list(df.columns)}")
    df["action"] = df["action"].astype(str).str.upper().str.strip()
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    return df


def _pair(sym: str) -> str:
    sym = sym.upper().strip()
    if sym == "BTC":
        # ccxt unified symbol (works even if Kraken internal is XBT)
        return "BTC/USDC"
    return f"{sym}/{QUOTE}"


def _nonzero_assets(balance_total: dict) -> dict:
    out = {}
    for k, v in (balance_total or {}).items():
        try:
            fv = float(v)
        except Exception:
            continue
        if fv > 0:
            out[k] = fv
    return out


def _has_open_orders(ex) -> bool:
    open_orders = ex.fetch_open_orders()
    return len(open_orders) > 0


def _market_amount_precision(ex, market_symbol: str, amount: float) -> float:
    # returns float but already rounded to exchange precision
    s = ex.amount_to_precision(market_symbol, amount)
    return float(s)


def _market_cost_precision(ex, market_symbol: str, cost: float) -> float:
    # Kraken ccxt does not always support cost_to_precision; keep as is but round reasonably
    return float(f"{cost:.8f}")


def _min_amount_ok(market: dict, amount: float) -> bool:
    lim = (market.get("limits") or {}).get("amount") or {}
    min_amt = lim.get("min")
    if min_amt is None:
        return True
    try:
        return float(amount) >= float(min_amt)
    except Exception:
        return True


def _min_cost_ok(market: dict, cost: float) -> bool:
    lim = (market.get("limits") or {}).get("cost") or {}
    min_cost = lim.get("min")
    if min_cost is None:
        return True
    try:
        return float(cost) >= float(min_cost)
    except Exception:
        return True


def main():
    ex = _connect()

    # 0) Read orders
    df = _read_orders(ORDERS_PATH)
    if df.empty:
        print("[kraken_execute] orders_today.csv empty -> nothing to do.")
        return

    # 1) Hard safety checks
    bal = ex.fetch_balance()
    totals = bal.get("total") or {}
    free = bal.get("free") or {}

    usdc_free = float(free.get("USDC", 0) or 0)
    usdc_total = float(totals.get("USDC", 0) or 0)

    print(f"[kraken_execute] LIVE_TRADING={LIVE_TRADING} (must be '1' to send)")
    print(f"[kraken_execute] USDC free={usdc_free:.8f} total={usdc_total:.8f}")
    print(f"[kraken_execute] caps: per_order={MAX_NOTIONAL_PER_ORDER} USDC, per_day={MAX_NOTIONAL_PER_DAY} USDC")

    if _has_open_orders(ex):
        raise SystemExit("[kraken_execute] ABORT: open orders exist on Kraken. Cancel them first.")

    # 2) Summarize balances (sanity)
    nz = _nonzero_assets(totals)
    print(f"[kraken_execute] non-zero total assets: {len(nz)}")

    # 3) Execute plan (with caps)
    spent_today = 0.0

    for i, row in df.iterrows():
        action = row["action"]
        sym = row["symbol"]
        market_symbol = _pair(sym)

        if market_symbol not in ex.markets:
            raise SystemExit(f"[kraken_execute] ABORT: market not found: {market_symbol}")

        market = ex.markets[market_symbol]

        if action == "BUY":
            remaining_day = max(0.0, MAX_NOTIONAL_PER_DAY - spent_today)
            notional = min(MAX_NOTIONAL_PER_ORDER, remaining_day, usdc_free)

            if notional <= 0:
                print(f"[kraken_execute] SKIP BUY {market_symbol}: no remaining day cap or no USDC.")
                continue

            # price for amount estimation
            ticker = ex.fetch_ticker(market_symbol)
            price = float(ticker.get("last") or 0)
            if price <= 0:
                raise SystemExit(f"[kraken_execute] ABORT: invalid price for {market_symbol}: {price}")

            amount = notional / price
            amount = _market_amount_precision(ex, market_symbol, amount)

            if amount <= 0:
                print(f"[kraken_execute] SKIP BUY {market_symbol}: amount rounds to 0.")
                continue

            # min checks
            if not _min_amount_ok(market, amount) or not _min_cost_ok(market, notional):
                print(f"[kraken_execute] SKIP BUY {market_symbol}: below min limits (amount={amount}, cost={notional}).")
                continue

            print(f"[kraken_execute] PLAN BUY {market_symbol}: notional={notional:.2f} USDC -> amount≈{amount}")

            if LIVE_TRADING != "1":
                print("[kraken_execute] DRY RUN: not sending order.")
                continue

            # Market BUY: ccxt typically uses amount in base currency.
            order = ex.create_market_buy_order(market_symbol, amount)
            print(f"[kraken_execute] SENT BUY {market_symbol}: id={order.get('id')}")

            # refresh balances after order (simple and safe)
            bal = ex.fetch_balance()
            free = bal.get("free") or {}
            usdc_free = float(free.get("USDC", 0) or 0)
            spent_today += float(notional)

        elif action == "SELL":
            base = market["base"]
            base_free = float(free.get(base, 0) or 0)

            if base_free <= 0:
                print(f"[kraken_execute] SKIP SELL {market_symbol}: no {base} free.")
                continue

            amount = _market_amount_precision(ex, market_symbol, base_free)
            if amount <= 0 or not _min_amount_ok(market, amount):
                print(f"[kraken_execute] SKIP SELL {market_symbol}: amount too small after precision/min.")
                continue

            print(f"[kraken_execute] PLAN SELL {market_symbol}: amount={amount}")

            if LIVE_TRADING != "1":
                print("[kraken_execute] DRY RUN: not sending order.")
                continue

            order = ex.create_market_sell_order(market_symbol, amount)
            print(f"[kraken_execute] SENT SELL {market_symbol}: id={order.get('id')}")

            bal = ex.fetch_balance()
            free = bal.get("free") or {}
            usdc_free = float(free.get("USDC", 0) or 0)

        else:
            raise SystemExit(f"[kraken_execute] ABORT: invalid action '{action}' in orders_today.csv")

    print("[kraken_execute] DONE ✅")


if __name__ == "__main__":
    main()
