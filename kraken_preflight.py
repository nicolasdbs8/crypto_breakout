import os
from pathlib import Path
import ccxt
import pandas as pd

ORDERS_PATH = Path("data/outputs/orders_today.csv")
QUOTE = "USDC"  # on trade spot contre USDC

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
    return ex

def _read_orders_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()  # no orders today (macro OFF, etc.)

    df = pd.read_csv(path)
    # minimal schema check
    required = {"action", "symbol"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"orders_today.csv missing columns: {missing} (got {list(df.columns)})")
    return df

def _kraken_pair(sym: str) -> str:
    # Your engine symbols are like "ETH", "SOL" (from CSV stem.upper()).
    # We trade spot vs USDC on Kraken: "ETH/USDC", etc.
    sym = sym.upper().strip()
    return f"{sym}/{QUOTE}"

def main():
    ex = _connect()

    # 1) Load markets
    markets = ex.load_markets()

    # 2) Balance snapshot
    bal = ex.fetch_balance()
    usdc = float(bal["total"].get("USDC", 0) or 0)
    nonzero_assets = {k: v for k, v in (bal.get("total") or {}).items() if v and float(v) > 0}

    print(f"[preflight] USDC total: {usdc:.8f}")
    print(f"[preflight] Non-zero totals (incl. dust): {len(nonzero_assets)} assets")

    # 3) Open orders
    try:
        open_orders = ex.fetch_open_orders()
    except Exception as e:
        raise SystemExit(f"[preflight] fetch_open_orders failed: {type(e).__name__}: {e}")

    print(f"[preflight] Open orders on Kraken: {len(open_orders)}")
    if len(open_orders) > 0:
        # Hard stop: we do not proceed if there are manual open orders.
        raise SystemExit("[preflight] ABORT: you have open orders on Kraken. Cancel them first.")

    # 4) Orders file
    df = _read_orders_csv(ORDERS_PATH)
    if df.empty:
        print(f"[preflight] {ORDERS_PATH.as_posix()} is EMPTY -> no orders today. ✅")
        return

    # 5) Validate each order
    problems = []
    planned = []

    for i, row in df.iterrows():
        action = str(row["action"]).upper().strip()
        sym = str(row["symbol"]).upper().strip()

        if action not in ("BUY", "SELL"):
            problems.append(f"row {i}: invalid action={action}")
            continue

        pair = _kraken_pair(sym)
        if pair not in markets:
            # Sometimes Kraken uses different symbols (e.g., XBT instead of BTC).
            # We fail fast so you see it.
            problems.append(f"row {i}: market not found on Kraken: {pair}")
            continue

        planned.append((action, sym, pair))

    if problems:
        print("[preflight] ❌ Problems:")
        for p in problems:
            print(" -", p)
        raise SystemExit("[preflight] ABORT due to problems above.")

    # 6) Print what we WOULD do
    print("[preflight] ✅ Orders look valid. Would place:")
    for action, sym, pair in planned:
        print(f" - {action} {pair} (engine_symbol={sym})")

    # 7) Cash sanity for BUY (no sizing here yet)
    if usdc <= 0:
        raise SystemExit("[preflight] ABORT: USDC balance is zero, cannot BUY.")

    print("[preflight] DONE ✅ (NO ORDERS SENT)")

if __name__ == "__main__":
    main()
