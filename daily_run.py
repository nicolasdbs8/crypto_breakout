import os
import subprocess
import sys
from pathlib import Path

def run(cmd, title):
    print("\n" + "=" * 80)
    print(title)
    print("CMD:", " ".join(cmd))
    print("=" * 80)
    p = subprocess.run(cmd, text=True)
    if p.returncode != 0:
        raise SystemExit(f"FAILED: {title} (exit code {p.returncode})")

def main():
    root = Path(__file__).resolve().parent
    py = sys.executable

    in_actions = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"

    # 0) data update
    if in_actions:
        # Binance API blocked (HTTP 451) on GitHub runners -> use Kraken recent candles
        run([py, str(root / "update_data_kraken_recent.py")], "0) UPDATE data (Kraken recent, CI-safe)")
    else:
        run([py, str(root / "update_data.py")], "0) UPDATE data (Binance)")

    # 1) backtest to generate outputs (uses data/*.csv)
    out_dir = root / "data" / "outputs" / "analysis" / "daily"
    out_dir.mkdir(parents=True, exist_ok=True)

    run([
        py, str(root / "main.py"),
        "--strategy", "s2_ma_trend",
        "--out_dir", str(out_dir),
    ], "1) BACKTEST (S2) -> outputs/analysis/daily")

    # optional
    if (root / "make_orders.py").exists():
        run([py, str(root / "make_orders.py")], "2) MAKE orders_today.csv")
    if (root / "paper_sim.py").exists():
        run([py, str(root / "paper_sim.py")], "3) PAPER sim")

    print("\nDONE ✅")

if __name__ == "__main__":
    main()
