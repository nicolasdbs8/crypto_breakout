import subprocess
import sys
from pathlib import Path

def run(cmd, title):
    print("\n" + "="*80)
    print(title)
    print("CMD:", " ".join(cmd))
    print("="*80)
    p = subprocess.run(cmd, text=True, capture_output=True)
    if p.stdout:
        print(p.stdout.strip())
    if p.stderr:
        print("\n[stderr]\n" + p.stderr.strip())
    if p.returncode != 0:
        raise SystemExit(f"FAILED: {title} (exit code {p.returncode})")

def main():
    root = Path(__file__).resolve().parent
    py = sys.executable

    # 0) update data (Binance CSV refresh)
    run([py, str(root / "update_data.py")], "0) UPDATE data")

    # 1) run main for the chosen strategy (S2)
    out_dir = root / "data" / "outputs" / "analysis" / "daily"
    out_dir.mkdir(parents=True, exist_ok=True)

    run([
        py, str(root / "main.py"),
        "--strategy", "s2_ma_trend",
        "--out_dir", str(out_dir),
    ], "1) BACKTEST (S2) -> outputs/analysis/daily")

    # 2) make orders (dry-run)
    if (root / "make_orders.py").exists():
        run([py, str(root / "make_orders.py")], "2) MAKE orders_today.csv")

    # 3) paper sim (stateful)
    if (root / "paper_sim.py").exists():
        run([py, str(root / "paper_sim.py")], "3) PAPER sim (live_state.json + live_journal.csv)")

    print("\nDONE ✅")

if __name__ == "__main__":
    main()