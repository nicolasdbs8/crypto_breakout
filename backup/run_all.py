import subprocess
import sys
from pathlib import Path


def run(cmd, title):
    print("\n" + "=" * 80)
    print(title)
    print("CMD:", " ".join(cmd))
    print("=" * 80)
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

    # 0) (Optional) Update data step (if you add update_data.py later)
    if (root / "update_data.py").exists():
        run([py, str(root / "update_data.py")], "0) UPDATE data (CSV refresh)")

    # 1) Main backtest
    run([py, str(root / "main.py")], "1) RUN main.py (full-range backtest)")

    # 2) CSV summaries (reasons + risk_frac)
    summary_script = """
import pandas as pd
from paths import resolve_input_path_str

print('--- reason counts ---')
df = pd.read_csv(resolve_input_path_str('trade_log.csv'))
print(df['reason'].value_counts().to_string())
print('trades', len(df))

print('--- risk_frac ---')
rf = pd.read_csv(resolve_input_path_str('risk_frac_daily.csv'))
print('max', float(rf['risk_frac'].max()))
print('p99', float(rf['risk_frac'].quantile(0.99)))
"""
    run([py, "-c", summary_script], "2) Summaries (reasons + risk_frac)")

    # 2b) Coherence checks (anti-lookahead + pricing/fees sanity)
    if (root / "verify_coherence.py").exists():
        run([py, str(root / "verify_coherence.py")], "2b) VERIFY coherence (timing/pricing/fees)")

    # 3) Rolling walk
    if (root / "rolling_walk.py").exists():
        run([py, str(root / "rolling_walk.py")], "3) RUN rolling_walk.py (rolling 3y)")

    # 4) Monte Carlo block
    if (root / "monte_carlo_block.py").exists():
        run([py, str(root / "monte_carlo_block.py")], "4) RUN monte_carlo_block.py (block bootstrap)")

    # 5) Live dry-run: generate orders (optional)
    if (root / "make_orders.py").exists():
        run([py, str(root / "make_orders.py")], "5) MAKE orders (dry-run -> orders_today.csv)")

    # 6) Paper sim: simulate next-open execution (optional)
    if (root / "paper_sim.py").exists():
        run([py, str(root / "paper_sim.py")], "6) PAPER sim (update live_state.json + live_journal.csv)")

    print("\nDONE ✅")


if __name__ == "__main__":
    main()
