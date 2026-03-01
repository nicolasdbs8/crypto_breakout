import json
import re
import subprocess
import sys
from pathlib import Path


# -----------------------------
# Helpers
# -----------------------------
def run(cmd, title, allow_fail=False):
    print("\n" + "=" * 80)
    print(title)
    print("CMD:", " ".join(cmd))
    print("=" * 80)
    p = subprocess.run(cmd, text=True, capture_output=True)

    if p.stdout:
        print(p.stdout.strip())
    if p.stderr:
        print("\n[stderr]\n" + p.stderr.strip())

    if p.returncode != 0 and not allow_fail:
        raise SystemExit(f"FAILED: {title} (exit code {p.returncode})")

    return p.stdout, p.returncode


def parse_metrics_from_stdout(stdout: str) -> dict | None:
    if not stdout:
        return None
    m = re.search(r"\{.*\}", stdout, flags=re.DOTALL)
    if not m:
        return None
    txt = m.group(0).strip()
    try:
        j = txt.replace("'", '"')
        return json.loads(j)
    except Exception:
        try:
            return eval(txt, {"__builtins__": {}})
        except Exception:
            return None


def suffix_for_strategy(strat: str) -> str:
    return "" if strat == "s1_breakout" else f"_{strat}"


def find_output_file(root: Path, filename: str) -> Path:
    # prefer data/outputs/*
    candidates = [
        root / "data" / "outputs" / filename,
        root / "data" / "output" / filename,
        root / filename,
    ]
    for p in candidates:
        if p.exists():
            return p
    hits = list(root.glob(f"**/{filename}"))
    if hits:
        return hits[0]
    raise FileNotFoundError(f"Could not locate output file: {filename}")


def snapshot_files(root: Path) -> set[Path]:
    # only snapshot root-level files (pollution zone)
    return {p for p in root.iterdir() if p.is_file()}


def move_new_root_files(root: Path, before: set[Path], analysis_dir: Path, tag: str):
    after = snapshot_files(root)
    new_files = sorted(after - before)

    if not new_files:
        return

    analysis_dir.mkdir(parents=True, exist_ok=True)
    for p in new_files:
        # avoid moving source code by mistake
        if p.suffix.lower() in {".py", ".md", ".toml", ".yaml", ".yml"}:
            continue
        dest = analysis_dir / p.name
        if dest.exists():
            # avoid overwrite: add tag
            dest = analysis_dir / f"{p.stem}__{tag}{p.suffix}"
        p.replace(dest)


# -----------------------------
# Main
# -----------------------------
def main():
    root = Path(__file__).resolve().parent
    py = sys.executable

    out_dir = root / "data" / "outputs"
    analysis_dir = out_dir / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)
    analysis_dir.mkdir(parents=True, exist_ok=True)

    strategies = ["s1_breakout", "s2_ma_trend", "s3_tsmom"]

    # 0) Update data
    if (root / "update_data.py").exists():
        before = snapshot_files(root)
        run([py, str(root / "update_data.py")], "0) UPDATE data (CSV refresh)")
        move_new_root_files(root, before, analysis_dir, tag="update_data")
    else:
        print("\n[WARN] update_data.py not found, skipping data refresh")

    metrics_by_strat = {}

    for strat in strategies:
        suffix = suffix_for_strategy(strat)

        # 1) Backtest
        before = snapshot_files(root)
        title = f"1) RUN main.py --strategy {strat}" if strat != "s1_breakout" else "1) RUN main.py (baseline)"
        cmd = [py, str(root / "main.py")] + ([] if strat == "s1_breakout" else ["--strategy", strat])
        stdout, _ = run(cmd, title)
        move_new_root_files(root, before, analysis_dir, tag=f"main_{strat}")

        metrics = parse_metrics_from_stdout(stdout) or {}
        metrics_by_strat[strat] = metrics
        (analysis_dir / f"metrics_{strat}.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")

        # locate main outputs (in data/outputs/)
        trade_file = find_output_file(root, f"trade_log{suffix}.csv" if suffix else "trade_log.csv")
        risk_file  = find_output_file(root, f"risk_frac_daily{suffix}.csv" if suffix else "risk_frac_daily.csv")
        eq_file    = find_output_file(root, f"equity_curve{suffix}.csv" if suffix else "equity_curve.csv")

        # 2) Summaries (reads from outputs, no more hardcoded paths)
        summary_script = f"""
import pandas as pd
print('--- reason counts ({strat}) ---')
df = pd.read_csv(r'{trade_file.as_posix()}')
print(df['reason'].value_counts().to_string())
print('trades', len(df))

print('--- risk_frac ({strat}) ---')
rf = pd.read_csv(r'{risk_file.as_posix()}')
print('max', float(rf['risk_frac'].max()))
print('p99', float(rf['risk_frac'].quantile(0.99)))
"""
        before = snapshot_files(root)
        run([py, "-c", summary_script], f"2) Summaries ({strat})")
        move_new_root_files(root, before, analysis_dir, tag=f"summ_{strat}")

        # 2b) Verify coherence for each strategy
        if (root / "verify_coherence.py").exists():
            before = snapshot_files(root)
            cmd_v = [py, str(root / "verify_coherence.py")] + ([] if strat == "s1_breakout" else ["--strategy", strat])
            run(cmd_v, f"2b) VERIFY coherence ({strat})")
            move_new_root_files(root, before, analysis_dir, tag=f"verify_{strat}")

        # 3) Rolling walk (if exists) -> may write to root; we'll vacuum it
        if (root / "rolling_walk.py").exists():
            before = snapshot_files(root)
            # if your rolling_walk.py supports args, keep them; otherwise it will ignore/fail -> allow_fail
            cmd_r = [py, str(root / "rolling_walk.py")]
            run(cmd_r, f"3) RUN rolling_walk.py ({strat})", allow_fail=True)
            move_new_root_files(root, before, analysis_dir, tag=f"rolling_{strat}")

        # 4) Monte Carlo block (if exists)
        if (root / "monte_carlo_block.py").exists():
            before = snapshot_files(root)
            cmd_mc = [py, str(root / "monte_carlo_block.py")]
            run(cmd_mc, f"4) RUN monte_carlo_block.py ({strat})", allow_fail=True)
            move_new_root_files(root, before, analysis_dir, tag=f"mc_{strat}")

    # Scoreboard
    print("\n" + "=" * 80)
    print("SCOREBOARD (readable)")
    print("=" * 80)
    for strat in strategies:
        m = metrics_by_strat.get(strat, {})
        if not m:
            print(f"{strat}: (missing metrics)")
            continue
        print(
            f"{strat}: "
            f"CAGR={m.get('CAGR', float('nan')):.3f} "
            f"MaxDD={m.get('MaxDD', float('nan')):.3f} "
            f"PF={m.get('ProfitFactor', float('nan')):.2f} "
            f"Trades/yr={m.get('TradesPerYear', float('nan')):.2f} "
            f"NumTrades={int(m.get('NumTrades', 0))} "
            f"Top5%={m.get('PctPnL_Top5', float('nan')):.3f}"
        )

    print(f"\nArtifacts moved to: {analysis_dir}")
    print("\nDONE ✅")


if __name__ == "__main__":
    main()