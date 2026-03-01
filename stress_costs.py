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
    return p.stdout


def main():
    root = Path(__file__).resolve().parent
    py = sys.executable

    # We only stress-test the contenders
    strategies = ["s1_breakout", "s2_ma_trend"]

    scenarios = [
        # Stress 1
        {
            "name": "stress1_fee0p30_slip0p25",
            "fee_entry": 0.0030,
            "fee_exit": 0.0030,
            "slippage": 0.0025,
        },
        # Stress 2 (ugly)
        {
            "name": "stress2_fee0p35_slip0p30",
            "fee_entry": 0.0035,
            "fee_exit": 0.0035,
            "slippage": 0.0030,
        },
    ]

    base_out = root / "data" / "outputs" / "analysis"
    base_out.mkdir(parents=True, exist_ok=True)

    for sc in scenarios:
        out_dir = base_out / sc["name"]
        out_dir.mkdir(parents=True, exist_ok=True)

        print("\n" + "#" * 80)
        print(f"SCENARIO: {sc['name']} -> out_dir={out_dir}")
        print("#" * 80)

        for strat in strategies:
            title = f"main.py --strategy {strat} ({sc['name']})"
            cmd = [
                py, str(root / "main.py"),
                "--strategy", strat,
                "--fee_entry", str(sc["fee_entry"]),
                "--fee_exit", str(sc["fee_exit"]),
                "--slippage", str(sc["slippage"]),
                "--out_dir", str(out_dir),
            ]
            run(cmd, title)

    print("\nDONE ✅")


if __name__ == "__main__":
    main()