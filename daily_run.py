# daily_run.py
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def run_step(cmd: list[str], title: str, required: bool) -> tuple[bool, str]:
    """
    Runs a command, returns (ok, combined_log).
    If required=False, failure does NOT stop the pipeline immediately.
    """
    print("\n" + "=" * 80)
    print(title)
    print("CMD:", " ".join(cmd))
    print("=" * 80)

    p = subprocess.run(cmd, text=True, capture_output=True)
    out = (p.stdout or "").strip()
    err = (p.stderr or "").strip()

    if out:
        print(out)
    if err:
        print("\n[stderr]\n" + err)

    ok = (p.returncode == 0)
    if not ok:
        msg = f"FAILED (exit code {p.returncode})"
        if required:
            print(msg + " [REQUIRED]")
        else:
            print(msg + " [NON-FATAL]")
    return ok, "\n".join([x for x in [out, err] if x])


def try_send_telegram(text: str) -> None:
    """
    Sends a Telegram message if secrets are present.
    Never fails the pipeline if Telegram fails.
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        print("[telegram] missing TELEGRAM_BOT_TOKEN and/or TELEGRAM_CHAT_ID -> skip")
        return

    try:
        import requests
    except Exception as e:
        print(f"[telegram] requests not available ({e}) -> skip")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(url, data=payload, timeout=15)
        if r.status_code != 200:
            print(f"[telegram] non-200 response: {r.status_code} body={r.text[:300]}")
        else:
            print("[telegram] sent OK")
    except Exception as e:
        print(f"[telegram] send failed: {e}")


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except Exception:
        return 0


def _orders_summary(root: Path) -> str:
    orders_path = root / "data" / "outputs" / "orders_today.csv"

    if not orders_path.exists():
        return f"{orders_path.as_posix()} : MISSING"

    if _file_size(orders_path) == 0:
        return f"{orders_path.as_posix()} : EMPTY (no orders)"

    try:
        lines = orders_path.read_text(encoding="utf-8").splitlines()
        n = max(0, len(lines) - 1)
        head = "\n".join(lines[: min(len(lines), 6)])
        return f"{orders_path.as_posix()} : {n} orders\nHEAD:\n{head}"
    except Exception as e:
        return f"{orders_path.as_posix()} : unreadable ({type(e).__name__}: {e})"


def main() -> None:
    root = Path(__file__).resolve().parent
    py = sys.executable
    in_actions = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"

    ok_all_required = True
    notes: list[str] = []

    # 0) UPDATE data
    if in_actions:
        ok, _ = run_step(
            [py, "-m", "python", "update_data_kraken_recent.py"] if False else [py, str(root / "update_data_kraken_recent.py")],
            "0) UPDATE data (Kraken recent, CI-safe)",
            required=True,
        )
    else:
        ok, _ = run_step(
            [py, str(root / "update_data.py")],
            "0) UPDATE data (local)",
            required=True,
        )
    ok_all_required &= ok
    if not ok:
        notes.append("UPDATE data failed")

    # 1) BACKTEST (required)
    out_dir = root / "data" / "outputs" / "analysis" / "daily"
    out_dir.mkdir(parents=True, exist_ok=True)
    ok_bt, _ = run_step(
        [
            py, str(root / "main.py"),
            "--strategy", "s2_ma_trend",
            "--out_dir", str(out_dir),
        ],
        "1) BACKTEST (S2) -> data/outputs/analysis/daily",
        required=True,
    )
    ok_all_required &= ok_bt
    if not ok_bt:
        notes.append("BACKTEST failed")

    # 2) MAKE orders (non-fatal: we still want Telegram even if it crashes)
    if (root / "make_orders.py").exists():
        ok_mo, _ = run_step([py, str(root / "make_orders.py")], "2) MAKE orders_today.csv", required=False)
        if not ok_mo:
            notes.append("MAKE_ORDERS failed (see logs)")
    else:
        notes.append("make_orders.py missing -> skipped")

    # 3) PAPER sim (non-fatal)
    if (root / "paper_sim.py").exists():
        ok_ps, _ = run_step([py, str(root / "paper_sim.py")], "3) PAPER sim", required=False)
        if not ok_ps:
            notes.append("PAPER_SIM failed (see logs)")
    else:
        notes.append("paper_sim.py missing -> skipped")

    # Telegram summary (always attempt)
    summary_lines = []
    summary_lines.append("✅ Daily paper run finished")
    summary_lines.append("Strategy: s2_ma_trend")
    summary_lines.append("Out dir: data/outputs/analysis/daily")
    summary_lines.append("")
    summary_lines.append(_orders_summary(root))
    if notes:
        summary_lines.append("")
        summary_lines.append("⚠️ Notes:")
        summary_lines.extend([f"- {n}" for n in notes])

    try_send_telegram("\n".join(summary_lines))

    # Fail the job only if required steps failed
    if not ok_all_required:
        raise SystemExit(1)

    print("\nDONE ✅")


if __name__ == "__main__":
    main()
