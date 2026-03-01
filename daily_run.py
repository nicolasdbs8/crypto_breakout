# daily_run.py
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


STRATEGY_NAME = "s4_donchian_atr"


def run_step(cmd: list[str], title: str, required: bool) -> tuple[bool, str]:
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
        print(msg + (" [REQUIRED]" if required else " [NON-FATAL]"))

    return ok, "\n".join([x for x in [out, err] if x])


def try_send_telegram(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        print("[telegram] missing secrets -> skip")
        return

    try:
        import requests
    except Exception:
        print("[telegram] requests not available -> skip")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    try:
        requests.post(
            url,
            data={
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        print("[telegram] sent OK")
    except Exception as e:
        print(f"[telegram] failed: {e}")


def main() -> None:
    root = Path(__file__).resolve().parent
    py = sys.executable
    in_actions = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"

    ok_all_required = True
    notes: list[str] = []

    # 0) UPDATE DATA
    if in_actions:
        ok, _ = run_step(
            [py, str(root / "update_data_kraken_recent.py")],
            "0) UPDATE data",
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
        notes.append("UPDATE failed")

    # 1) BACKTEST
    out_dir = root / "data" / "outputs" / "analysis" / "daily"
    out_dir.mkdir(parents=True, exist_ok=True)

    ok_bt, _ = run_step(
        [
            py,
            str(root / "main.py"),
            "--strategy",
            STRATEGY_NAME,
            "--out_dir",
            str(out_dir),
        ],
        f"1) BACKTEST ({STRATEGY_NAME})",
        required=True,
    )

    ok_all_required &= ok_bt
    if not ok_bt:
        notes.append("BACKTEST failed")

    # 2) MAKE ORDERS
    if (root / "make_orders.py").exists():
        ok_mo, _ = run_step(
            [py, str(root / "make_orders.py")],
            "2) MAKE orders",
            required=False,
        )
        if not ok_mo:
            notes.append("MAKE_ORDERS failed")
    else:
        notes.append("make_orders.py missing")

    # 3) PAPER SIM
    if (root / "paper_sim.py").exists():
        ok_ps, _ = run_step(
            [py, str(root / "paper_sim.py")],
            "3) PAPER sim",
            required=False,
        )
        if not ok_ps:
            notes.append("PAPER_SIM failed")
    else:
        notes.append("paper_sim.py missing")

    # Telegram summary
    summary = []
    summary.append("✅ Daily paper run finished")
    summary.append(f"Strategy: {STRATEGY_NAME}")
    summary.append("")

    orders_path = root / "data" / "outputs" / "orders_today.csv"

    if orders_path.exists() and orders_path.stat().st_size > 0:
        lines = orders_path.read_text(encoding="utf-8").splitlines()
        n = max(0, len(lines) - 1)
        summary.append(f"Orders today: {n}")
    else:
        summary.append("Orders today: 0")

    if notes:
        summary.append("")
        summary.append("⚠️ Notes:")
        summary.extend(notes)

    try_send_telegram("\n".join(summary))

    if not ok_all_required:
        raise SystemExit(1)

    print("\nDONE ✅")


if __name__ == "__main__":
    main()
