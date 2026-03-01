# daily_run.py
from __future__ import annotations

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


def try_send_telegram(text: str):
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
        r = requests.post(url, data=payload, timeout=20)
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


def _orders_summary(orders_path: Path) -> str:
    if not orders_path.exists():
        return f"{orders_path.as_posix()}: MISSING"
    if _file_size(orders_path) == 0:
        return f"{orders_path.as_posix()}: EMPTY (no orders)"
    try:
        lines = orders_path.read_text(encoding="utf-8").splitlines()
        n = max(0, len(lines) - 1)  # header + N rows
        head = "\n".join(lines[: min(len(lines), 8)])
        return f"{orders_path.as_posix()}: {n} orders\nHEAD:\n{head}"
    except Exception as e:
        return f"{orders_path.as_posix()}: present but unreadable ({type(e).__name__}: {e})"


def main():
    root = Path(__file__).resolve().parent
    py = sys.executable
    in_actions = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"

    # 0) UPDATE data
    if in_actions:
        run([py, str(root / "update_data_kraken_recent.py")], "0) UPDATE data (Kraken recent, CI-safe)")
    else:
        run([py, str(root / "update_data.py")], "0) UPDATE data (Binance/local)")

    # 1) BACKTEST (daily reference run)
    out_dir = root / "data" / "outputs" / "analysis" / "daily"
    out_dir.mkdir(parents=True, exist_ok=True)
    run(
        [py, str(root / "main.py"), "--strategy", "s2_ma_trend", "--out_dir", str(out_dir)],
        "1) BACKTEST (S2) -> data/outputs/analysis/daily",
    )

    # 2) MAKE orders (same strategy)
    run([py, str(root / "make_orders.py"), "--strategy", "s2_ma_trend"], "2) MAKE orders_today.csv")

    # 3) PAPER sim
    run([py, str(root / "paper_sim.py")], "3) PAPER sim")

    # Telegram summary
    orders_path = root / "data" / "outputs" / "orders_today.csv"
    msg = "\n".join(
        [
            "✅ Daily paper run OK",
            "Strategy: s2_ma_trend",
            "Out dir: data/outputs/analysis/daily",
            "",
            _orders_summary(orders_path),
        ]
    )
    try_send_telegram(msg)

    print("\nDONE ✅")


if __name__ == "__main__":
    main()
