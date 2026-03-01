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
    """
    Reads orders_today.csv if present and returns a compact summary.
    """
    orders_path = root / "orders_today.csv"
    if not orders_path.exists():
        return "orders_today.csv: MISSING"

    if _file_size(orders_path) == 0:
        return "orders_today.csv: EMPTY (no orders)"

    # try to count lines quickly without pandas
    try:
        lines = orders_path.read_text(encoding="utf-8").splitlines()
        # header + N rows
        n = max(0, len(lines) - 1)
        head = "\n".join(lines[: min(len(lines), 6)])
        return f"orders_today.csv: {n} orders\nHEAD:\n{head}"
    except Exception as e:
        return f"orders_today.csv: present but unreadable ({type(e).__name__}: {e})"

def main():
    root = Path(__file__).resolve().parent
    py = sys.executable

    in_actions = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"

    # 0) UPDATE data
    if in_actions:
        # Binance API blocked (HTTP 451) on GitHub runners -> use Kraken recent candles
        run([py, str(root / "update_data_kraken_recent.py")], "0) UPDATE data (Kraken recent, CI-safe)")
    else:
        # Local dev can keep Binance full/incremental
        run([py, str(root / "update_data.py")], "0) UPDATE data (Binance)")

    # 1) BACKTEST
    out_dir = root / "data" / "outputs" / "analysis" / "daily"
    out_dir.mkdir(parents=True, exist_ok=True)

    run([
        py, str(root / "main.py"),
        "--strategy", "s2_ma_trend",
        "--out_dir", str(out_dir),
    ], "1) BACKTEST (S2) -> outputs/analysis/daily")

    # 2) MAKE orders
    if (root / "make_orders.py").exists():
        run([py, str(root / "make_orders.py")], "2) MAKE orders_today.csv")
    else:
        print("[warn] make_orders.py missing -> skip")

    # 3) PAPER sim
    if (root / "paper_sim.py").exists():
        run([py, str(root / "paper_sim.py")], "3) PAPER sim")
    else:
        print("[warn] paper_sim.py missing -> skip")

    # Telegram summary (never fail pipeline)
    summary_lines = []
    summary_lines.append("✅ Daily paper run OK")
    summary_lines.append("Strategy: s2_ma_trend")
    summary_lines.append(f"Out dir: data/outputs/analysis/daily")
    summary_lines.append("")
    summary_lines.append(_orders_summary(root))

    msg = "\n".join(summary_lines)
    try_send_telegram(msg)

    print("\nDONE ✅")

if __name__ == "__main__":
    main()
