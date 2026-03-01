# daily_run.py
from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class StepResult:
    name: str
    ok: bool
    returncode: int = 0
    err_tail: str = ""
    out_tail: str = ""


def run_capture(cmd: list[str], title: str, cwd: Path) -> StepResult:
    print("\n" + "=" * 80)
    print(title)
    print("CMD:", " ".join(cmd))
    print("=" * 80)

    p = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        env={**os.environ, "PYTHONPATH": str(cwd)},  # ensure local imports
    )

    if p.stdout:
        print(p.stdout)
    if p.stderr:
        print("[stderr]\n" + p.stderr)

    def tail(s: str, n: int = 1200) -> str:
        s = (s or "").strip()
        return s[-n:] if len(s) > n else s

    ok = p.returncode == 0
    return StepResult(
        name=title,
        ok=ok,
        returncode=p.returncode,
        out_tail=tail(p.stdout),
        err_tail=tail(p.stderr),
    )


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
        r = requests.post(url, data=payload, timeout=20)
        if r.status_code != 200:
            print(f"[telegram] non-200 response: {r.status_code} body={r.text[:500]}")
        else:
            print("[telegram] sent OK")
    except Exception as e:
        print(f"[telegram] send failed: {e}")


def file_size(p: Path) -> int:
    try:
        return p.stat().st_size
    except Exception:
        return 0


def orders_summary(root: Path) -> str:
    p = root / "orders_today.csv"
    if not p.exists():
        return "orders_today.csv: MISSING"
    if file_size(p) == 0:
        return "orders_today.csv: EMPTY (no orders)"
    try:
        lines = p.read_text(encoding="utf-8").splitlines()
        n = max(0, len(lines) - 1)
        head = "\n".join(lines[: min(len(lines), 8)])
        return f"orders_today.csv: {n} orders\nHEAD:\n{head}"
    except Exception as e:
        return f"orders_today.csv: present but unreadable ({type(e).__name__}: {e})"


def parse_last_metrics(out_tail: str) -> str:
    """
    main.py prints a dict like {'StartEquity':..., 'CAGR':...}
    We'll try to grab the last {...} block from stdout tail.
    """
    s = out_tail or ""
    i = s.rfind("{")
    j = s.rfind("}")
    if i != -1 and j != -1 and j > i:
        return s[i : j + 1].strip()
    return ""


def main() -> None:
    root = Path(__file__).resolve().parent
    py = sys.executable

    in_actions = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"

    # where artifacts should land
    out_dir = root / "data" / "outputs" / "analysis" / "daily"
    out_dir.mkdir(parents=True, exist_ok=True)

    results: list[StepResult] = []

    # 0) UPDATE data
    if in_actions:
        # Binance blocked on GH runners -> use Kraken recent candles
        results.append(run_capture([py, "update_data_kraken_recent.py"], "0) UPDATE data (Kraken recent, CI-safe)", root))
    else:
        results.append(run_capture([py, "update_data.py"], "0) UPDATE data (local)", root))

    # If update failed, we stop early (but still try to telegram)
    hard_fail = not results[-1].ok

    # 1) BACKTEST
    if not hard_fail:
        r_bt = run_capture(
            [py, "main.py", "--strategy", "s2_ma_trend", "--out_dir", str(out_dir)],
            "1) BACKTEST (S2) -> data/outputs/analysis/daily",
            root,
        )
        results.append(r_bt)
        hard_fail = hard_fail or (not r_bt.ok)

    # 2) MAKE orders (soft fail in paper stage)
    r_orders = run_capture([py, "make_orders.py"], "2) MAKE orders_today.csv", root)
    results.append(r_orders)

    # 3) PAPER sim (soft fail)
    r_paper = run_capture([py, "paper_sim.py"], "3) PAPER sim", root)
    results.append(r_paper)

    # Build Telegram message (always attempt)
    lines: list[str] = []
    lines.append("📈 Daily paper run")
    lines.append(f"Repo: {os.environ.get('GITHUB_REPOSITORY', '')}".strip())
    lines.append(f"Workflow: {os.environ.get('GITHUB_WORKFLOW', '')}".strip())
    lines.append("")
    for r in results:
        status = "OK ✅" if r.ok else f"FAIL ❌ (code={r.returncode})"
        lines.append(f"- {r.name}: {status}")

    # add metrics if we have them
    metrics = ""
    for r in results:
        if r.name.startswith("1) BACKTEST"):
            metrics = parse_last_metrics(r.out_tail)
            break
    if metrics:
        lines.append("")
        lines.append("Backtest metrics (stdout):")
        lines.append(metrics)

    lines.append("")
    lines.append(orders_summary(root))

    # attach the most relevant error tail (orders first)
    if not r_orders.ok and r_orders.err_tail:
        lines.append("")
        lines.append("make_orders error (tail):")
        lines.append(r_orders.err_tail[-800:])

    try_send_telegram("\n".join([x for x in lines if x.strip() != ""]))

    # Final exit code: fail only if UPDATE or BACKTEST failed
    if hard_fail:
        raise SystemExit("FAILED: hard-fail step (update or backtest).")

    print("\nDONE ✅")


if __name__ == "__main__":
    main()
