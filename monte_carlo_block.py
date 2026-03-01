import argparse
from pathlib import Path
import numpy as np
import pandas as pd


def compute_maxdd_from_equity(equity: np.ndarray) -> float:
    peak = np.maximum.accumulate(equity)
    dd = equity / peak - 1.0
    return float(dd.min())


def simulate_block_bootstrap(returns: np.ndarray, start_equity: float, block_len: int, n_sims: int, seed: int = 42):
    rng = np.random.default_rng(seed)
    n = len(returns)
    out_end = np.empty(n_sims, dtype=float)
    out_cagr = np.empty(n_sims, dtype=float)
    out_maxdd = np.empty(n_sims, dtype=float)

    years_span = n / 365.25

    for i in range(n_sims):
        # sample blocks until we have n returns
        res = np.empty(n, dtype=float)
        k = 0
        while k < n:
            j = rng.integers(0, n - block_len + 1)
            take = min(block_len, n - k)
            res[k:k+take] = returns[j:j+take]
            k += take

        equity = start_equity * np.cumprod(1.0 + res)
        out_end[i] = float(equity[-1])
        out_maxdd[i] = compute_maxdd_from_equity(equity)

        # CAGR
        if equity[-1] > 0 and years_span > 0:
            out_cagr[i] = float((equity[-1] / start_equity) ** (1.0 / years_span) - 1.0)
        else:
            out_cagr[i] = np.nan

    return out_end, out_cagr, out_maxdd, float(years_span)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--equity", default="equity_curve.csv", help="Path to equity_curve*.csv")
    p.add_argument("--out_prefix", default="", help="Prefix for output files (e.g., s2_ma_trend)")
    p.add_argument("--n_sims", type=int, default=10000, help="Number of MC simulations (default 10000)")
    p.add_argument("--block_lens", default="10,20,40", help="Comma-separated block lengths")
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()


def main():
    args = parse_args()
    eq_path = Path(args.equity)

    df = pd.read_csv(eq_path, index_col=0, parse_dates=True)
    if "equity" not in df.columns:
        raise SystemExit(f"{eq_path} must contain column 'equity'. Columns: {list(df.columns)}")

    eq = df["equity"].dropna().astype(float).values
    if len(eq) < 100:
        raise SystemExit("Equity curve too short for MC.")

    # Use simple daily returns based on equity curve (includes all strategy effects)
    rets = eq[1:] / eq[:-1] - 1.0
    start_equity = float(eq[0])

    block_lens = [int(x.strip()) for x in args.block_lens.split(",") if x.strip()]
    out_prefix = args.out_prefix.strip()
    suffix = f"_{out_prefix}" if out_prefix else ""

    summary_rows = []

    for L in block_lens:
        end_eq, cagr, maxdd, years_span = simulate_block_bootstrap(
            returns=rets,
            start_equity=start_equity,
            block_len=L,
            n_sims=args.n_sims,
            seed=args.seed + L,
        )

        def q(a, p):
            return float(np.quantile(a, p))

        row = {
            "block_len": L,
            "N_sims": args.n_sims,
            "n_days": int(len(rets)),
            "years_span": float(years_span),
            "StartEquity_used": float(start_equity),
            "EndEquity_p05": q(end_eq, 0.05),
            "EndEquity_p50": q(end_eq, 0.50),
            "EndEquity_p95": q(end_eq, 0.95),
            "CAGR_p05": q(cagr, 0.05),
            "CAGR_p50": q(cagr, 0.50),
            "CAGR_p95": q(cagr, 0.95),
            "MaxDD_p05": q(maxdd, 0.05),
            "MaxDD_p50": q(maxdd, 0.50),
            "MaxDD_p95": q(maxdd, 0.95),
            "P_MaxDD_lt_-0_40": float(np.mean(maxdd < -0.40)),
            "P_MaxDD_lt_-0_50": float(np.mean(maxdd < -0.50)),
            "P_MaxDD_lt_-0_60": float(np.mean(maxdd < -0.60)),
            "P_CAGR_lt_0_15": float(np.mean(cagr < 0.15)),
            "P_EndEquity_lt_Start": float(np.mean(end_eq < start_equity)),
        }
        summary_rows.append(row)

        print(f"\n=== MONTE CARLO (block bootstrap, L={L}) ===")
        print(f"block_len: {L}")
        print(f"N_sims: {args.n_sims}")
        print(f"n_days: {len(rets)}")
        print(f"years_span: {years_span:.6f}")
        print(f"StartEquity_used: {start_equity:.6f}")
        print(f"EndEquity_p05: {row['EndEquity_p05']:.6f}")
        print(f"EndEquity_p50: {row['EndEquity_p50']:.6f}")
        print(f"EndEquity_p95: {row['EndEquity_p95']:.6f}")
        print(f"CAGR_p05: {row['CAGR_p05']:.6f}")
        print(f"CAGR_p50: {row['CAGR_p50']:.6f}")
        print(f"CAGR_p95: {row['CAGR_p95']:.6f}")
        print(f"MaxDD_p05: {row['MaxDD_p05']:.6f}")
        print(f"MaxDD_p50: {row['MaxDD_p50']:.6f}")
        print(f"MaxDD_p95: {row['MaxDD_p95']:.6f}")
        print(f"P(MaxDD<-0.40): {row['P_MaxDD_lt_-0_40']:.6f}")
        print(f"P(MaxDD<-0.50): {row['P_MaxDD_lt_-0_50']:.6f}")
        print(f"P(MaxDD<-0.60): {row['P_MaxDD_lt_-0_60']:.6f}")
        print(f"P(CAGR<0.15): {row['P_CAGR_lt_0_15']:.6f}")
        print(f"P(EndEquity<Start): {row['P_EndEquity_lt_Start']:.6f}")

        sims_out = Path(f"monte_carlo_block_sims_L{L}{suffix}.csv")
        pd.DataFrame({"EndEquity": end_eq, "CAGR": cagr, "MaxDD": maxdd}).to_csv(sims_out, index=False)

    summary_out = Path(f"monte_carlo_block_summary{suffix}.csv")
    pd.DataFrame(summary_rows).to_csv(summary_out, index=False)
    print(f"\nSaved: {summary_out} and monte_carlo_block_sims_L*{suffix}.csv")


if __name__ == "__main__":
    main()