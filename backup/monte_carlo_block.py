import numpy as np
import pandas as pd
from paths import output_path_str, resolve_input_path_str


def max_drawdown(equity: np.ndarray) -> float:
    peak = np.maximum.accumulate(equity)
    dd = equity / peak - 1.0
    return float(dd.min())


def compute_cagr(start_eq: float, end_eq: float, years: float) -> float:
    if years <= 0 or start_eq <= 0:
        return np.nan
    return float((end_eq / start_eq) ** (1.0 / years) - 1.0)


def load_equity_curve(path="equity_curve.csv") -> pd.Series:
    df = pd.read_csv(resolve_input_path_str(path))

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
    else:
        first = df.columns[0]
        df = df.rename(columns={first: "date"})
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

    if "equity" not in df.columns:
        raise ValueError("equity_curve.csv must have an 'equity' column.")

    s = df["equity"].astype(float).dropna().sort_index()
    if len(s) < 200:
        raise ValueError(f"Not enough equity points: {len(s)}. Need at least ~200.")
    return s


def block_bootstrap_returns(r: np.ndarray, n_days: int, block_len: int, rng: np.random.Generator) -> np.ndarray:
    """
    Circular block bootstrap:
    - pick random start index
    - take block_len consecutive returns, wrapping around the end
    - repeat until we have n_days returns
    """
    N = len(r)
    out = np.empty(n_days, dtype=float)
    filled = 0

    while filled < n_days:
        start = int(rng.integers(0, N))
        take = min(block_len, n_days - filled)

        # slice with wrap
        end = start + take
        if end <= N:
            out[filled:filled+take] = r[start:end]
        else:
            first_part = N - start
            out[filled:filled+first_part] = r[start:N]
            remaining = take - first_part
            out[filled+first_part:filled+take] = r[0:remaining]

        filled += take

    return out


def run_mc_block(eq: pd.Series, block_len: int, N_sims: int = 10000, seed: int = 42):
    r = eq.pct_change().dropna().astype(float).values
    n_days = len(r)
    years = (eq.index[-1] - eq.index[0]).days / 365.0
    start_equity = float(eq.iloc[0])

    rng = np.random.default_rng(seed)

    end_eqs = np.empty(N_sims, dtype=float)
    maxdds = np.empty(N_sims, dtype=float)
    cagrs = np.empty(N_sims, dtype=float)

    for k in range(N_sims):
        sample = block_bootstrap_returns(r, n_days=n_days, block_len=block_len, rng=rng)

        equity_path = np.empty(n_days + 1, dtype=float)
        equity_path[0] = start_equity
        for i, ret in enumerate(sample, start=1):
            equity_path[i] = equity_path[i - 1] * (1.0 + ret)

        end_eqs[k] = equity_path[-1]
        maxdds[k] = max_drawdown(equity_path)
        cagrs[k] = compute_cagr(start_equity, equity_path[-1], years)

    return {
        "block_len": int(block_len),
        "N_sims": int(N_sims),
        "n_days": int(n_days),
        "years_span": float(years),
        "StartEquity_used": float(start_equity),
        "end_eqs": end_eqs,
        "maxdds": maxdds,
        "cagrs": cagrs,
    }


def summarize(end_eqs, maxdds, cagrs):
    def q(a, p): return float(np.quantile(a, p))
    return {
        "EndEquity_p05": q(end_eqs, 0.05),
        "EndEquity_p50": q(end_eqs, 0.50),
        "EndEquity_p95": q(end_eqs, 0.95),
        "CAGR_p05": q(cagrs, 0.05),
        "CAGR_p50": q(cagrs, 0.50),
        "CAGR_p95": q(cagrs, 0.95),
        "MaxDD_p05": q(maxdds, 0.05),
        "MaxDD_p50": q(maxdds, 0.50),
        "MaxDD_p95": q(maxdds, 0.95),
        "P(MaxDD<-0.40)": float(np.mean(maxdds < -0.40)),
        "P(MaxDD<-0.35)": float(np.mean(maxdds < -0.35)),
        "P(MaxDD<-0.30)": float(np.mean(maxdds < -0.30)),
        "P(MaxDD<-0.25)": float(np.mean(maxdds < -0.25)),
        "P(CAGR<0.15)": float(np.mean(cagrs < 0.15)),
        "P(EndEquity<Start)": float(np.mean(end_eqs < end_eqs[0] * 0 + 3500.0)),  # compare to 3500 baseline
    }


def main():
    eq = load_equity_curve("equity_curve.csv")

    # Test several block lengths (in trading days)
    # 10 = ~2 weeks, 20 = ~1 month, 40 = ~2 months
    block_lengths = [10, 20, 40]

    all_rows = []
    for L in block_lengths:
        out = run_mc_block(eq, block_len=L, N_sims=10000, seed=42)
        summ = summarize(out["end_eqs"], out["maxdds"], out["cagrs"])

        row = {
            "block_len": L,
            "N_sims": out["N_sims"],
            "n_days": out["n_days"],
            "years_span": out["years_span"],
            "StartEquity_used": out["StartEquity_used"],
            **summ,
        }
        all_rows.append(row)

        print(f"\n=== MONTE CARLO (block bootstrap, L={L}) ===")
        for k, v in row.items():
            if isinstance(v, float):
                print(f"{k}: {v:.6f}")
            else:
                print(f"{k}: {v}")

        # save per-L sims
        pd.DataFrame({
            "end_equity": out["end_eqs"],
            "maxdd": out["maxdds"],
            "cagr": out["cagrs"],
        }).to_csv(output_path_str(f"monte_carlo_block_sims_L{L}.csv"), index=False)

    # save summary table
    summary_df = pd.DataFrame(all_rows)
    summary_df.to_csv(output_path_str("monte_carlo_block_summary.csv"), index=False)
    print("\nSaved: monte_carlo_block_summary.csv and monte_carlo_block_sims_L*.csv")


if __name__ == "__main__":
    main()
