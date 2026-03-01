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

    # handle two common formats:
    # 1) columns: date,equity
    # 2) first col is unnamed index-like date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
    else:
        # try first column as date
        first = df.columns[0]
        df = df.rename(columns={first: "date"})
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")

    if "equity" not in df.columns:
        raise ValueError("equity_curve.csv must have an 'equity' column.")

    s = df["equity"].astype(float).dropna().sort_index()
    if len(s) < 50:
        raise ValueError(f"Not enough equity points: {len(s)}. Need at least ~50.")
    return s


def main():
    eq = load_equity_curve("equity_curve.csv")

    # daily returns from equity curve (close-to-close)
    r = eq.pct_change().dropna().astype(float).values
    n_days = len(r)

    # Use the same calendar span as the backtest
    years = (eq.index[-1] - eq.index[0]).days / 365.0
    start_equity = float(eq.iloc[0])  # actual start point of curve

    # Monte Carlo parameters
    N = 10000
    rng = np.random.default_rng(42)

    end_eqs = np.empty(N, dtype=float)
    maxdds = np.empty(N, dtype=float)
    cagrs = np.empty(N, dtype=float)

    # IID bootstrap on daily returns (A)
    for k in range(N):
        sample = rng.choice(r, size=n_days, replace=True)
        equity_path = np.empty(n_days + 1, dtype=float)
        equity_path[0] = start_equity
        for i, ret in enumerate(sample, start=1):
            equity_path[i] = equity_path[i - 1] * (1.0 + ret)

        end_eqs[k] = equity_path[-1]
        maxdds[k] = max_drawdown(equity_path)
        cagrs[k] = compute_cagr(start_equity, equity_path[-1], years)

    def q(a, p): return float(np.quantile(a, p))

    summary = {
        "N_sims": int(N),
        "n_days": int(n_days),
        "years_span": float(years),
        "StartEquity_used": float(start_equity),

        "EndEquity_p05": q(end_eqs, 0.05),
        "EndEquity_p50": q(end_eqs, 0.50),
        "EndEquity_p95": q(end_eqs, 0.95),

        "CAGR_p05": q(cagrs, 0.05),
        "CAGR_p50": q(cagrs, 0.50),
        "CAGR_p95": q(cagrs, 0.95),

        # Drawdowns: note more negative = worse. Here p05 is "bad tail".
        "MaxDD_p05": q(maxdds, 0.05),
        "MaxDD_p50": q(maxdds, 0.50),
        "MaxDD_p95": q(maxdds, 0.95),

        "P(MaxDD<-0.35)": float(np.mean(maxdds < -0.35)),
        "P(MaxDD<-0.30)": float(np.mean(maxdds < -0.30)),
        "P(MaxDD<-0.25)": float(np.mean(maxdds < -0.25)),
        "P(CAGR<0.15)": float(np.mean(cagrs < 0.15)),
        "P(EndEquity<Start)": float(np.mean(end_eqs < start_equity)),
    }

    print("\n=== MONTE CARLO (daily IID bootstrap) ===")
    for k, v in summary.items():
        if isinstance(v, float):
            print(f"{k}: {v:.6f}")
        else:
            print(f"{k}: {v}")

    # save outputs
    pd.DataFrame([summary]).to_csv(output_path_str("monte_carlo_daily_summary.csv"), index=False)
    pd.DataFrame({"end_equity": end_eqs, "maxdd": maxdds, "cagr": cagrs}).to_csv(output_path_str("monte_carlo_daily_sims.csv"), index=False)

    worst = np.argsort(maxdds)[:10]
    worst_df = pd.DataFrame({
        "sim": worst,
        "end_equity": end_eqs[worst],
        "maxdd": maxdds[worst],
        "cagr": cagrs[worst],
    }).sort_values("maxdd")
    worst_df.to_csv(output_path_str("monte_carlo_daily_worst10.csv"), index=False)
    print("\nSaved: monte_carlo_daily_summary.csv, monte_carlo_daily_sims.csv, monte_carlo_daily_worst10.csv")


if __name__ == "__main__":
    main()
