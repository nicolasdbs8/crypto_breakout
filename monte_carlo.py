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


def main():
    # ---- Inputs (must exist) ----
    trades = pd.read_csv(resolve_input_path_str("trade_log.csv"))
    eq = pd.read_csv(resolve_input_path_str("equity_curve.csv"))

    # ---- Parse dates ----
    trades["entry_date"] = pd.to_datetime(trades["entry_date"])
    trades["exit_date"] = pd.to_datetime(trades["exit_date"])

    if "date" in eq.columns:
        eq["date"] = pd.to_datetime(eq["date"])
        eq = eq.set_index("date")
    else:
        # if your equity_curve.csv was saved with index, pandas may load it as unnamed col
        # try common fallback
        if eq.columns[0].lower().startswith("unnamed"):
            eq = eq.rename(columns={eq.columns[0]: "date"})
            eq["date"] = pd.to_datetime(eq["date"])
            eq = eq.set_index("date")
        else:
            raise ValueError("equity_curve.csv must have a 'date' column or an index-like first column.")

    eq = eq.sort_index()
    if "equity" not in eq.columns:
        raise ValueError("equity_curve.csv must have an 'equity' column.")

    # ---- Build equity lookup for entry_date (approx equity at entry) ----
    # We align to the last available equity value <= entry_date
    eq_series = eq["equity"].dropna().copy()
    eq_dates = eq_series.index.values

    def equity_at_or_before(ts: pd.Timestamp) -> float:
        # fast searchsorted
        i = np.searchsorted(eq_dates, np.datetime64(ts), side="right") - 1
        if i < 0:
            return float(eq_series.iloc[0])
        return float(eq_series.iloc[i])

    # ---- Compute per-trade relative return ----
    # ret_i = pnl / equity_entry_approx
    # This makes returns scale-invariant and MC-compatible with sizing.
    pnl = trades["pnl"].astype(float).values
    entry_eq = np.array([equity_at_or_before(ts) for ts in trades["entry_date"]], dtype=float)
    rets = pnl / entry_eq

    # sanity: clip absurd outliers if any (shouldn't be needed; keep wide)
    # Example: prevent any single trade from > +/- 80% of equity (usually impossible with your cap)
    rets = np.clip(rets, -0.8, 0.8)

    n_trades = len(rets)
    if n_trades < 10:
        raise ValueError("Not enough trades for Monte Carlo. Need at least ~10.")

    # ---- Time span (years) for CAGR comparability ----
    days = (eq_series.index[-1] - eq_series.index[0]).days
    years = days / 365.0 if days > 0 else 0.0

    start_equity = 3500.0  # keep consistent with your reports

    # ---- Monte Carlo settings ----
    N = 5000
    rng = np.random.default_rng(42)

    end_eqs = np.empty(N, dtype=float)
    maxdds = np.empty(N, dtype=float)
    cagrs = np.empty(N, dtype=float)

    # Bootstrap with replacement (standard)
    for k in range(N):
        sample = rng.choice(rets, size=n_trades, replace=True)
        equity_path = np.empty(n_trades + 1, dtype=float)
        equity_path[0] = start_equity
        for i, r in enumerate(sample, start=1):
            equity_path[i] = equity_path[i - 1] * (1.0 + r)

        end_eqs[k] = equity_path[-1]
        maxdds[k] = max_drawdown(equity_path)
        cagrs[k] = compute_cagr(start_equity, equity_path[-1], years)

    # ---- Summary ----
    def q(a, p): return float(np.quantile(a, p))

    summary = {
        "N_sims": N,
        "n_trades": n_trades,
        "years_span": years,
        "EndEquity_p05": q(end_eqs, 0.05),
        "EndEquity_p50": q(end_eqs, 0.50),
        "EndEquity_p95": q(end_eqs, 0.95),
        "CAGR_p05": q(cagrs, 0.05),
        "CAGR_p50": q(cagrs, 0.50),
        "CAGR_p95": q(cagrs, 0.95),
        "MaxDD_p05": q(maxdds, 0.05),  # note: more negative is worse
        "MaxDD_p50": q(maxdds, 0.50),
        "MaxDD_p95": q(maxdds, 0.95),
        "P(MaxDD<-0.35)": float(np.mean(maxdds < -0.35)),
        "P(CAGR<0.15)": float(np.mean(cagrs < 0.15)),
        "P(EndEquity<Start)": float(np.mean(end_eqs < start_equity)),
    }

    print("\n=== MONTE CARLO (bootstrap trade returns) ===")
    for k, v in summary.items():
        if isinstance(v, float):
            print(f"{k}: {v:.6f}")
        else:
            print(f"{k}: {v}")

    # Save outputs
    pd.DataFrame([summary]).to_csv(output_path_str("monte_carlo_summary.csv"), index=False)
    pd.DataFrame({"end_equity": end_eqs, "maxdd": maxdds, "cagr": cagrs}).to_csv(output_path_str("monte_carlo_sims.csv"), index=False)

    # Worst 10 sims (by MaxDD)
    worst = np.argsort(maxdds)[:10]
    worst_df = pd.DataFrame({
        "sim": worst,
        "end_equity": end_eqs[worst],
        "maxdd": maxdds[worst],
        "cagr": cagrs[worst],
    }).sort_values("maxdd")
    worst_df.to_csv(output_path_str("monte_carlo_worst10.csv"), index=False)
    print("\nSaved: monte_carlo_summary.csv, monte_carlo_sims.csv, monte_carlo_worst10.csv")


if __name__ == "__main__":
    main()
