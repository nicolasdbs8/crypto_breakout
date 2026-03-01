import pandas as pd
import numpy as np
import json

INITIAL_CAPITAL = 3500.0
RISK_PER_TRADE = 0.0125

BTC_PATH = "data_research/BTC_USD_FULL.csv"


def compute_indicators(df):
    df["SMA200"] = df["close"].rolling(200).mean()
    df["SMA100"] = df["close"].rolling(100).mean()
    df["HH120"] = df["high"].rolling(120).max()
    df["LL50"] = df["low"].rolling(50).min()
    df["ATR20"] = (
        df["high"] - df["low"]
    ).rolling(20).mean()

    # slope SMA200
    df["SMA200_slope"] = df["SMA200"].diff(20)

    return df


def run_backtest(df):
    capital = INITIAL_CAPITAL
    equity_curve = []
    trades = []

    in_position = False
    entry_price = 0
    stop_price = 0
    position_size = 0

    for i in range(200, len(df)):

        row = df.iloc[i]
        prev = df.iloc[i - 1]

        macro_on = (
            prev["close"] > prev["SMA200"]
            and prev["SMA200_slope"] > 0
        )

        if not in_position:
            if (
                macro_on
                and prev["close"] > prev["HH120"]
                and prev["close"] > prev["SMA200"]
            ):
                entry_price = row["open"]
                risk_per_unit = 2 * prev["ATR20"]
                stop_price = entry_price - risk_per_unit
                risk_amount = capital * RISK_PER_TRADE
                position_size = risk_amount / risk_per_unit

                in_position = True
                entry_capital = capital

        else:
            exit_signal = False

            # stop
            if row["low"] < stop_price:
                exit_price = stop_price
                exit_signal = True

            # trend exit
            elif row["close"] < row["SMA100"]:
                exit_price = row["open"]
                exit_signal = True

            if exit_signal:
                pnl = (exit_price - entry_price) * position_size
                capital += pnl

                R = pnl / (entry_capital * RISK_PER_TRADE)
                trades.append(R)

                in_position = False

        equity_curve.append(capital)

    return trades, pd.Series(equity_curve)


def performance(trades, equity):
    start = INITIAL_CAPITAL
    end = equity.iloc[-1]

    total_return = (end / start) - 1
    years = len(equity) / 365
    cagr = (end / start) ** (1 / years) - 1 if years > 0 else 0

    max_dd = (equity / equity.cummax() - 1).min()

    wins = [r for r in trades if r > 0]
    losses = [r for r in trades if r <= 0]

    profit_factor = (
        sum(wins) / abs(sum(losses))
        if losses else np.inf
    )

    expectancy = np.mean(trades) if trades else 0

    return {
        "StartEquity": start,
        "EndEquity": float(end),
        "TotalReturn": float(total_return),
        "CAGR": float(cagr),
        "MaxDD": float(max_dd),
        "ProfitFactor": float(profit_factor),
        "Expectancy_R": float(expectancy),
        "NumTrades": len(trades),
    }


def main():
    df = pd.read_csv(BTC_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    df = compute_indicators(df)

    trades, equity = run_backtest(df)
    results = performance(trades, equity)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
