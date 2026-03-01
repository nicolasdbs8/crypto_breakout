import pandas as pd
import numpy as np
import json

INITIAL_CAPITAL = 3500.0
RISK_PER_TRADE = 0.0125

BTC_PATH = "data_research/BTC_USD_FULL.csv"


def compute_indicators(df):
    df["SMA200"] = df["close"].rolling(200).mean()
    df["SMA100"] = df["close"].rolling(100).mean()
    df["HH120"] = df["high"].rolling(120).max().shift(1)
    df["LL50"] = df["low"].rolling(50).min().shift(1)

    df["TR"] = np.maximum(
        df["high"] - df["low"],
        np.maximum(
            abs(df["high"] - df["close"].shift(1)),
            abs(df["low"] - df["close"].shift(1)),
        ),
    )
    df["ATR20"] = df["TR"].rolling(20).mean()
    df["SMA200_slope"] = df["SMA200"].diff(20)

    return df


def run_backtest(df):
    capital = INITIAL_CAPITAL
    trades = []
    in_position = False

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
                and not np.isnan(prev["ATR20"])
            ):
                entry_price = row["open"]
                risk_per_unit = 2 * prev["ATR20"]
                if risk_per_unit <= 0:
                    continue
                stop_price = entry_price - risk_per_unit
                risk_amount = capital * RISK_PER_TRADE
                position_size = risk_amount / risk_per_unit
                entry_capital = capital
                in_position = True

        else:
            exit_signal = False

            if row["low"] <= stop_price:
                exit_price = stop_price
                exit_signal = True
            elif row["close"] < row["SMA100"]:
                exit_price = row["open"]
                exit_signal = True

            if exit_signal:
                pnl = (exit_price - entry_price) * position_size
                capital += pnl
                R = pnl / (entry_capital * RISK_PER_TRADE)
                trades.append(R)
                in_position = False

    return trades


def analyze(trades):
    trades = np.array(trades)

    wins = trades[trades > 0]
    losses = trades[trades <= 0]

    total_R = trades.sum()
    top3_R = np.sort(trades)[-3:].sum() if len(trades) >= 3 else trades.sum()
    top3_pct = top3_R / total_R if total_R != 0 else 0

    result = {
        "NumTrades": int(len(trades)),
        "HitRate": float(len(wins) / len(trades)) if len(trades) > 0 else 0,
        "AvgWin_R": float(wins.mean()) if len(wins) > 0 else 0,
        "AvgLoss_R": float(losses.mean()) if len(losses) > 0 else 0,
        "MaxWin_R": float(trades.max()) if len(trades) > 0 else 0,
        "MaxLoss_R": float(trades.min()) if len(trades) > 0 else 0,
        "Total_R": float(total_R),
        "Top3_R_pct_of_total": float(top3_pct),
        "Sorted_R": trades.tolist(),
    }

    return result


def main():
    df = pd.read_csv(BTC_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    df = compute_indicators(df)
    trades = run_backtest(df)

    analysis = analyze(trades)

    print(json.dumps(analysis, indent=2))


if __name__ == "__main__":
    main()
