import pandas as pd
from indicators import sma, highest, lowest, atr, momentum, realized_vol, rolling_quantile, slope_simple, safe_div

def prepare_indicators(data: dict):
    for sym, df in data.items():
        c = df["close"]

        df["SMA200"] = sma(c, 200)
        df["SMA100"] = sma(c, 100)

        df["HH120"] = highest(c, 120)
        df["LL50"] = lowest(df["low"], 50)

        df["ATR20"] = atr(df, 20)

        df["mom90"] = momentum(c, 90)
        df["mom180"] = momentum(c, 180)

        df["vol30"] = realized_vol(c, 30)
        df["vol90"] = realized_vol(c, 90)

        df["vol30_p75"] = rolling_quantile(df["vol30"], 252, 0.75)

    return data

def macro_filter(btc_df: pd.DataFrame) -> pd.Series:
    cond1 = btc_df["close"] > btc_df["SMA200"]
    cond2 = slope_simple(btc_df["SMA200"], 20) > 0
    return (cond1 & cond2).fillna(False)

def entry_signal(df: pd.DataFrame) -> pd.Series:
    # IMPORTANT: HH120 must be shifted to avoid lookahead bias
    return (
        (df["close"] > df["HH120"].shift(1)) &
        (df["close"] > df["SMA200"]) &
        (df["vol30"] < df["vol30_p75"]) &
        (df["mom90"] > 0)
    ).fillna(False)

def exit_signal(df: pd.DataFrame) -> pd.Series:
    return (
        (df["close"] < df["SMA100"]) |
        (df["low"] < df["LL50"])
    ).fillna(False)

def rank_score(df: pd.DataFrame) -> pd.Series:
    # score = mom180 / vol90, but safe division
    return safe_div(df["mom180"], df["vol90"])