import pandas as pd
from indicators import (
    sma, highest, lowest, atr, momentum,
    realized_vol, rolling_quantile, slope_simple, safe_div
)

# =============================================================================
# Indicators
# =============================================================================

def prepare_indicators(data: dict):
    """
    Compute a superset of indicators used by all strategies.
    Keep this centralized to avoid accidental mismatches.
    """
    for sym, df in data.items():
        c = df["close"]

        # Long regime
        df["SMA200"] = sma(c, 200)
        df["SMA100"] = sma(c, 100)
        df["SMA50"]  = sma(c, 50)

        # Breakout / risk controls
        # Note: existing S1 uses HH120 computed on CLOSE; keep it for backward compatibility.
        df["HH120"] = highest(c, 120)

        # Add HIGH-based Donchian level for S4 (more correct than close-based)
        df["HH120_high"] = highest(df["high"], 120)

        # For LL stops you already use LOW-based 50
        df["LL50"]  = lowest(df["low"], 50)

        # ATR
        df["ATR20"] = atr(df, 20)

        # Add HIGH-based HH20 for Chandelier stop in S4
        df["HH20_high"] = highest(df["high"], 20)

        # Momentum
        df["mom90"]  = momentum(c, 90)
        df["mom180"] = momentum(c, 180)

        # Volatility
        df["vol30"] = realized_vol(c, 30)
        df["vol90"] = realized_vol(c, 90)
        df["vol30_p75"] = rolling_quantile(df["vol30"], 252, 0.75)

    return data


def macro_filter(btc_df: pd.DataFrame) -> pd.Series:
    """
    macro ON if:
      - BTC close > SMA200
      - slope(SMA200, 20) > 0
    """
    cond1 = btc_df["close"] > btc_df["SMA200"]
    cond2 = slope_simple(btc_df["SMA200"], 20) > 0
    return (cond1 & cond2).fillna(False)

# =============================================================================
# Strategy interface
# =============================================================================

class BaseStrategy:
    """
    Strategy produces:
      - entry_mask(df): boolean Series computed on same-day CLOSE.
      - rank_score(df): float Series computed on same-day CLOSE (higher is better).
      - open_exit_mask(df): boolean Series for OPEN-based exit at t,
            based on condition known at CLOSE t-1.
      - ll50_stop_enabled: if True, apply LL50(prev_day) intraday stop.
    """
    name: str = "base"
    ll50_stop_enabled: bool = True

    def entry_mask(self, df: pd.DataFrame) -> pd.Series:
        raise NotImplementedError

    def rank_score(self, df: pd.DataFrame) -> pd.Series:
        raise NotImplementedError

    def open_exit_mask(self, df: pd.DataFrame) -> pd.Series:
        # default: no open-based exit
        return pd.Series(False, index=df.index)

# =============================================================================
# S1: Breakout (your baseline)
# =============================================================================

class S1Breakout(BaseStrategy):
    name = "s1_breakout"
    ll50_stop_enabled = True

    def entry_mask(self, df: pd.DataFrame) -> pd.Series:
        # IMPORTANT: HH120 shifted to avoid lookahead.
        return (
            (df["close"] > df["HH120"].shift(1)) &
            (df["close"] > df["SMA200"]) &
            (df["vol30"] < df["vol30_p75"]) &
            (df["mom90"] > 0)
        ).fillna(False)

    def rank_score(self, df: pd.DataFrame) -> pd.Series:
        return safe_div(df["mom180"], df["vol90"])

    def open_exit_mask(self, df: pd.DataFrame) -> pd.Series:
        # exit_SMA100: if close < SMA100 at t-1 => exit at OPEN(t)
        return (df["close"] < df["SMA100"]).fillna(False)

# =============================================================================
# S2: MA trend (regime-following, less "spiky")
# =============================================================================

class S2MATrend(BaseStrategy):
    name = "s2_ma_trend"
    ll50_stop_enabled = True

    def entry_mask(self, df: pd.DataFrame) -> pd.Series:
        # Simple trend confirm:
        # - price above SMA200
        # - SMA50 above SMA200 (trend)
        # - mom180 > 0 (avoid dead ranges)
        return (
            (df["close"] > df["SMA200"]) &
            (df["SMA50"] > df["SMA200"]) &
            (df["mom180"] > 0)
        ).fillna(False)

    def rank_score(self, df: pd.DataFrame) -> pd.Series:
        # favor strong trend with volatility normalization
        return safe_div(df["mom180"], df["vol90"])

    def open_exit_mask(self, df: pd.DataFrame) -> pd.Series:
        # exit if trend breaks:
        # - close < SMA200 OR SMA50 < SMA200
        return ((df["close"] < df["SMA200"]) | (df["SMA50"] < df["SMA200"])).fillna(False)

# =============================================================================
# S3: Time-series momentum (TSMOM-ish)
# =============================================================================

class S3TSMOM(BaseStrategy):
    name = "s3_tsmom"
    ll50_stop_enabled = True

    def entry_mask(self, df: pd.DataFrame) -> pd.Series:
        # minimal: long only when momentum positive and in long regime
        return ((df["mom180"] > 0) & (df["close"] > df["SMA200"])).fillna(False)

    def rank_score(self, df: pd.DataFrame) -> pd.Series:
        # pure momentum ranking
        return df["mom180"].fillna(float("-inf"))

    def open_exit_mask(self, df: pd.DataFrame) -> pd.Series:
        # exit when momentum turns negative OR lose SMA200 regime
        return ((df["mom180"] < 0) | (df["close"] < df["SMA200"])).fillna(False)

# =============================================================================
# S4: Donchian breakout 120 (HIGH-based) + Chandelier ATR trailing + SMA100 exit
# =============================================================================

class S4DonchianATR(BaseStrategy):
    """
    Entry (signal on CLOSE t):
      - close > HH120_high(t-1)   (Donchian breakout, no lookahead)
      - close > SMA200           (simple trend filter)

    Exit (signal on CLOSE t):
      - close < SMA100           (trend deterioration)
      - close < (HH20_high(t-1) - 3*ATR20)  (Chandelier stop, volatility adaptive)

    Intraday stop (engine-level, if enabled):
      - low < LL50(t-1) => stop out (your existing LL50 mechanism)
    """
    name = "s4_donchian_atr"
    ll50_stop_enabled = True

    def entry_mask(self, df: pd.DataFrame) -> pd.Series:
        # HIGH-based Donchian level is shifted to avoid lookahead.
        hh120_prev = df["HH120_high"].shift(1)
        return (
            (df["close"] > hh120_prev) &
            (df["close"] > df["SMA200"])
        ).fillna(False)

    def rank_score(self, df: pd.DataFrame) -> pd.Series:
        # Same robust ranking as S1/S2: momentum normalized by volatility.
        return safe_div(df["mom180"], df["vol90"])

    def open_exit_mask(self, df: pd.DataFrame) -> pd.Series:
        # Chandelier stop computed using HH20_high(t-1) and ATR20(t)
        hh20_prev = df["HH20_high"].shift(1)
        chand_stop = hh20_prev - 3.0 * df["ATR20"]

        return (
            (df["close"] < df["SMA100"]) |
            (df["close"] < chand_stop)
        ).fillna(False)

# =============================================================================
# Factory
# =============================================================================

def build_strategy(name: str) -> BaseStrategy:
    """
    Supported:
      - s1_breakout (default)
      - s2_ma_trend
      - s3_tsmom
      - s4_donchian_atr
    """
    key = (name or "").strip().lower()
    if key in ("", "s1", "breakout", "s1_breakout"):
        return S1Breakout()
    if key in ("s2", "ma", "ma_trend", "s2_ma_trend"):
        return S2MATrend()
    if key in ("s3", "tsmom", "s3_tsmom"):
        return S3TSMOM()
    if key in ("s4", "donchian", "donchian_atr", "s4_donchian_atr"):
        return S4DonchianATR()
    raise ValueError(f"Unknown strategy: {name}")
