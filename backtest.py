import pandas as pd
from strategy import macro_filter, build_strategy
from paths import output_path_str

WF_START = None
WF_END = None


def run_backtest(
    data: dict,
    portfolio,
    *,
    btc_symbol: str = "BTC",
    strategy_name: str = "s1_breakout",
    risk_out_name: str = "risk_frac_daily.csv",
    debug_index: bool = False,
    macro_enabled: bool = True,
    macro_override: pd.Series | None = None,
):
    """
    Daily OHLCV backtest with explicit anti-lookahead timing:

    - Signals are computed on CLOSE of t-1.
    - Entries/OPEN exits execute at OPEN of t (with slippage + fees via Portfolio).
    - Intraday stops execute during day t at the stop level (Portfolio applies slippage/fees).

    macro_enabled:
      - True  -> macro gate active (liquidate + block entries when macro OFF)
      - False -> macro ignored (always treated as ON)

    macro_override:
      - optional boolean Series indexed by date, used instead of macro_filter(btc_df)
    """

    if btc_symbol not in data:
        raise KeyError(f"Missing {btc_symbol}.csv in data/. Found: {list(data.keys())}")

    strat = build_strategy(strategy_name)

    btc_df = data[btc_symbol]

    # Macro series
    if macro_override is not None:
        macro = macro_override.astype(bool).reindex(btc_df.index, method="ffill").fillna(False)
    else:
        macro = macro_filter(btc_df)  # computed per day close; applied with 1-day lag

    dates = btc_df.index

    if debug_index and len(dates) > 0:
        print("index dtype:", type(dates[0]), "example:", dates[0])

    if WF_START is not None and WF_END is not None:
        dates = dates[(dates >= WF_START) & (dates <= WF_END)]

    entries_raw = {sym: strat.entry_mask(df) for sym, df in data.items()}
    scores_raw  = {sym: strat.rank_score(df) for sym, df in data.items()}
    open_exit_raw = {sym: strat.open_exit_mask(df) for sym, df in data.items()}

    equity_curve = []
    risk_rows = []

    date_list = list(dates)

    for i, date in enumerate(date_list):
        prev_date = date_list[i - 1] if i > 0 else None

        open_prices = {}
        close_prices = {}

        for sym, df in data.items():
            if date in df.index:
                open_prices[sym] = float(df.loc[date, "open"])
                close_prices[sym] = float(df.loc[date, "close"])

        equity_open = portfolio.equity(open_prices)
        risk_on_book = portfolio._risk_on_book()
        risk_frac = (risk_on_book / equity_open) if equity_open > 0 else 0.0
        risk_rows.append((date, risk_on_book, risk_frac))

        if prev_date is None:
            eq_close = portfolio.equity(close_prices)
            equity_curve.append((date, eq_close))
            continue

        macro_prev = bool(macro.loc[prev_date]) if prev_date in macro.index else False
        if not macro_enabled:
            macro_prev = True

        # A) MACRO OFF: liquidate all ALTS at today's OPEN
        if not macro_prev:
            for sym in list(portfolio.positions.keys()):
                if sym in open_prices:
                    portfolio.exit(date, sym, open_prices[sym], reason="macro_off")
            eq_close = portfolio.equity(close_prices)
            equity_curve.append((date, eq_close))
            continue

        # B) OPEN exits from signals known at prev close
        for sym in list(portfolio.positions.keys()):
            if sym not in data:
                continue
            df = data[sym]
            if prev_date not in df.index:
                continue

            try:
                exit_prev = bool(open_exit_raw[sym].loc[prev_date])
            except Exception:
                exit_prev = False

            if exit_prev and (sym in open_prices):
                portfolio.exit(date, sym, open_prices[sym], reason="exit_open")

        equity_open = portfolio.equity(open_prices)

        # C) ENTRIES at today's OPEN based on prev close signals
        ranked = []
        for sym, df in data.items():
            if sym == btc_symbol:
                continue
            if prev_date not in df.index:
                continue
            sc = scores_raw[sym].loc[prev_date]
            if sc != sc:
                continue
            ranked.append((sym, float(sc)))

        ranked.sort(key=lambda x: x[1], reverse=True)
        top_symbols = [s for s, _ in ranked[:3]]

        for sym in top_symbols:
            if sym not in open_prices:
                continue
            if sym in portfolio.positions:
                continue
            if bool(entries_raw[sym].loc[prev_date]):
                atr20_prev = data[sym].loc[prev_date, "ATR20"] if "ATR20" in data[sym].columns else None
                portfolio.enter(
                    date=date,
                    symbol=sym,
                    open_price=open_prices[sym],
                    atr20=atr20_prev,
                    equity_now=equity_open,
                    count_in_limit=True,
                    risk_override=None,
                    qty_multiplier=1.0,
                )

        # D) INTRADAY STOPS
        for sym in list(portfolio.positions.keys()):
            pos = portfolio.positions.get(sym)
            if pos is None:
                continue
            if sym not in data:
                continue
            df = data[sym]
            if date not in df.index:
                continue

            low_today = float(df.loc[date, "low"])
            stop_price = float(pos.stop)
            if low_today <= stop_price:
                portfolio.exit(date, sym, stop_price, reason="stop_ATR")

        if getattr(strat, "ll50_stop_enabled", True):
            for sym in list(portfolio.positions.keys()):
                if sym not in data:
                    continue
                df = data[sym]
                if date not in df.index or prev_date not in df.index:
                    continue

                ll50_prev = df.loc[prev_date, "LL50"] if "LL50" in df.columns else None
                if ll50_prev is None or ll50_prev != ll50_prev:
                    continue

                low_today = float(df.loc[date, "low"])
                if low_today <= float(ll50_prev):
                    portfolio.exit(date, sym, float(ll50_prev), reason="stop_LL50")

        eq_close = portfolio.equity(close_prices)
        equity_curve.append((date, eq_close))

    equity_df = pd.DataFrame(equity_curve, columns=["date", "equity"]).set_index("date")
    risk_df = pd.DataFrame(risk_rows, columns=["date", "risk_on_book", "risk_frac"]).set_index("date")
    risk_df.to_csv(output_path_str(risk_out_name))

    return portfolio.trade_log, equity_df
