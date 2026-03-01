import pandas as pd
from strategy import prepare_indicators, macro_filter

BTC_PATH = "data_kraken_long/BTC.csv"

def main():
    df = pd.read_csv(BTC_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates(subset=["date"]).set_index("date")
    df = df[["open", "high", "low", "close", "volume"]]

    data = {"BTC": df}
    data = prepare_indicators(data)
    btc = data["BTC"]

    macro = macro_filter(btc).astype(bool)

    total_days = len(macro)
    on_days = int(macro.sum())
    off_days = total_days - on_days

    print("----- KRAKEN XBTUSD MACRO SUMMARY (LONG) -----")
    print(f"Date range: {macro.index.min().date()} → {macro.index.max().date()}")
    print(f"Total days: {total_days}")
    print(f"Macro ON days: {on_days} ({on_days/total_days:.2%})")
    print(f"Macro OFF days: {off_days} ({off_days/total_days:.2%})")

    durations = []
    cur = bool(macro.iloc[0])
    length = 1
    for v in macro.iloc[1:]:
        v = bool(v)
        if v == cur:
            length += 1
        else:
            durations.append((cur, length))
            cur = v
            length = 1
    durations.append((cur, length))

    on_d = [d for s, d in durations if s]
    off_d = [d for s, d in durations if not s]

    print("\n----- REGIME DURATIONS -----")
    if on_d:
        print(f"ON avg duration: {sum(on_d)/len(on_d):.1f} days")
        print(f"ON max duration: {max(on_d)} days")
    if off_d:
        print(f"OFF avg duration: {sum(off_d)/len(off_d):.1f} days")
        print(f"OFF max duration: {max(off_d)} days")
    print("----------------------------")

if __name__ == "__main__":
    main()
