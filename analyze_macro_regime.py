import pandas as pd
from data import load_ohlcv_folder
from strategy import prepare_indicators, macro_filter

def main():
    data = load_ohlcv_folder("data/")
    data = prepare_indicators(data)

    btc = data.get("BTC")
    if btc is None:
        raise ValueError("BTC data missing.")

    macro = macro_filter(btc)
    macro = macro.astype(bool)

    total_days = len(macro)
    on_days = macro.sum()
    off_days = total_days - on_days

    print("----- MACRO SUMMARY -----")
    print(f"Total days: {total_days}")
    print(f"Macro ON days: {on_days} ({on_days/total_days:.2%})")
    print(f"Macro OFF days: {off_days} ({off_days/total_days:.2%})")

    # Regime durations
    durations = []
    current_state = macro.iloc[0]
    length = 1

    for val in macro.iloc[1:]:
        if val == current_state:
            length += 1
        else:
            durations.append((current_state, length))
            current_state = val
            length = 1
    durations.append((current_state, length))

    on_durations = [d for state, d in durations if state]
    off_durations = [d for state, d in durations if not state]

    print("\n----- REGIME DURATIONS -----")

    if on_durations:
        print(f"ON avg duration: {sum(on_durations)/len(on_durations):.1f} days")
        print(f"ON max duration: {max(on_durations)} days")

    if off_durations:
        print(f"OFF avg duration: {sum(off_durations)/len(off_durations):.1f} days")
        print(f"OFF max duration: {max(off_durations)} days")

    print("----------------------------")

if __name__ == "__main__":
    main()
