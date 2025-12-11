import glob
import os
import sys

import pandas as pd

# Add current directory and parent to Python path
current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, current_dir)
sys.path.insert(0, parent_dir)

from compute_consumption import (
    append_minute_consumption,
    attach_anomalies_to_df,
    detect_counter_resets,
    detect_phantom_totalizer_jumps,
)


def find_latest_all_minutes(path_root: str):
    pattern = os.path.join(path_root, "adquisicion", "minute_data", "all_minutes*.csv")
    files = glob.glob(pattern)
    if not files:
        # fallback to plain all_minutes.csv
        fallback = os.path.join(
            path_root, "adquisicion", "minute_data", "all_minutes.csv"
        )
        return fallback if os.path.exists(fallback) else None
    # return latest by modification time
    return max(files, key=os.path.getmtime)


def main():
    root = os.path.dirname(os.path.dirname(__file__))
    src = find_latest_all_minutes(root)
    if src is None:
        print("No combined all_minutes CSV found in adquisicion/minute_data")
        return 2

    print(f"Loading combined CSV: {src}")
    # Try to detect CSV format first
    try:
        df = pd.read_csv(src, sep=";", decimal=",", index_col=0, parse_dates=True)
        print('Loaded CSV with European format (sep=";", decimal=",")')
    except Exception as e:
        print(f"European format failed: {e}, trying auto-detection...")
        df = pd.read_csv(src, sep=None, engine="python", index_col=0, parse_dates=True)
        print("Loaded CSV with auto-detected format")

    print(f"Loaded DataFrame with columns: {list(df.columns)}")

    # Check if consumption columns already exist (processed by run_compute_for_minutes.py)
    cons_cols = [c for c in df.columns if c.endswith("_cons")]
    anom_cols = [c for c in df.columns if c.endswith("_anom")]

    if cons_cols:
        print(
            f"Found existing consumption columns: {cons_cols}, skipping recalculation"
        )
        result = df.copy()
    else:
        print("Calculating consumption columns...")
        result = append_minute_consumption(df)

    # Step 2: Detect regular anomalies (negative compensations)
    if anom_cols:
        has_values = any(result[col].notna().sum() > 0 for col in anom_cols)
        if has_values:
            print(f"Found existing anomaly columns with data: {anom_cols}")
        else:
            print(f"Generating regular anomalies: {anom_cols}")
            result = attach_anomalies_to_df(result)
    else:
        print("Generating anomaly columns...")
        result = attach_anomalies_to_df(result)
        anom_cols = [c for c in result.columns if c.endswith("_anom")]

    # Step 3: Detect and mark counter resets (runs before phantom jump handling)
    print("\n--- Detecting counter resets ---")
    result = detect_counter_resets(result)

    # Step 4: Detect phantom totalizer jumps (last anomaly pass)
    print("\n--- Detecting phantom totalizer jumps ---")
    result = detect_phantom_totalizer_jumps(result, threshold=1000000)

    # Count final anomalies
    total_anomalies = sum(result[col].notna().sum() for col in anom_cols)
    print(f"Total anomalies after reset detection: {total_anomalies}")

    out_dir = os.path.join(root, "procesado", "Data")
    os.makedirs(out_dir, exist_ok=True)
    from datetime import datetime

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(out_dir, f"consumption_minutes_with_anom_{timestamp}.csv")
    result.to_csv(out_path, index=True, sep=";", decimal=",")
    print(f"Saved minute consumption to: {out_path}")
    print(f"Final DataFrame columns: {list(result.columns)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
