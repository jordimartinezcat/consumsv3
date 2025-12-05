import os
import sys
import glob
import pandas as pd

# Add current directory and parent to Python path
current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, current_dir)
sys.path.insert(0, parent_dir)

from compute_consumption import append_minute_consumption, attach_anomalies_to_df


def find_latest_all_minutes(path_root: str):
    pattern = os.path.join(path_root, 'adquisicion', 'minute_data', 'all_minutes*.csv')
    files = glob.glob(pattern)
    if not files:
        # fallback to plain all_minutes.csv
        fallback = os.path.join(path_root, 'adquisicion', 'minute_data', 'all_minutes.csv')
        return fallback if os.path.exists(fallback) else None
    # return latest by modification time
    return max(files, key=os.path.getmtime)


def main():
    root = os.path.dirname(os.path.dirname(__file__))
    src = find_latest_all_minutes(root)
    if src is None:
        print('No combined all_minutes CSV found in adquisicion/minute_data')
        return 2

    print(f'Loading combined CSV: {src}')
    # Try to detect CSV format first
    try:
        df = pd.read_csv(src, sep=';', decimal=',')
        print('Loaded CSV with European format (sep=";", decimal=",")')
    except Exception as e:
        print(f'European format failed: {e}, trying auto-detection...')
        df = pd.read_csv(src, sep=None, engine='python')
        print('Loaded CSV with auto-detected format')
    
    # try to parse index if there is a timestamp column
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
    elif df.index.name in ('timestamp', 'timeStamp'):
        df.index = pd.to_datetime(df.index)
    
    print(f'Loaded DataFrame with columns: {list(df.columns)}')

    result = append_minute_consumption(df)
    
    # Check if anomaly columns already exist
    anom_cols = [c for c in result.columns if c.endswith('_anom')]
    if anom_cols:
        # Check if they have any non-null values
        has_values = any(result[col].notna().sum() > 0 for col in anom_cols)
        if has_values:
            print(f'Found existing anomaly columns with data: {anom_cols}')
        else:
            print(f'Found empty anomaly columns, regenerating: {anom_cols}')
            result = attach_anomalies_to_df(result)
    else:
        print('No anomaly columns found, generating them...')
        result = attach_anomalies_to_df(result)

    out_dir = os.path.join(root, 'procesado', 'Data')
    os.makedirs(out_dir, exist_ok=True)
    from datetime import datetime
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = os.path.join(out_dir, f'consumption_minutes_with_anom_{timestamp}.csv')
    result.to_csv(out_path, index=True)
    print(f'Saved minute consumption to: {out_path}')
    print(f'Final DataFrame columns: {list(result.columns)}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
