import os
import shutil
import pandas as pd
from procesado.compute_consumption import distribute_negative_compensations


def main():
    root = os.path.dirname(os.path.dirname(__file__))
    src = os.path.join(root, 'procesado', 'consumption_minutes.csv')
    if not os.path.exists(src):
        print('consumption_minutes.csv not found in procesado')
        return 2

    backup = src + '.bak'
    shutil.copy2(src, backup)
    print(f'Backup created: {backup}')

    df = pd.read_csv(src, index_col=0)
    anom = distribute_negative_compensations(df)
    # join anomaly columns to df
    for c in anom.columns:
        df[c] = anom[c]

    try:
        df.to_csv(src, index=True)
        print(f'Applied anomaly distribution and updated: {src}')
        out_path = src
    except PermissionError:
        out_path = os.path.join(root, 'procesado', 'consumption_minutes_with_anom.csv')
        df.to_csv(out_path, index=True)
        print(f'Could not overwrite original, saved updated CSV to: {out_path}')
    # show a small summary of non-null anomaly rows
    sample = df[[c for c in df.columns if c.endswith('_anom')]].dropna(how='all')
    print('Rows with anomalies (first 20):')
    if sample.empty:
        print('No anomalies detected')
    else:
        print(sample.head(20))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
