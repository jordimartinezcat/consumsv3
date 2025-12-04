import os
import pandas as pd
from procesado.compute_consumption import distribute_negative_compensations


def main():
    root = os.path.dirname(os.path.dirname(__file__))
    bak = os.path.join(root, 'procesado', 'consumption_minutes.csv.bak')
    if not os.path.exists(bak):
        print('Backup CSV not found:', bak)
        return 2

    df = pd.read_csv(bak, index_col=0)
    anom = distribute_negative_compensations(df)
    for c in anom.columns:
        df[c] = anom[c]

    out = os.path.join(root, 'procesado', 'consumption_minutes_with_anom_fixed.csv')
    df.to_csv(out, index=True)
    print('Saved fixed anomalies CSV to:', out)
    # show sample around example time
    df['timeStamp'] = pd.to_datetime(df['timeStamp'])
    df = df.set_index('timeStamp')
    start = '2025-06-21 15:36:00'
    end = '2025-06-21 15:59:00'
    print(df.loc[start:end, [col for col in df.columns if col.endswith('_anom') or col.endswith('_cons') or col.endswith('_TOT')]])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
