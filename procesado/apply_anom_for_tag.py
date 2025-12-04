import os
import pandas as pd
import numpy as np

def apply_for_tag(tag_base='PBD07_FTR_T01_TOT'):
    root = os.path.dirname(os.path.dirname(__file__))
    bak = os.path.join(root, 'procesado', 'consumption_minutes.csv.bak')
    if not os.path.exists(bak):
        raise SystemExit('Backup CSV not found: ' + bak)

    df = pd.read_csv(bak, index_col=0)
    # working copies
    totals_raw = df[tag_base].astype(float).reset_index(drop=True)
    cons_col = tag_base + '_rect_0_cons'
    if cons_col not in df.columns:
        raise SystemExit('Consumption column not found: ' + cons_col)
    cons = df[cons_col].astype(float).reset_index(drop=True)

    n = len(df)
    anom = np.full(n, np.nan)

    i = 0
    while i < n - 1:
        cur = cons.iat[i]
        nxt = cons.iat[i + 1]
        if cur < 0 and nxt > 0:
            net = cur + nxt
            if net > 0:
                # find zero-run in totals_raw ending at i
                j = i
                while j >= 0 and (totals_raw.iat[j] == 0 or np.isnan(totals_raw.iat[j])):
                    j -= 1
                start = j + 1
                end = i
                count = end - start + 1
                if count > 0:
                    per = net / count
                    anom[start:end+1] = np.nan_to_num(anom[start:end+1], nan=0.0) + per
            i += 2
        else:
            i += 1

    anom_series = pd.Series(anom, index=df.index)
    # replace zeros with NaN for cleanliness, keep actual values
    anom_series = anom_series.where(~(anom_series == 0), other=pd.NA)
    colname = tag_base + '_anom'
    df[colname] = anom_series

    out = os.path.join(root, 'procesado', f'consumption_minutes_with_{tag_base}_anom.csv')
    df.to_csv(out, index=True)
    print('Saved with anomaly column:', out)
    # print affected rows
    df['timeStamp'] = pd.to_datetime(df['timeStamp'])
    df2 = df.set_index('timeStamp')
    start = '2025-06-21 15:36:00'
    end = '2025-06-21 15:59:00'
    print(df2.loc[start:end, [tag_base, tag_base + '_rect_0_cons', colname]])


if __name__ == '__main__':
    apply_for_tag()
