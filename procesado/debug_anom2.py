import pandas as pd
import numpy as np

fn = 'procesado/consumption_minutes.csv'
df = pd.read_csv(fn, index_col=0)
df['timeStamp'] = pd.to_datetime(df['timeStamp'])
df = df.set_index('timeStamp')

cons = df['PBD07_FTR_T01_TOT_rect_0_cons'].astype(float).reset_index(drop=True)
totals_raw = df['PBD07_FTR_T01_TOT'].astype(float).reset_index(drop=True)

ts_idx = df.index

for i in range(len(cons)-1):
    cur = cons.iat[i]
    nxt = cons.iat[i+1]
    if cur < 0 and nxt > 0:
        net = cur + nxt
        print('Found neg at', ts_idx[i], 'cur', cur, 'nxt', nxt, 'net', net)
        j = i
        while j >= 0 and (totals_raw.iat[j] == 0 or np.isnan(totals_raw.iat[j])):
            j -= 1
        start = j+1
        end = i
        count = end - start + 1
        print('zero run start', ts_idx[start], 'end', ts_idx[end], 'count', count)
        per = net / count if count>0 else None
        print('per-minute', per)

print('done')
