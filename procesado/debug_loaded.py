import pandas as pd
import numpy as np

fn = r'adquisicion/minute_data/all_minutes_20251204_133752.csv'
df = pd.read_csv(fn, sep=';', decimal=',')
cons = df['PBD07_FTR_T01_TOT_rect_0_cons'].astype(float).reset_index(drop=True)
totals_raw = df['PBD07_FTR_T01_TOT'].astype(float).reset_index(drop=True)
idx = pd.to_datetime(df['timeStamp'])

for i in range(len(cons)-1):
    cur = cons.iat[i]
    nxt = cons.iat[i+1]
    if cur < 0 and nxt > 0:
        net = cur + nxt
        print('Found neg at', idx.iat[i], 'cur', cur, 'nxt', nxt, 'net', net)
        j = i
        while j >= 0 and (totals_raw.iat[j] == 0 or np.isnan(totals_raw.iat[j])):
            j -= 1
        start = j+1
        end = i
        count = end - start + 1
        print('zero run start', idx.iat[start], 'end', idx.iat[end], 'count', count)
        if count>0:
            print('per-minute', net/count)

print('done')
