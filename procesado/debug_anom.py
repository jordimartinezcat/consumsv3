import pandas as pd
from procesado.compute_consumption import distribute_negative_compensations

fn = 'procesado/consumption_minutes.csv'
df = pd.read_csv(fn, index_col=0)
df['timeStamp'] = pd.to_datetime(df['timeStamp'])
df = df.set_index('timeStamp')

ts_start = pd.to_datetime('2025-06-21 15:36:00')
ts_end = pd.to_datetime('2025-06-21 15:59:00')
window = df.loc[ts_start:ts_end]
print(window[['PBD07_FTR_T01_TOT','PBD07_FTR_T01_TOT_rect_0','PBD07_FTR_T01_TOT_rect_0_cons']])

anom = distribute_negative_compensations(df, total_columns=['PBD07_FTR_T01_TOT_rect_0'])
print('\nAnomaly column (non-null rows):')
print(anom.loc[ts_start:ts_end].dropna(how='all'))
