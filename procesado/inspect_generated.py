import pandas as pd
from procesado.compute_consumption import distribute_negative_compensations

fn = r'adquisicion/minute_data/all_minutes_20251204_133752.csv'
print('Loading', fn)
df = pd.read_csv(fn, sep=';', decimal=',')
df['timeStamp'] = pd.to_datetime(df['timeStamp'])
df = df.set_index('timeStamp')

col_anom = 'PBD07_FTR_T01_TOT_rect_0_anom'
start = '2025-06-21 15:36:00'
end = '2025-06-21 15:59:00'
print('\nSlice from file:')
print(df.loc[start:end, ['PBD07_FTR_T01_TOT', 'PBD07_FTR_T01_TOT_rect_0', 'PBD07_FTR_T01_TOT_rect_0_cons', col_anom]])

print('\nNon-null anomaly rows in file:')
print(df[df[col_anom].notna()].head(20))

print('\nNow recomputing anomalies from loaded DF:')
anom = distribute_negative_compensations(df)
print(anom.loc[start:end].dropna(how='all'))
