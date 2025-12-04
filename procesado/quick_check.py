import pandas as pd
df = pd.read_csv('adquisicion/minute_data/all_minutes_20251204_135636.csv', sep=';', decimal=',')
print('Columns:', df.columns.tolist())
print('Shape:', df.shape)
cols_anom = [c for c in df.columns if 'anom' in c]
print('Anom cols:', cols_anom)
for c in cols_anom:
    print(c, 'non-null:', df[c].notna().sum())
print('\nSlice 15:36-15:59 anom values:')
print(df.loc[247236:247258, ['timeStamp', 'PBD07_FTR_T01_TOT_rect_0_anom']])
