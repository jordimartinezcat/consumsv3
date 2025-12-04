import pandas as pd
df = pd.read_csv('adquisicion/minute_data/all_minutes.csv')
print('Columns:', df.columns.tolist())
print('Shape:', df.shape)
cols_anom = [c for c in df.columns if 'anom' in c]
print('Anom cols:', cols_anom)
for c in cols_anom:
    print(c, 'non-null:', df[c].notna().sum())
