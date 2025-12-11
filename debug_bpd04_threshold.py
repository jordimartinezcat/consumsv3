import pandas as pd

# Read TOT_L and TOT_H
df_l = pd.read_csv('adquisicion/minute_data/CL_CAT_BPD04_FTR_G01_TOT_L.csv')
df_h = pd.read_csv('adquisicion/minute_data/CL_CAT_BPD04_FTR_G01_TOT_H.csv')

# Convert timestamps
df_l['timeStamp'] = pd.to_datetime(df_l['timeStamp'])
df_h['timeStamp'] = pd.to_datetime(df_h['timeStamp'])

# Merge
df = df_l.merge(df_h, on='timeStamp', how='inner')

# Calculate 32-bit TOT
df['TOT32'] = df.apply(
    lambda row: int((int(row['CL_CAT_BPD04_FTR_G01_TOT_H']) << 16) | (int(row['CL_CAT_BPD04_FTR_G01_TOT_L']) & 0xFFFF))
    if pd.notna(row['CL_CAT_BPD04_FTR_G01_TOT_H']) and pd.notna(row['CL_CAT_BPD04_FTR_G01_TOT_L'])
    else 0,
    axis=1
)

# Calculate diff
df['diff'] = df['TOT32'].diff()

# Check the jump at 07:52
mask = (df['timeStamp'] >= '2025-08-12 07:50') & (df['timeStamp'] <= '2025-08-12 07:55')
print("Diferencias (diff) alrededor de 07:52:")
print(df[mask][['timeStamp', 'TOT32', 'diff']].to_string(index=False))

print("\n" + "="*80)
max_reasonable = 100000
print(f"Threshold max_reasonable_consumption = {max_reasonable}")
print(f"¿Salto -26284 < -{max_reasonable}? {-26284 < -max_reasonable}")
print(f"  → {-26284} NO es menor que {-max_reasonable}, por eso NO se detecta")
print("\nSolución: Reducir threshold o detectar CUALQUIER caída a 0 desde valor >1000")
