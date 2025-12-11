import pandas as pd
import numpy as np

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
    else np.nan,
    axis=1
)

# Calculate consumption
df['CSM'] = df['TOT32'].diff()

# Find the negative jump at 2025-08-12 07:52
neg_timestamp = pd.to_datetime('2025-08-12 07:52:00')
neg_idx = df[df['timeStamp'] == neg_timestamp].index[0]

print("Context around 2025-08-12 07:52 (-26284 jump):")
print("="*100)
start_idx = max(0, neg_idx - 10)
end_idx = min(len(df), neg_idx + 10)
print(df.iloc[start_idx:end_idx][['timeStamp', 'CL_CAT_BPD04_FTR_G01_TOT_H', 'CL_CAT_BPD04_FTR_G01_TOT_L', 'TOT32', 'CSM']].to_string(index=False))

# Check if this is a valid counter reset
print("\n" + "="*100)
print("Reset Analysis:")
tot_before = df.loc[neg_idx - 1, 'TOT32']
tot_after = df.loc[neg_idx, 'TOT32']
print(f"  TOT32 before jump: {tot_before}")
print(f"  TOT32 after jump:  {tot_after}")
print(f"  Difference:        {tot_after - tot_before}")
print(f"  Is after near zero (<1000)? {tot_after <= 1000}")
print(f"\n  ⚠️ This should be detected as ANOMALOUS JUMP (TOT goes to 0 from 26284)")
