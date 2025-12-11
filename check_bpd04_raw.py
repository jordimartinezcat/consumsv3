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

# Filter for August 12, 2025
mask = (df['timeStamp'] >= '2025-08-12 08:00') & (df['timeStamp'] <= '2025-08-12 10:00')

print("BPD04 data on 2025-08-12 (08:00-10:00):")
print("Looking for negative consumption values...")
print()

# Find big negative jumps
neg_mask = df['CSM'] < -1000
if neg_mask.any():
    print(f"⚠️ Found {neg_mask.sum()} large negative consumption values:")
    print(df[neg_mask][['timeStamp', 'CL_CAT_BPD04_FTR_G01_TOT_H', 'CL_CAT_BPD04_FTR_G01_TOT_L', 'TOT32', 'CSM']].to_string(index=False))
    
    # Show context around the first negative value
    first_neg_idx = df[neg_mask].index[0]
    print("\n" + "="*80)
    print(f"Context around first negative (index {first_neg_idx}):")
    start_idx = max(0, first_neg_idx - 3)
    end_idx = min(len(df), first_neg_idx + 3)
    print(df.iloc[start_idx:end_idx][['timeStamp', 'CL_CAT_BPD04_FTR_G01_TOT_H', 'CL_CAT_BPD04_FTR_G01_TOT_L', 'TOT32', 'CSM']].to_string(index=False))
else:
    print("✓ No large negative consumption found")
    print("\nShowing data around 09:00:")
    print(df[mask][['timeStamp', 'CL_CAT_BPD04_FTR_G01_TOT_H', 'CL_CAT_BPD04_FTR_G01_TOT_L', 'TOT32', 'CSM']].head(20).to_string(index=False))
