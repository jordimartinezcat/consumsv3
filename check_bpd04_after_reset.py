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

# Filter for a longer range after the reset - hasta minuto 30 de la HORA SIGUIENTE
mask = (df['timeStamp'] >= '2025-08-12 07:50') & (df['timeStamp'] <= '2025-08-12 09:30')

print("BPD04 behavior after 'reset' at 07:52 (hasta 09:30 - 1h38min después):")
print("="*80)
result_df = df[mask][['timeStamp', 'CL_CAT_BPD04_FTR_G01_TOT_L', 'TOT32']]
print(result_df.to_string(index=False))

print("\n" + "="*80)
print("Analysis:")
tot_values = df[mask]['TOT32'].values
# El reset es en la posición 2 (07:52), verificamos desde posición 3 en adelante
if np.all(tot_values[3:] == 0):
    print("  ⚠️ Totalizador se queda en 0 después del 'reset' durante más de 1h30min")
    print("  → Esto NO es un reset real, es un FALLO DE HARDWARE")
    print("  → El totalizador debería incrementar después de un reset")
else:
    max_after = tot_values[3:].max()
    first_nonzero_idx = np.where(tot_values[3:] > 0)[0]
    if len(first_nonzero_idx) > 0:
        first_nonzero_pos = first_nonzero_idx[0] + 3
        first_nonzero_time = df[mask].iloc[first_nonzero_pos]['timeStamp']
        print(f"  ✓ Totalizador incrementa después del reset")
        print(f"  → Primera lectura >0: {first_nonzero_time} (valor={tot_values[first_nonzero_pos]})")
        print(f"  → Máximo en ventana: {max_after}")
        print("  → Podría ser un reset real del hardware")
    else:
        print("  ⚠️ No se encontraron valores >0 en la ventana")
