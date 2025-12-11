"""
Test que la corrección de anomalías funciona correctamente para BPD04
"""
import sys
import os
import pandas as pd
import logging

# Setup
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "CAT_Conexions", "src"))

logging.basicConfig(level=logging.INFO, format='%(message)s')

# Read raw data
df_l = pd.read_csv('adquisicion/minute_data/CL_CAT_BPD04_FTR_G01_TOT_L.csv')
df_h = pd.read_csv('adquisicion/minute_data/CL_CAT_BPD04_FTR_G01_TOT_H.csv')

df_l['timeStamp'] = pd.to_datetime(df_l['timeStamp'])
df_h['timeStamp'] = pd.to_datetime(df_h['timeStamp'])

# Merge
df = df_l.merge(df_h, on='timeStamp', how='inner')
df = df.sort_values('timeStamp').reset_index(drop=True)

print("="*100)
print("PRUEBA: Detección de anomalías en BPD04_FTR_G01")
print("="*100)

# Apply the anomaly detection function from run_compute_for_minutes
def combine_tot_high_low(df, near_zero_threshold=1000, max_reasonable_consumption=100000):
    combined = df.copy()
    processed = set()
    
    for col in combined.columns:
        if col in processed:
            continue
        if col.endswith("_TOT_H"):
            base = col[:-6]
            low_col = base + "_TOT_L"
            tot_col = base + "_TOT"
            anomaly_col = base + "_TOT_is_anomalous_jump"
            
            if low_col in combined.columns:
                import numpy as np
                
                high_s = pd.to_numeric(combined[col], errors="coerce").fillna(0)
                low_s = pd.to_numeric(combined[low_col], errors="coerce").fillna(0)
                high_arr = high_s.to_numpy(dtype="int64")
                low_arr = low_s.to_numpy(dtype="int64")
                result_arr = (high_arr << 16) | (low_arr & 0xFFFF)
                combined[tot_col] = pd.Series(result_arr, index=combined.index)
                
                tot_diff = combined[tot_col].diff()
                is_anomalous_jump = pd.Series(False, index=combined.index)
                
                possible_resets = tot_diff < -max_reasonable_consumption
                reset_indices = combined[possible_resets].index
                
                anomaly_count = 0
                reset_count = 0
                
                for idx in reset_indices:
                    pos = combined.index.get_loc(idx)
                    if pos == 0:
                        continue
                    
                    tot_before = combined[tot_col].iloc[pos - 1]
                    tot_after = combined[tot_col].iloc[pos]
                    
                    if abs(tot_after) <= near_zero_threshold:
                        window_size = min(90, len(combined) - pos - 1)
                        incrementa_de_forma_estable = False
                        
                        if window_size > 10:
                            window_values = combined[tot_col].iloc[pos:pos+window_size+1].values
                            
                            first_nonzero_idx = None
                            for i, val in enumerate(window_values[1:], start=1):
                                if val > 100:
                                    first_nonzero_idx = i
                                    break
                            
                            if first_nonzero_idx is not None:
                                if first_nonzero_idx <= 60:
                                    valores_despues = window_values[first_nonzero_idx:first_nonzero_idx+5]
                                    if len(valores_despues) >= 5 and all(v > 0 for v in valores_despues):
                                        incrementa_de_forma_estable = True
                        
                        if incrementa_de_forma_estable:
                            reset_count += 1
                            print(f"  ✓ Reset real en {combined['timeStamp'].iloc[pos]}: TOT incrementa de forma estable después")
                        else:
                            is_anomalous_jump.iloc[pos] = True
                            anomaly_count += 1
                            print(f"  ⚠️ ANOMALÍA en {combined['timeStamp'].iloc[pos]}: TOT cae de {tot_before:,.0f} a {tot_after:,.0f} pero NO incrementa de forma estable")
                    else:
                        is_anomalous_jump.iloc[pos] = True
                        anomaly_count += 1
                        print(f"  ⚠️ ANOMALÍA en {combined['timeStamp'].iloc[pos]}: TOT después = {tot_after:,.0f} (no cerca de 0)")
                
                combined[anomaly_col] = is_anomalous_jump
                
                print(f"\n{base}: {reset_count} resets reales, {anomaly_count} anomalías detectadas")
                
                processed.add(col)
                processed.add(low_col)
    
    return combined

# Apply detection
df_result = combine_tot_high_low(df)

# Check around August 12 07:52
print("\n" + "="*100)
print("Resultado alrededor de 2025-08-12 07:52:")
print("="*100)
mask = (df_result['timeStamp'] >= '2025-08-12 07:50') & (df_result['timeStamp'] <= '2025-08-12 08:00')
print(df_result[mask][['timeStamp', 'CL_CAT_BPD04_FTR_G01_TOT', 'CL_CAT_BPD04_FTR_G01_TOT_is_anomalous_jump']].to_string(index=False))

# Verify hourly consumption would be 0
print("\n" + "="*100)
print("Verificación: consumo horario en 08:00-09:00 debería ser 0")
print("="*100)

df_result['CSM'] = df_result['CL_CAT_BPD04_FTR_G01_TOT'].diff()
df_result['CSM'] = df_result.apply(
    lambda row: 0 if row['CL_CAT_BPD04_FTR_G01_TOT_is_anomalous_jump'] else row['CSM'],
    axis=1
)

mask_hour = (df_result['timeStamp'] >= '2025-08-12 08:00') & (df_result['timeStamp'] < '2025-08-12 09:00')
hourly_sum = df_result[mask_hour]['CSM'].sum()

print(f"Suma horaria 08:00-09:00: {hourly_sum:,.2f} litros")
if hourly_sum == 0:
    print("✅ CORRECTO: El consumo es 0 (anomalía detectada)")
else:
    print(f"❌ ERROR: El consumo no es 0, es {hourly_sum:,.2f}")
