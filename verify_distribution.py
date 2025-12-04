import pandas as pd
import numpy as np

# Cargar el archivo CSV
df = pd.read_csv(r'c:\Projects\Python\Consums_v3\procesado\consumption_minutes_with_anom_20251204_150004.csv', index_col=0)

# Encontrar columnas
cons_col = [c for c in df.columns if c.endswith('_cons')][0]
total_col = cons_col.replace('_cons', '')
# La columna raw debería ser la que termina en _TOT sin _rect_0
raw_col = None
for col in df.columns:
    if col.endswith('_TOT') and not col.endswith('_rect_0') and not col.endswith('_cons'):
        raw_col = col
        break

anom_col = [c for c in df.columns if c.endswith('_anom')][0]

print(f"Columna de consumo: {cons_col}")
print(f"Columna total rectificada: {total_col}")
print(f"Columna raw: {raw_col}")
print(f"Columna de anomalía: {anom_col}")

if raw_col not in df.columns:
    print(f"ERROR: Columna {raw_col} no encontrada")
    exit(1)

totals_raw = pd.to_numeric(df[raw_col], errors='coerce').fillna(np.nan)
cons = pd.to_numeric(df[cons_col], errors='coerce').fillna(0)

# Encontrar casos con anomalías
anom_indices = df[df[anom_col].notna()].index

print(f"\nAnálisis de distribución de anomalías:")
print(f"Total casos con anomalía: {len(anom_indices)}")

# Analizar los primeros 3 casos
for idx in anom_indices[:3]:
    row_pos = df.index.get_loc(idx)
    print(f"\n=== Caso con anomalía en posición {row_pos} (índice {idx}) ===")
    
    # Contexto ampliado
    context_start = max(0, row_pos - 10)
    context_end = min(len(df), row_pos + 3)
    
    context_df = df.iloc[context_start:context_end]
    
    print("Contexto (últimos 10 minutos + 2 siguientes):")
    print(context_df[[raw_col, cons_col, anom_col]].to_string())
    
    # Verificar cuántos minutos consecutivos hacia atrás tienen total_raw = 0
    zero_count = 0
    j = row_pos - 1
    while j >= 0 and totals_raw.iloc[j] == 0:
        zero_count += 1
        j -= 1
    
    print(f"\nMinutos consecutivos hacia atrás con {raw_col} = 0: {zero_count}")
    
    if row_pos < len(df) - 1:
        cur_cons = cons.iloc[row_pos]
        next_cons = cons.iloc[row_pos + 1]
        net = cur_cons + next_cons
        print(f"Consumo actual: {cur_cons}, siguiente: {next_cons}, neto: {net}")
        
        if zero_count > 0:
            per_minute = net / zero_count
            print(f"Distribución teórica: {net} / {zero_count} = {per_minute:.4f} por minuto")
        else:
            print("No hay minutos con total=0 para distribuir → se aplica todo al minuto negativo")

# Buscar si hay algún minuto con total_raw = 0 en todo el dataset
zero_totals = (totals_raw == 0).sum()
print(f"\nTotal de minutos con {raw_col} = 0 en todo el dataset: {zero_totals}")

if zero_totals > 0:
    zero_positions = df[totals_raw == 0].index[:5]
    print(f"Primeras 5 posiciones con {raw_col} = 0:")
    for pos in zero_positions:
        print(f"  Posición {df.index.get_loc(pos)}: {raw_col} = {totals_raw.loc[pos]}")