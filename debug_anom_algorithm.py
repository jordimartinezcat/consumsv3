import pandas as pd
import numpy as np

# Cargar el archivo CSV
df = pd.read_csv(r'c:\Projects\Python\Consums_v3\procesado\consumption_minutes_with_anom_20251204_142635.csv', index_col=0)

# Encontrar columnas
cons_col = [c for c in df.columns if c.endswith('_cons')][0]
total_col = cons_col.replace('_cons', '')
raw_col = total_col.replace('_rect_0', '_TOT') if total_col.endswith('_rect_0') else total_col

print(f"Columna de consumo: {cons_col}")
print(f"Columna total: {total_col}")
print(f"Columna raw: {raw_col}")

# Verificar si la columna raw existe
if raw_col in df.columns:
    print(f"✓ Columna raw encontrada: {raw_col}")
    totals_raw = pd.to_numeric(df[raw_col], errors='coerce').fillna(np.nan)
else:
    print(f"✗ Columna raw NO encontrada: {raw_col}")
    print(f"Columnas disponibles: {list(df.columns)}")
    totals_raw = pd.to_numeric(df[total_col], errors='coerce').fillna(np.nan)

cons = pd.to_numeric(df[cons_col], errors='coerce').fillna(0)

# Análisis detallado de los primeros casos negativos
negative_indices = df[df[cons_col] < 0].index[:3]

for idx in negative_indices:
    row_pos = df.index.get_loc(idx)
    print(f"\n=== Análisis detallado para posición {row_pos} ===")
    
    # Contexto ampliado
    context_start = max(0, row_pos - 5)
    context_end = min(len(df), row_pos + 6)
    
    context_cons = cons.iloc[context_start:context_end]
    context_raw = totals_raw.iloc[context_start:context_end]
    
    print("Contexto de consumos:")
    for i, (pos, val) in enumerate(context_cons.items()):
        marker = " <-- NEGATIVO" if pos == idx else ""
        print(f"  Pos {context_start + i}: {val:.1f}{marker}")
    
    print("Contexto de totales RAW:")
    for i, (pos, val) in enumerate(context_raw.items()):
        marker = " <-- NEGATIVO" if pos == idx else ""
        print(f"  Pos {context_start + i}: {val:.1f}{marker}")
    
    # Simular el algoritmo para este caso
    if row_pos < len(df) - 1:
        cur = cons.iloc[row_pos]
        nxt = cons.iloc[row_pos + 1]
        
        print(f"\nAnálisis del algoritmo:")
        print(f"  cur = {cur:.1f}, nxt = {nxt:.1f}")
        
        if cur < 0 and nxt > 0:
            net = cur + nxt
            print(f"  net = {net:.1f}")
            
            if net > 0:
                print(f"  Buscando backward desde posición {row_pos}...")
                
                # Buscar hacia atrás
                j = row_pos
                while j >= 0 and (totals_raw.iloc[j] == 0 or np.isnan(totals_raw.iloc[j])):
                    print(f"    j={j}: raw={totals_raw.iloc[j]:.1f} -> continuando hacia atrás")
                    j -= 1
                
                start = j + 1
                end = row_pos
                count = end - start + 1
                
                print(f"  Resultado: j={j}, start={start}, end={end}, count={count}")
                
                if count > 0:
                    per = net / count
                    print(f"  Distribución: {per:.4f} por minuto en rango {start}:{end+1}")
                else:
                    print(f"  No se aplicará distribución (count={count})")
            else:
                print(f"  net <= 0, no se aplicará distribución")
        else:
            print(f"  No es patrón negativo+positivo")