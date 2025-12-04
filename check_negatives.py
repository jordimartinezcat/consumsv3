import pandas as pd

# Cargar el archivo CSV
df = pd.read_csv(r'c:\Projects\Python\Consums_v3\procesado\consumption_minutes_with_anom_20251204_142635.csv', index_col=0)

# Encontrar columnas
cons_col = [c for c in df.columns if c.endswith('_cons')][0]
anom_col = [c for c in df.columns if c.endswith('_anom')][0]

print(f"Columna de consumo: {cons_col}")
print(f"Columna de anomalía: {anom_col}")

# Buscar consumos negativos
negative_mask = df[cons_col] < 0
negative_indices = df[negative_mask].index
print(f"\nTotal de consumos negativos: {len(negative_indices)}")

print("\nPrimeros 5 consumos negativos y su contexto:")
for i, idx in enumerate(negative_indices[:5]):
    row_pos = df.index.get_loc(idx)
    context_start = max(0, row_pos-2)
    context_end = min(len(df), row_pos+3)
    context_df = df.iloc[context_start:context_end]
    
    print(f"\n=== Consumo negativo {i+1} en posición {row_pos} (índice {idx}) ===")
    print(context_df[[cons_col, anom_col]].to_string())
    
    # Verificar si hay patrón negativo+positivo
    if row_pos < len(df) - 1:
        current_cons = df.iloc[row_pos][cons_col]
        next_cons = df.iloc[row_pos + 1][cons_col] 
        print(f"Actual: {current_cons:.2f}, Siguiente: {next_cons:.2f}")
        if current_cons < 0 and next_cons > 0:
            print("¡Patrón negativo+positivo detectado!")
        else:
            print("No hay patrón negativo+positivo")

# Verificar si hay algún valor en la columna anom
anom_count = df[anom_col].notna().sum()
print(f"\nTotal de valores no nulos en columna anomalía: {anom_count}")

# Mostrar algunos valores de la columna anom para debug
print(f"\nPrimeros 20 valores de la columna {anom_col}:")
print(df[anom_col].head(20))