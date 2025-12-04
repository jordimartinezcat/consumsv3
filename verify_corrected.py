import pandas as pd
import numpy as np

# Cargar el nuevo archivo CSV
df = pd.read_csv(r'c:\Projects\Python\Consums_v3\procesado\consumption_minutes_with_anom_20251204_150456.csv', index_col=0)

# Encontrar columnas
cons_col = [c for c in df.columns if c.endswith('_cons')][0]
raw_col = 'PBD07_FTR_T01_TOT'  # Columna de totalizador RAW
anom_col = [c for c in df.columns if c.endswith('_anom')][0]

print(f"Total de anomalías no nulas: {df[anom_col].notna().sum()}")

# Buscar consumos negativos para verificar que se mantienen
negative_cons = df[df[cons_col] < 0]
print(f"Total de consumos negativos conservados: {len(negative_cons)}")

if len(negative_cons) > 0:
    print("\nPrimeros 5 consumos negativos:")
    print(negative_cons[[cons_col, anom_col]].head())

# Buscar casos específicos donde debería haber distribución
# Busquemos el rango 28780-28783 que vimos en la captura
if 28780 in df.index:
    print(f"\n=== Análisis del rango 28780-28790 ===")
    context = df.iloc[28770:28790]
    print(context[[raw_col, cons_col, anom_col]].to_string())

# Verificar si hay distribuciones en zonas con totalizador = 0
anom_data = df[df[anom_col].notna()]
print(f"\nCasos con anomalías detectadas: {len(anom_data)}")

if len(anom_data) > 0:
    print("Valores de anomalía distribuidos:")
    for idx, row in anom_data.iterrows():
        pos = df.index.get_loc(idx)
        print(f"  Posición {pos}: anomalía = {row[anom_col]:.4f}")

print(f"\nVerificación: minutos con totalizador = 0: {(df[raw_col] == 0).sum()}")