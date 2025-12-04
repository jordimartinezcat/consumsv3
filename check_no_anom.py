import pandas as pd

# Cargar el archivo CSV
df = pd.read_csv(r'c:\Projects\Python\Consums_v3\procesado\consumption_minutes_with_anom_20251204_151252.csv', index_col=0)

cons_col = 'PBD07_FTR_T01_TOT_rect_0_cons'
anom_col = 'PBD07_FTR_T01_TOT_rect_0_anom'

neg_cons = df[df[cons_col] < 0]
no_anom = neg_cons[neg_cons[anom_col].isna()]

print(f"Total consumos negativos: {len(neg_cons)}")
print(f"Sin anomalía: {len(no_anom)}")
print("\nCasos sin anomalía y por qué:")

for idx, row in no_anom.iterrows():
    pos = df.index.get_loc(idx)
    if pos + 1 < len(df):
        next_cons = df.iloc[pos + 1][cons_col]
        net = row[cons_col] + next_cons
        
        print(f"\nPos {pos}: consumo={row[cons_col]:.1f}")
        print(f"  Siguiente: {next_cons:.1f}")
        print(f"  Neto: {net:.1f}")
        
        if next_cons <= 0:
            print("  Razón: El siguiente no es positivo")
        elif net <= 0:
            print("  Razón: El neto no es positivo")
        else:
            print("  Razón: ¿Error en el algoritmo?")
    else:
        print(f"\nPos {pos}: consumo={row[cons_col]:.1f} (último registro)")