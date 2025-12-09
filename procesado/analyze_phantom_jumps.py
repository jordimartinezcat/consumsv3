"""
Analizar saltos "fantasma" en el totalizador: valores anómalos rodeados por
valores constantes del acumulador con consumo = 0.

Patrón buscado:
- El totalizador tiene el mismo valor 10 minutos antes y 10 minutos después
- En el medio hay uno o más minutos con valor diferente del totalizador
- El consumo calculado es 0 en esos minutos
"""

import os
import pandas as pd
import numpy as np


def analyze_phantom_jumps(csv_path):
    """
    Analiza saltos fantasma en datos minutales.
    
    Returns:
        pd.DataFrame con casos encontrados
    """
    print(f"Cargando {csv_path}...")
    
    # Intentar diferentes formatos
    try:
        df = pd.read_csv(csv_path)
        print("CSV cargado con formato estándar")
    except:
        try:
            df = pd.read_csv(csv_path, sep=';', decimal=',')
            print("CSV cargado con formato europeo")
        except:
            df = pd.read_csv(csv_path, sep=None, engine='python')
            print("CSV cargado con autodetección")
    
    print(f"Shape: {df.shape}")
    print(f"Columnas: {list(df.columns)}")
    
    # Convertir timestamp
    if 'timeStamp' in df.columns:
        df['timeStamp'] = pd.to_datetime(df['timeStamp'])
    
    # Identificar columnas de totalizador y consumo
    tot_cols = [c for c in df.columns if '_TOT' in c and not c.endswith('_cons') and not c.endswith('_anom')]
    cons_cols = [c for c in df.columns if c.endswith('_cons')]
    
    print(f"\nColumnas totalizador: {tot_cols}")
    print(f"Columnas consumo: {cons_cols}")
    
    # Usar la columna rect_0 (totalizador corregido) y su consumo
    tot_col = [c for c in tot_cols if 'rect_0' in c][0]
    cons_col = [c for c in cons_cols if 'rect_0' in c][0]
    
    print(f"\nAnalizando: {tot_col} y {cons_col}")
    
    # Crear ventanas de -10 y +10 minutos
    df['tot_prev_10'] = df[tot_col].shift(10)
    df['tot_next_10'] = df[tot_col].shift(-10)
    df['cons_is_zero'] = df[cons_col] == 0.0
    
    # Detectar filas donde:
    # 1. El totalizador 10 min antes == 10 min después
    # 2. El totalizador actual es diferente
    # 3. El consumo es 0
    phantom_mask = (
        (df['tot_prev_10'] == df['tot_next_10']) &  # Totalizador igual antes y después
        (df[tot_col] != df['tot_prev_10']) &         # Valor actual diferente
        (df['cons_is_zero']) &                        # Consumo = 0
        (df['tot_prev_10'].notna()) &                 # Valores válidos
        (df['tot_next_10'].notna())
    )
    
    phantom_cases = df[phantom_mask].copy()
    
    print(f"\n{'='*80}")
    print(f"RESULTADOS DEL ANÁLISIS")
    print(f"{'='*80}")
    print(f"Total de minutos analizados: {len(df):,}")
    print(f"Casos de saltos fantasma encontrados: {len(phantom_cases):,}")
    print(f"Porcentaje: {100 * len(phantom_cases) / len(df):.4f}%")
    
    if len(phantom_cases) > 0:
        print(f"\n{'='*80}")
        print(f"DETALLE DE CASOS ENCONTRADOS")
        print(f"{'='*80}")
        
        # Mostrar primeros 20 casos
        display_cols = ['timeStamp', tot_col, 'tot_prev_10', 'tot_next_10', cons_col]
        print("\nPrimeros 20 casos:")
        print(phantom_cases[display_cols].head(20).to_string())
        
        # Estadísticas de saltos
        phantom_cases['jump_size'] = abs(phantom_cases[tot_col] - phantom_cases['tot_prev_10'])
        
        print(f"\n{'='*80}")
        print(f"ESTADÍSTICAS DE SALTOS")
        print(f"{'='*80}")
        print(f"Salto mínimo: {phantom_cases['jump_size'].min():,.0f}")
        print(f"Salto máximo: {phantom_cases['jump_size'].max():,.0f}")
        print(f"Salto promedio: {phantom_cases['jump_size'].mean():,.0f}")
        print(f"Salto mediano: {phantom_cases['jump_size'].median():,.0f}")
        
        # Analizar el caso específico reportado
        case_2025_06_02 = phantom_cases[
            (phantom_cases['timeStamp'] >= '2025-06-02 08:50:00') & 
            (phantom_cases['timeStamp'] <= '2025-06-02 09:10:00')
        ]
        
        if len(case_2025_06_02) > 0:
            print(f"\n{'='*80}")
            print(f"CASO ESPECÍFICO: 2025-06-02 09:00")
            print(f"{'='*80}")
            print(case_2025_06_02[display_cols].to_string())
            print(f"\nSalto en este caso: {case_2025_06_02['jump_size'].iloc[0]:,.0f}")
        
        # Agrupar por fechas para ver distribución temporal
        phantom_cases['date'] = phantom_cases['timeStamp'].dt.date
        cases_by_date = phantom_cases.groupby('date').size().sort_values(ascending=False)
        
        print(f"\n{'='*80}")
        print(f"DISTRIBUCIÓN TEMPORAL (Top 20 días)")
        print(f"{'='*80}")
        print(cases_by_date.head(20).to_string())
        
        # Guardar casos completos
        output_file = os.path.join(
            os.path.dirname(csv_path),
            'phantom_jumps_analysis.csv'
        )
        phantom_cases[display_cols + ['jump_size', 'date']].to_csv(
            output_file, 
            sep=';', 
            decimal=',',
            index=False
        )
        print(f"\nCasos completos guardados en: {output_file}")
    
    else:
        print("\nNo se encontraron casos de saltos fantasma.")
    
    return phantom_cases


if __name__ == '__main__':
    # Buscar el archivo de minutos más reciente
    data_dir = os.path.join(
        os.path.dirname(__file__),
        '..',
        'procesado',
        'Data'
    )
    
    # Usar el archivo base (sin anom) para análisis
    csv_file = os.path.join(data_dir, 'consumption_minutes.csv')
    
    if not os.path.exists(csv_file):
        print(f"ERROR: No se encuentra {csv_file}")
        import glob
        pattern = os.path.join(data_dir, 'consumption_minutes*.csv')
        files = glob.glob(pattern)
        if files:
            csv_file = sorted(files)[-1]
            print(f"Usando archivo alternativo: {csv_file}")
        else:
            raise FileNotFoundError(f"No hay archivos en {data_dir}")
    
    phantom_cases = analyze_phantom_jumps(csv_file)
    
    print(f"\n{'='*80}")
    print("ANÁLISIS COMPLETADO")
    print(f"{'='*80}")
