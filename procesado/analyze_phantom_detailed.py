"""
Análisis detallado del caso específico 2025-06-02 09:00
y búsqueda de patrones de saltos fantasma con ventanas variables.
"""

import os
import pandas as pd
import numpy as np


def inspect_specific_case(df, timestamp_str, tot_col, cons_col, window_minutes=20):
    """
    Inspecciona un caso específico con contexto antes y después.
    """
    target_time = pd.to_datetime(timestamp_str)
    
    # Ventana de ±window_minutes alrededor del timestamp
    mask = (
        (df['timeStamp'] >= target_time - pd.Timedelta(minutes=window_minutes)) &
        (df['timeStamp'] <= target_time + pd.Timedelta(minutes=window_minutes))
    )
    
    window_data = df[mask].copy()
    
    print(f"\n{'='*80}")
    print(f"INSPECCIÓN DETALLADA: {timestamp_str}")
    print(f"Ventana: ±{window_minutes} minutos")
    print(f"{'='*80}")
    
    if len(window_data) > 0:
        display_cols = ['timeStamp', tot_col, cons_col]
        print(window_data[display_cols].to_string(index=False))
        
        # Calcular diferencias
        window_data['tot_diff'] = window_data[tot_col].diff()
        
        print(f"\n{'='*80}")
        print("ANÁLISIS DE CAMBIOS")
        print(f"{'='*80}")
        
        changes = window_data[window_data['tot_diff'] != 0]
        if len(changes) > 0:
            print("\nMomentos donde el totalizador cambia:")
            print(changes[['timeStamp', tot_col, 'tot_diff', cons_col]].to_string(index=False))
        else:
            print("\nEl totalizador NO cambia en toda la ventana.")
        
        # Verificar si hay un salto aislado
        unique_tots = window_data[tot_col].unique()
        print(f"\nValores únicos del totalizador: {unique_tots}")
        
        if len(unique_tots) == 2:
            counts = window_data[tot_col].value_counts()
            print(f"\nDistribución de valores:")
            print(counts.to_string())
            
            if counts.min() <= 5:  # Si hay un valor que aparece pocas veces
                print("\n⚠️  POSIBLE SALTO FANTASMA DETECTADO:")
                print(f"   Valor dominante: {counts.idxmax()} (aparece {counts.max()} veces)")
                print(f"   Valor anómalo: {counts.idxmin()} (aparece {counts.min()} veces)")
    else:
        print(f"No hay datos en la ventana alrededor de {timestamp_str}")
    
    return window_data


def analyze_flexible_phantom_jumps(df, tot_col, cons_col):
    """
    Busca saltos fantasma con criterios más flexibles:
    - Ventanas de 5, 10, 15 minutos
    - Totalizador igual antes/después pero diferente en el medio
    - Consumo = 0
    """
    results = {}
    
    for window in [5, 10, 15, 20]:
        print(f"\n{'='*80}")
        print(f"ANÁLISIS CON VENTANA DE {window} MINUTOS")
        print(f"{'='*80}")
        
        df[f'tot_prev_{window}'] = df[tot_col].shift(window)
        df[f'tot_next_{window}'] = df[tot_col].shift(-window)
        
        phantom_mask = (
            (df[f'tot_prev_{window}'] == df[f'tot_next_{window}']) &
            (df[tot_col] != df[f'tot_prev_{window}']) &
            (df[cons_col] == 0.0) &
            (df[f'tot_prev_{window}'].notna()) &
            (df[f'tot_next_{window}'].notna())
        )
        
        phantom_cases = df[phantom_mask].copy()
        
        print(f"Casos encontrados: {len(phantom_cases):,}")
        
        if len(phantom_cases) > 0:
            phantom_cases['jump_size'] = abs(phantom_cases[tot_col] - phantom_cases[f'tot_prev_{window}'])
            print(f"Salto promedio: {phantom_cases['jump_size'].mean():,.0f}")
            print(f"Salto máximo: {phantom_cases['jump_size'].max():,.0f}")
            
            # Mostrar primeros casos
            display_cols = ['timeStamp', tot_col, f'tot_prev_{window}', f'tot_next_{window}', cons_col]
            print("\nPrimeros 10 casos:")
            print(phantom_cases[display_cols].head(10).to_string(index=False))
            
            results[window] = phantom_cases
    
    return results


def analyze_zero_consumption_jumps(df, tot_col, cons_col):
    """
    Analiza todos los cambios del totalizador cuando el consumo es 0.
    """
    print(f"\n{'='*80}")
    print("ANÁLISIS DE CAMBIOS DEL TOTALIZADOR CON CONSUMO = 0")
    print(f"{'='*80}")
    
    # Calcular cambio del totalizador
    df['tot_change'] = df[tot_col].diff()
    
    # Casos donde totalizador cambia pero consumo es 0
    anomalies = df[(df['tot_change'] != 0) & (df[cons_col] == 0.0)].copy()
    
    print(f"Total cambios del totalizador con consumo=0: {len(anomalies):,}")
    
    if len(anomalies) > 0:
        print(f"\nEstadísticas de cambios:")
        print(f"Cambio mínimo: {anomalies['tot_change'].min():,.0f}")
        print(f"Cambio máximo: {anomalies['tot_change'].max():,.0f}")
        print(f"Cambio promedio: {anomalies['tot_change'].mean():,.0f}")
        
        # Top 20 mayores cambios
        top_changes = anomalies.nlargest(20, 'tot_change')
        print(f"\nTop 20 mayores cambios del totalizador con consumo=0:")
        print(top_changes[['timeStamp', tot_col, 'tot_change', cons_col]].to_string(index=False))
    
    return anomalies


if __name__ == '__main__':
    data_dir = os.path.join(
        os.path.dirname(__file__),
        'Data'
    )
    
    csv_file = os.path.join(data_dir, 'consumption_minutes.csv')
    
    print(f"Cargando {csv_file}...")
    df = pd.read_csv(csv_file)
    df['timeStamp'] = pd.to_datetime(df['timeStamp'])
    
    tot_col = 'PBD07_FTR_T01_TOT_rect_0'
    cons_col = 'PBD07_FTR_T01_TOT_rect_0_cons'
    
    # 1. Inspeccionar caso específico 2025-06-02 09:00
    window_data = inspect_specific_case(
        df,
        '2025-06-02 09:00:00',
        tot_col,
        cons_col,
        window_minutes=30
    )
    
    # 2. Análisis de cambios con consumo=0
    anomalies = analyze_zero_consumption_jumps(df, tot_col, cons_col)
    
    # 3. Análisis con ventanas flexibles
    flexible_results = analyze_flexible_phantom_jumps(df, tot_col, cons_col)
    
    print(f"\n{'='*80}")
    print("ANÁLISIS COMPLETADO")
    print(f"{'='*80}")
