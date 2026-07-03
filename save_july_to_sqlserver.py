#!/usr/bin/env python3
"""
Script para guardar datos de JULIO 2026 en SQL Server (Consums_dia).

Ejecuta save_daily_data_to_sqlserver() para cada día de julio 2026.
"""

import os
import sys
import logging
from datetime import datetime, timedelta

# Configurar rutas
ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

from persistencia.save_daily_to_sqlserver import save_daily_data_to_sqlserver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """Guardar todos los días de julio 2026 en SQL Server."""
    
    print("="*80)
    print("GUARDADO DE JULIO 2026 EN SQL SERVER (Consums_dia)")
    print("="*80)
    print()
    
    # Definir rango de fechas: 1 de julio a 31 de julio 2026
    start_date = datetime(2026, 7, 1)
    end_date = datetime(2026, 7, 31)
    
    total_days = (end_date - start_date).days + 1
    
    print(f"Procesando {total_days} días: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
    print()
    
    successful = 0
    failed = 0
    failed_dates = []
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        try:
            print(f"\n[{current_date.day}/{total_days}] Procesando {date_str}...")
            
            # Llamar a la función de guardado
            records = save_daily_data_to_sqlserver(date=date_str)
            
            print(f"  ✓ {date_str}: {records} registros guardados")
            successful += 1
            
        except Exception as e:
            print(f"  ✗ {date_str}: ERROR - {e}")
            logging.error(f"Error procesando {date_str}: {e}", exc_info=True)
            failed += 1
            failed_dates.append(date_str)
        
        # Siguiente día
        current_date += timedelta(days=1)
    
    # Resumen final
    print("\n" + "="*80)
    print("RESUMEN FINAL")
    print("="*80)
    print(f"Total días procesados: {total_days}")
    print(f"  ✓ Exitosos: {successful}")
    print(f"  ✗ Fallidos: {failed}")
    
    if failed_dates:
        print("\nFechas con errores:")
        for date_str in failed_dates:
            print(f"  - {date_str}")
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
