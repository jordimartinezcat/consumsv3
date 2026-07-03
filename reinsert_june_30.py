"""
Reinsertar datos del 30/06 y 01/07 de 2026 en SQL Server
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from persistencia.save_daily_to_sqlserver import save_daily_data_to_sqlserver

def main():
    print("=" * 80)
    print("REINSERTANDO 30/06 Y 01/07 DE 2026 EN SQL SERVER")
    print("=" * 80)
    
    dates = ['2026-06-30', '2026-07-01']
    success_count = 0
    failed_dates = []
    
    for date_str in dates:
        try:
            print(f"\nProcesando {date_str}...")
            records = save_daily_data_to_sqlserver(date=date_str)
            print(f"✓ {date_str}: {records} registros guardados")
            success_count += 1
        except Exception as e:
            print(f"✗ {date_str}: ERROR - {e}")
            failed_dates.append(date_str)
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 80)
    print(f"RESUMEN: {success_count}/{len(dates)} días procesados correctamente")
    if failed_dates:
        print(f"Días fallidos: {', '.join(failed_dates)}")
    print("=" * 80)
    
    return 0 if success_count == len(dates) else 1

if __name__ == "__main__":
    sys.exit(main())
