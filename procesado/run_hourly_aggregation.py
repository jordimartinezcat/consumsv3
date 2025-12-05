"""
Script principal para ejecutar la agregación de consumos horarios.

Procesa los datos minutales más recientes y genera un archivo CSV con:
- Suma horaria directa de consumos
- Suma horaria aplicando correcciones de anomalías
"""
import os
import sys

# Agregar rutas necesarias
current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, current_dir)
sys.path.insert(0, parent_dir)

from compute_hourly_consumption import process_latest_minute_data


def main():
    """Función principal para ejecutar la agregación horaria."""
    try:
        print("=== Iniciando Agregación Horaria de Consumos ===")
        
        # Procesar datos y generar archivo horario
        output_file = process_latest_minute_data()
        
        print("=== Procesamiento Completado Exitosamente ===")
        print(f"Archivo generado: {output_file}")
        
        return 0
        
    except FileNotFoundError as e:
        print(f"Error: Archivo no encontrado - {e}")
        return 1
    except Exception as e:
        print(f"Error durante el procesamiento: {e}")
        return 2


if __name__ == '__main__':
    raise SystemExit(main())