"""
Script de ejecución para guardar datos diarios en SQL Server.
"""

import logging
import os
import sys

# Ajustar path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from persistencia.save_daily_to_sqlserver import save_daily_data_to_sqlserver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """
    Ejecutar guardado de datos diarios en SQL Server.
    """
    logging.info("=== Inicio: Guardado SQL Server ===")
    
    try:
        total_records = save_daily_data_to_sqlserver()
        
        logging.info(f"=== Fin: {total_records} registros guardados ===")
        return 0
    
    except Exception as e:
        logging.error(f"[ERROR] Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
