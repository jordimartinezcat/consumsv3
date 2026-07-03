"""
Script de ejecución para actualizar contadores desde Consums_dia_old.
"""
import logging
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from persistencia.update_from_old import update_counters_from_old

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """
    Ejecutar actualización de contadores desde Consums_dia_old.
    Actualiza tanto MSSQL (Consums_dia) como PostgreSQL (ite_consums_datarect).
    """
    logging.info("=== Inicio: Actualización desde Consums_dia_old ===")
    
    try:
        mssql_records, pg_records = update_counters_from_old()
        
        logging.info(f"=== Fin ===")
        logging.info(f"  - MSSQL (Consums_dia): {mssql_records} registros actualizados")
        logging.info(f"  - PostgreSQL (ite_consums_datarect): {pg_records} registros insertados")
        return 0
    
    except Exception as e:
        logging.error(f"[ERROR] Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
