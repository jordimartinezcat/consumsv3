"""
Main execution script for saving hourly consumption data to PostgreSQL.
Reads the latest consumption_hourly CSV and persists to ga_datalake.ite_consums_data.
"""

import logging
import os
import sys

# Ajustar path para importar módulos
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from persistencia.save_hourly_consumption import save_hourly_to_db

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    """
    Main execution function for hourly data persistence.
    """
    try:
        logging.info("=== Starting Hourly Consumption Persistence ===")

        # Save hourly data to database
        total_records = save_hourly_to_db()

        logging.info(f"=== Persistence Completed Successfully ===")
        logging.info(f"Total records inserted: {total_records}")

    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
        sys.exit(1)
    except ValueError as e:
        logging.error(f"Validation error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
