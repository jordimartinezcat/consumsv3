"""
Script to save hourly consumption corrections to PostgreSQL.
Executes save_hourly_corrections_to_db() with error handling and logging.
"""

import logging
import os
import sys

# Add parent directory to path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from persistencia.save_hourly_corrections import save_hourly_corrections_to_db

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def main():
    """
    Main function to execute hourly consumption corrections persistence.
    """
    try:
        logging.info("=== Starting Hourly Consumption Corrections Persistence ===")

        # Save corrections to database
        total_records = save_hourly_corrections_to_db()

        logging.info("=== Persistence Completed Successfully ===")
        logging.info(f"Total correction records inserted: {total_records}")

        return 0

    except FileNotFoundError as e:
        logging.error(f"File error: {e}")
        return 1
    except ValueError as e:
        logging.error(f"Validation error: {e}")
        return 1
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
