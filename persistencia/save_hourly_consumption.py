"""
Module for saving hourly consumption data to PostgreSQL ga_datalake.ite_consums_data table.
Reads consumption_hourly CSV files and inserts direct consumption values (not corrected).
"""

import glob
import logging
import os
import sys
from datetime import datetime

import pandas as pd

# Ajustar path para importar db_connection
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from persistencia.db_connection import get_db_connection, get_tag_id

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def get_latest_hourly_file(data_dir=None):
    """
    Find the most recent consumption_hourly CSV file.

    Args:
        data_dir (str, optional): Directory containing CSV files.
                                 Defaults to procesado/Data/

    Returns:
        str: Path to the latest file

    Raises:
        FileNotFoundError: If no files found
    """
    if data_dir is None:
        data_dir = os.path.join(ROOT, "procesado", "Data")

    pattern = os.path.join(data_dir, "consumption_hourly_*.csv")
    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(f"No consumption_hourly files found in {data_dir}")

    # Sort by filename (contains timestamp) and get the latest
    latest = sorted(files)[-1]
    logging.info(f"Latest hourly file: {os.path.basename(latest)}")
    return latest


def convert_tag_name_to_csm(tag_total):
    """
    Convert totalizer tag name to consumption tag name.
    Example: PBD07_FTR_T01_TOT → PBD07_FTR_T01_CSM

    Args:
        tag_total (str): Tag name ending with _TOT

    Returns:
        str: Tag name ending with _CSM
    """
    if not tag_total.endswith("_TOT"):
        raise ValueError(f"Tag '{tag_total}' does not end with '_TOT'")

    return tag_total.replace("_TOT", "_CSM")


def save_hourly_to_db(csv_path=None, cfg=None):
    """
    Read hourly consumption CSV and save direct consumption values to PostgreSQL.

    Args:
        csv_path (str, optional): Path to CSV file. If None, uses latest file.
        cfg (dict, optional): Configuration dictionary. If None, loads from config.

    Process:
        1. Read CSV with European format (sep=';', decimal=',')
        2. For each tag ending with _TOT_hourly_cons:
           a. Convert tag name from _TOT to _CSM
           b. Query ga_landing.cfg_tags to get idtag
           c. Insert timestamp, current datetime, idtag, and consumption value
              into ga_datalake.ite_consums_data

    Returns:
        int: Number of records inserted

    Raises:
        ValueError: If tag not found in cfg_tags
        Exception: If database operation fails
    """
    # Get latest CSV if not specified
    if csv_path is None:
        csv_path = get_latest_hourly_file()

    logging.info(f"Reading hourly data from: {csv_path}")

    # Read CSV with European format
    df = pd.read_csv(csv_path, sep=";", decimal=",")

    logging.info(f"Loaded {len(df)} hourly records")
    logging.info(f"Columns: {df.columns.tolist()}")

    # Get database connection
    db = get_db_connection(cfg)

    # Find consumption columns (ending with _hourly_cons, not _corrected or _has_corrections)
    consumption_cols = [
        col
        for col in df.columns
        if col.endswith("_hourly_cons")
        and not col.endswith("_corrected")
        and not col.endswith("_has_corrections")
    ]

    if not consumption_cols:
        raise ValueError("No consumption columns found in CSV")

    logging.info(
        f"Found {len(consumption_cols)} consumption column(s): {consumption_cols}"
    )

    total_inserted = 0
    insertion_time = datetime.now()

    for cons_col in consumption_cols:
        # Extract base tag name (remove _hourly_cons suffix)
        # Example: PBD07_FTR_T01_TOT_hourly_cons → PBD07_FTR_T01_TOT
        base_tag = cons_col.replace("_hourly_cons", "")

        # Convert _TOT to _CSM
        csm_tag = convert_tag_name_to_csm(base_tag)
        logging.info(f"Processing tag: {base_tag} → {csm_tag}")

        # Get idtag from cfg_tags
        try:
            idtag = get_tag_id(db, csm_tag)
        except ValueError as e:
            logging.error(str(e))
            raise

        # Prepare insert query
        insert_query = """
            INSERT INTO ga_datalake.ite_consums_data 
            (timestamp, insertion_date, idtag, value)
            VALUES (%s, %s, %s, %s)
        """

        # Prepare data for insertion
        records_to_insert = []
        for _, row in df.iterrows():
            timestamp = row["timeStamp"]
            value = row[cons_col]

            # Skip NaN values
            if pd.isna(value):
                continue

            records_to_insert.append((timestamp, insertion_time, idtag, float(value)))

        logging.info(
            f"Inserting {len(records_to_insert)} records for tag {csm_tag} (idtag={idtag})"
        )

        # Execute batch insert using SQLAlchemy
        try:
            # Build INSERT statement with all values
            values_str = ",".join(
                f"('{ts}', '{insertion_time.strftime('%Y-%m-%d %H:%M:%S')}', {idtag}, {val})"
                for ts, _, _, val in records_to_insert
            )
            insert_sql = f"""
                INSERT INTO ga_datalake.ite_consums_data 
                (data, data_insercio, idtag, valor)
                VALUES {values_str}
            """

            from sqlalchemy import text

            with db.connect() as conn:
                conn.execute(text(insert_sql))
                conn.commit()

            total_inserted += len(records_to_insert)
            logging.info(
                f"Successfully inserted {len(records_to_insert)} records for {csm_tag}"
            )
        except Exception as e:
            logging.error(f"Failed to insert records for {csm_tag}: {e}")
            raise

    logging.info(f"Total records inserted: {total_inserted}")
    return total_inserted
