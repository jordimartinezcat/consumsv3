"""
Module to save hourly consumption corrections to PostgreSQL.
Only inserts records that have corrections (where *_hourly_has_corrections is True).
"""

import glob
import logging
import os
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import text

# Ajustar path para importar módulos
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from persistencia.db_connection import get_db_connection, get_tag_id, get_tag_per10

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def get_latest_hourly_file(data_dir=None):
    """
    Find the most recent consumption_hourly_*.csv file.

    Args:
        data_dir (str, optional): Directory to search.
                                 If None, uses procesado/Data/

    Returns:
        str: Full path to the latest CSV file

    Raises:
        FileNotFoundError: If no CSV files found
    """
    if data_dir is None:
        data_dir = os.path.join(ROOT, "procesado", "Data")

    pattern = os.path.join(data_dir, "consumption_hourly_*.csv")
    files = glob.glob(pattern)

    if not files:
        raise FileNotFoundError(
            f"No consumption_hourly_*.csv files found in {data_dir}"
        )

    # Sort by filename (contains timestamp) and get the latest
    latest_file = sorted(files)[-1]
    logging.info(f"Latest hourly file: {os.path.basename(latest_file)}")

    return latest_file


def convert_tag_name_to_csm(tag_total):
    """
    Convert tag name from _TOT to _CSM format.

    Args:
        tag_total (str): Tag name ending with _TOT

    Returns:
        str: Tag name ending with _CSM
    """
    return tag_total.replace("_TOT", "_CSM")


def save_hourly_corrections_to_db(csv_path=None, cfg=None):
    """
    Save hourly consumption corrections to ga_datalake.ite_consums_datarect.

    Only inserts records where corrections were applied (*_hourly_has_corrections = True).
    All corrections will have tipus=1 (script corrections).

    Process for each consumption column:
        1. Filter rows where *_hourly_has_corrections is True
        2. Convert tag name from _TOT to _CSM
        3. Query ga_landing.cfg_tags to get idtag
        4. Insert timestamp, current datetime, idtag, corrected_value, tipus=1, description
           into ga_datalake.ite_consums_datarect

    Args:
        csv_path (str, optional): Path to CSV file. If None, uses latest file.
        cfg (dict, optional): Configuration dictionary. If None, loads from config.

    Returns:
        int: Number of correction records inserted

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

        # Build column names
        corrected_col = f"{base_tag}_hourly_cons_corrected"
        has_corrections_col = f"{base_tag}_hourly_has_corrections"

        # Verify columns exist
        if corrected_col not in df.columns or has_corrections_col not in df.columns:
            logging.warning(f"Correction columns not found for {base_tag}, skipping")
            continue

        # Filter only rows with corrections
        df_with_corrections = df[df[has_corrections_col] == True].copy()

        if len(df_with_corrections) == 0:
            logging.info(f"No corrections found for {base_tag}, skipping")
            continue

        # Convert _TOT to _CSM
        csm_tag = convert_tag_name_to_csm(base_tag)
        logging.info(f"Processing corrections for tag: {base_tag} → {csm_tag}")

        # Get idtag from cfg_tags
        idtag = get_tag_id(db, csm_tag)

        # Check per10 flag for this tag
        per10_enabled = get_tag_per10(db.engine, base_tag)
        per10_multiplier = 10.0 if per10_enabled else 1.0
        if per10_enabled:
            logging.info(f"✓ per10 multiplier ENABLED for {csm_tag} (x10)")

        # Prepare records for insertion
        records_to_insert = []
        for _, row in df_with_corrections.iterrows():
            timestamp = row["timeStamp"]
            corrected_value = row[corrected_col]

            # Skip NaN values
            if pd.isna(corrected_value):
                continue

            # Apply per10 multiplier if enabled
            final_value = float(corrected_value) * per10_multiplier

            # Create description
            original_value = row[cons_col]
            descrip = f"Script correction: {original_value:.3f} → {corrected_value:.3f}"

            records_to_insert.append(
                (timestamp, insertion_time, idtag, final_value, descrip)
            )

        logging.info(
            f"Inserting {len(records_to_insert)} correction records for tag {csm_tag} (idtag={idtag})"
        )

        # Execute batch insert using SQLAlchemy
        try:
            # Build INSERT statement with all values
            values_str = ",".join(
                f"('{ts}', '{insertion_time.strftime('%Y-%m-%d %H:%M:%S')}', {idtag}, {val}, 1, '{desc}')"
                for ts, _, _, val, desc in records_to_insert
            )
            insert_sql = f"""
                INSERT INTO ga_datalake.ite_consums_datarect 
                (data, data_insercio, idtag, valor, tipus, descrip)
                VALUES {values_str}
            """

            from sqlalchemy import text

            with db.connect() as conn:
                conn.execute(text(insert_sql))
                conn.commit()

            total_inserted += len(records_to_insert)
            logging.info(
                f"Successfully inserted {len(records_to_insert)} correction records for {csm_tag}"
            )
        except Exception as e:
            logging.error(f"Failed to insert correction records for {csm_tag}: {e}")
            raise

    logging.info(f"Total correction records inserted: {total_inserted}")
    return total_inserted
    return total_inserted
