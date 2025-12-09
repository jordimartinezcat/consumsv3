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
from sqlalchemy.exc import IntegrityError

# Ajustar path para importar db_connection
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from persistencia.db_connection import get_db_connection, get_tag_id

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

SOURCE_TZ = "UTC"
TARGET_TZ = "Europe/Madrid"


def to_local_timestamp(value):
    """Parse a timestamp value (assumed UTC) and return localized datetime."""

    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Invalid timestamp value: {value}")

    if ts.tzinfo is None:
        ts = ts.tz_localize(SOURCE_TZ)
    else:
        ts = ts.tz_convert(SOURCE_TZ)

    local_ts = ts.tz_convert(TARGET_TZ)
    return local_ts.to_pydatetime()


def ensure_unique_index(engine):
    """Create the (data,idtag) unique index required for upserts if it doesn't exist."""

    from sqlalchemy import text

    stmt = text(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_ite_consums_data_data_idtag
        ON ga_datalake.ite_consums_data (data, idtag)
        """
    )

    try:
        with engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        logging.info("Ensured unique index ux_ite_consums_data_data_idtag exists")
    except IntegrityError:
        logging.warning(
            "Duplicate rows detected for (data,idtag); deleting extras before recreating index"
        )
        from sqlalchemy import text

        cleanup_sql = text(
            """
            WITH ranked AS (
                SELECT ctid, ROW_NUMBER() OVER (
                    PARTITION BY data, idtag
                    ORDER BY data_insercio DESC
                ) AS rn
                FROM ga_datalake.ite_consums_data
            )
            DELETE FROM ga_datalake.ite_consums_data
            WHERE ctid IN (SELECT ctid FROM ranked WHERE rn > 1)
            """
        )

        with engine.connect() as conn:
            result = conn.execute(cleanup_sql)
            conn.commit()
            logging.info("Removed %s duplicate rows", result.rowcount)

        # retry index creation
        with engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        logging.info("Unique index created after cleanup")


def find_consumption_sources(df: pd.DataFrame):
    """Return list of tuples (base_tag, column_name, is_corrected).

    Prefer *_hourly_cons_corrected when available; fall back to *_hourly_cons.
    """

    corrected_suffix = "_hourly_cons_corrected"
    raw_suffix = "_hourly_cons"

    sources = []

    # Track which base tags already mapped via corrected columns
    mapped = set()

    for col in df.columns:
        if col.endswith(corrected_suffix):
            base_tag = col[: -len(corrected_suffix)]
            sources.append((base_tag, col, True))
            mapped.add(base_tag)

    for col in df.columns:
        if col.endswith(raw_suffix) and not col.endswith(corrected_suffix):
            base_tag = col[: -len(raw_suffix)]
            if base_tag in mapped:
                continue
            sources.append((base_tag, col, False))

    return sources


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
              c. Upsert timestamp, idtag, and consumption value into
                  ga_datalake.ite_consums_data

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

    # Ensure unique index exists so ON CONFLICT works
    ensure_unique_index(db)

    consumption_sources = find_consumption_sources(df)

    if not consumption_sources:
        raise ValueError("No consumption columns found in CSV")

    log_sources = [f"{base} (corrected={is_corr})" for base, _, is_corr in consumption_sources]
    logging.info(
        "Found %d consumption column(s): %s",
        len(consumption_sources),
        ", ".join(log_sources),
    )

    total_upserts = 0
    insertion_time = datetime.now()

    for base_tag, source_col, is_corrected in consumption_sources:
        if is_corrected:
            logging.info(
                "Using corrected consumption column for %s (%s)", base_tag, source_col
            )
        else:
            logging.info("Using raw consumption column for %s (%s)", base_tag, source_col)

        # Convert _TOT to _CSM
        csm_tag = convert_tag_name_to_csm(base_tag)
        logging.info(f"Processing tag: {base_tag} → {csm_tag}")

        # Get idtag from cfg_tags
        try:
            idtag = get_tag_id(db, csm_tag)
        except ValueError as e:
            logging.error(str(e))
            raise

        # Prepare data for insertion
        records_to_insert = []
        for _, row in df.iterrows():
            timestamp_raw = row["timeStamp"]
            value = row[source_col]

            # Skip NaN values
            if pd.isna(value):
                continue

            records_to_insert.append(
                {
                    "data": to_local_timestamp(timestamp_raw),
                    "data_insercio": insertion_time,
                    "idtag": int(idtag),
                    "valor": float(value),
                }
            )

        logging.info(
            f"Inserting {len(records_to_insert)} records for tag {csm_tag} (idtag={idtag})"
        )

        # Execute batch upsert using SQLAlchemy
        try:
            from sqlalchemy import text

            insert_sql = text(
                """
                INSERT INTO ga_datalake.ite_consums_data
                    (data, data_insercio, idtag, valor)
                VALUES
                    (:data, :data_insercio, :idtag, :valor)
                ON CONFLICT (data, idtag)
                DO UPDATE SET
                    valor = EXCLUDED.valor,
                    data_insercio = EXCLUDED.data_insercio
                """
            )

            with db.connect() as conn:
                conn.execute(insert_sql, records_to_insert)
                conn.commit()

            total_upserts += len(records_to_insert)
            logging.info(
                f"Upserted {len(records_to_insert)} records for {csm_tag}"
            )
        except Exception as e:
            logging.error(f"Failed to insert records for {csm_tag}: {e}")
            raise

    logging.info(f"Total records upserted: {total_upserts}")
    return total_upserts
