"""
Module for saving hourly consumption data to PostgreSQL ga_datalake.ite_consums_data table.
Reads consumption_hourly CSV files and inserts direct consumption values (not corrected).
"""

import glob
import logging
import math
import os
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

# Ajustar path para importar db_connection
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from persistencia.db_connection import get_db_connection, get_tag_id, get_tag_per10

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


def ensure_corrections_index(engine):
    """Ensure corrections table also has the needed unique index."""

    stmt = text(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_ite_consums_datarect_data_idtag_tipus
        ON ga_datalake.ite_consums_datarect (data, idtag, tipus)
        """
    )

    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()
    logging.info("Ensured unique index ux_ite_consums_datarect_data_idtag_tipus exists")


def find_consumption_sources(df: pd.DataFrame):
    """Return tuples (base_tag, raw_col, corrected_col, has_flag_col)."""

    raw_suffix = "_hourly_cons"
    corrected_suffix = "_hourly_cons_corrected"
    flag_suffix = "_hourly_has_corrections"

    sources = []

    for col in df.columns:
        if not col.endswith(raw_suffix) or col.endswith(corrected_suffix):
            continue

        base_tag = col[: -len(raw_suffix)]
        corrected_col = f"{base_tag}{corrected_suffix}"
        if corrected_col not in df.columns:
            corrected_col = None

        flag_col = f"{base_tag}{flag_suffix}"
        if flag_col not in df.columns:
            flag_col = None

        sources.append((base_tag, col, corrected_col, flag_col))

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
    Example: CL_CAT_PBD07_FTR_T01_TOT → PBD07_FTR_T01_CSM
            (removes CL_CAT_ prefix and replaces _TOT with _CSM)

    Args:
        tag_total (str): Tag name ending with _TOT (may include CL_CAT_ prefix)

    Returns:
        str: Tag name ending with _CSM (without CL_CAT_ prefix)
    """
    if not tag_total.endswith("_TOT"):
        raise ValueError(f"Tag '{tag_total}' does not end with '_TOT'")

    # Remove CL_CAT_ prefix if present
    tag_without_prefix = tag_total.replace("CL_CAT_", "")

    # Replace _TOT with _CSM
    return tag_without_prefix.replace("_TOT", "_CSM")


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

    # Ensure unique indices exist so ON CONFLICT works
    ensure_unique_index(db)
    ensure_corrections_index(db)

    consumption_sources = find_consumption_sources(df)

    if not consumption_sources:
        raise ValueError("No consumption columns found in CSV")

    log_sources = [
        f"{base} (has_corrected={corrected_col is not None})"
        for base, _, corrected_col, _ in consumption_sources
    ]
    logging.info(
        "Found %d consumption column(s): %s",
        len(consumption_sources),
        ", ".join(log_sources),
    )

    total_upserts = 0
    total_corrections = 0
    insertion_time = datetime.now()

    # Tracking de señales procesadas
    successful_signals = []
    missing_signals = []
    error_signals = []

    for base_tag, source_col, corrected_col, flag_col in consumption_sources:
        # Determinar qué columna usar como fuente de datos
        # SIEMPRE preferir corrected_col si existe
        data_col = corrected_col if corrected_col else source_col

        logging.info("Processing consumption column for %s (%s)", base_tag, data_col)

        try:
            # Convert _TOT to _CSM
            csm_tag = convert_tag_name_to_csm(base_tag)
            logging.info(f"Processing tag: {base_tag} → {csm_tag}")

            # Get idtag from cfg_tags
            try:
                idtag = get_tag_id(db, csm_tag)
            except ValueError as e:
                logging.warning(f"⚠️  Tag not found in cfg_tags: {csm_tag}")
                missing_signals.append(csm_tag)
                continue

            # Check per10 flag for this tag
            per10_enabled = get_tag_per10(db.engine, base_tag)
            per10_multiplier = 10.0 if per10_enabled else 1.0
            if per10_enabled:
                logging.info(f"✓ per10 multiplier ENABLED for {csm_tag} (x10)")

            # Prepare data for insertion
            records_to_insert = []
            correction_records = []
            negative_consumption_count = 0

            # PASO 1: Detectar pares compensatorios (negativo + positivo)
            compensatory_pairs = (
                {}
            )  # {index: info} para índices que forman parte de un par

            df_sorted = df.sort_index()
            for i in range(len(df_sorted) - 1):
                curr_value = df_sorted[data_col].iloc[i]
                next_value = df_sorted[data_col].iloc[i + 1]
                curr_idx = i
                next_idx = i + 1
                curr_ts = df_sorted.index[i]
                next_ts = df_sorted.index[i + 1]

                # Detectar par: curr negativo, next positivo, magnitudes similares
                if pd.notna(curr_value) and pd.notna(next_value):
                    if curr_value < -100 and next_value > 100:
                        ratio = abs(next_value / curr_value)
                        if 0.8 < ratio < 1.2:  # Magnitudes dentro del 20%
                            # Par compensatorio detectado
                            net_value = curr_value + next_value
                            logging.warning(
                                f"⚠️  Par compensatorio detectado en {csm_tag}:"
                            )
                            logging.warning(
                                f"   Índice {curr_idx}: {curr_value:.2f} L + Índice {next_idx}: {next_value:.2f} L = {net_value:.2f} L"
                            )

                            # Marcar ambos índices para corrección
                            compensatory_pairs[curr_idx] = {
                                "type": "negative",
                                "net_value": net_value,
                                "next_idx": next_idx,
                                "original_neg": curr_value,
                                "original_pos": next_value,
                            }
                            compensatory_pairs[next_idx] = {
                                "type": "positive"
                            }  # Simplemente marcar como parte del par

            # PASO 2: Procesar datos aplicando correcciones
            for position, (idx, row) in enumerate(df.iterrows()):
                timestamp_raw = row["timeStamp"]
                value = row[data_col]  # Usar data_col (corrected si existe, sino raw)

                # Skip NaN values
                if pd.isna(value):
                    continue

                try:
                    local_ts = to_local_timestamp(timestamp_raw)
                except ValueError as exc:
                    logging.warning(
                        "Skipping timestamp %s for %s: %s", timestamp_raw, base_tag, exc
                    )
                    continue

                raw_value = float(value)

                # Si estamos usando la columna corrected, también obtener el raw para registrar corrección
                raw_from_source = None
                if data_col == corrected_col and source_col in df.columns:
                    raw_from_source = row[source_col]
                    if pd.notna(raw_from_source):
                        raw_from_source = float(raw_from_source)
                        # Si son diferentes, registrar la corrección
                        if not math.isclose(
                            raw_value, raw_from_source, rel_tol=1e-9, abs_tol=1e-6
                        ):
                            descrip = f"Reset correction: {raw_from_source:.3f} -> {raw_value:.3f}"
                            correction_records.append(
                                {
                                    "data": local_ts,
                                    "data_insercio": insertion_time,
                                    "idtag": int(idtag),
                                    "valor": raw_value
                                    * per10_multiplier,  # Aplicar per10
                                    "tipus": 1,
                                    "descrip": descrip,
                                }
                            )

                # Verificar si es parte de un par compensatorio (usar position para indexar)
                if position in compensatory_pairs:
                    pair_info = compensatory_pairs[position]
                    if (
                        pair_info["type"] == "negative"
                    ):  # Es el timestamp negativo (el primero del par)
                        net_value = float(pair_info["net_value"])

                        # Crear corrección para el timestamp negativo (usar valor neto con per10)
                        descrip = f"Compensatory pair correction: {pair_info['original_neg']:.3f} + {pair_info['original_pos']:.3f} = {net_value:.3f}"
                        correction_records.append(
                            {
                                "data": local_ts,
                                "data_insercio": insertion_time,
                                "idtag": int(idtag),
                                "valor": float(max(0.0, net_value))
                                * per10_multiplier,  # Aplicar per10
                                "tipus": 1,
                                "descrip": descrip,
                            }
                        )
                        raw_value = float(max(0.0, net_value))
                        negative_consumption_count += 1
                    else:  # Es el timestamp positivo (el segundo del par)
                        # Crear corrección para eliminarlo (ya se contabilizó en el primero)
                        descrip = f"Compensatory pair correction: eliminated (counted in previous hour)"
                        correction_records.append(
                            {
                                "data": local_ts,
                                "data_insercio": insertion_time,
                                "idtag": int(idtag),
                                "valor": 0.0
                                * per10_multiplier,  # Aplicar per10 (0.0 * 10 = 0.0)
                                "tipus": 1,
                                "descrip": descrip,
                            }
                        )
                        raw_value = 0.0

                # Detectar consumos negativos NO compensados (safety net)
                elif raw_value < 0:
                    negative_consumption_count += 1
                    logging.warning(
                        f"⚠️  Consumo negativo NO compensado en {csm_tag} a las {local_ts}: {raw_value:.2f} L"
                    )
                    logging.warning(
                        f"   → Corrigiendo a 0.0 y guardando como rectificación"
                    )

                    # Crear rectificación automática (aplicar per10 si está habilitado)
                    descrip = f"Negative consumption correction: {raw_value:.3f} → 0.0 (automatic)"
                    correction_records.append(
                        {
                            "data": local_ts,
                            "data_insercio": insertion_time,
                            "idtag": int(idtag),
                            "valor": 0.0
                            * per10_multiplier,  # Aplicar per10 (0.0 * 10 = 0.0)
                            "tipus": 1,
                            "descrip": descrip,
                        }
                    )
                    # Guardar 0 en lugar del valor negativo
                    raw_value = 0.0

                # Aplicar multiplicador per10 si está habilitado
                final_value = raw_value * per10_multiplier

                records_to_insert.append(
                    {
                        "data": local_ts,
                        "data_insercio": insertion_time,
                        "idtag": int(idtag),
                        "valor": final_value,
                    }
                )

            logging.info(
                f"Inserting {len(records_to_insert)} records for tag {csm_tag} (idtag={idtag})"
            )

            if negative_consumption_count > 0:
                logging.warning(
                    f"⚠️  Detectados {negative_consumption_count} consumos negativos en {csm_tag} → Corregidos a 0"
                )

            # Execute batch upsert using SQLAlchemy
            try:
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
                logging.info(f"Upserted {len(records_to_insert)} records for {csm_tag}")
            except Exception as e:
                logging.error(f"Failed to insert records for {csm_tag}: {e}")
                error_signals.append(csm_tag)
                continue

            # Delete existing corrections for this period/tag before inserting new ones
            # This ensures old erroneous corrections don't persist
            if records_to_insert:
                try:
                    timestamps = [r["data"] for r in records_to_insert]
                    min_date = min(timestamps)
                    max_date = max(timestamps)

                    delete_sql = text(
                        """
                        DELETE FROM ga_datalake.ite_consums_datarect
                        WHERE idtag = :idtag
                          AND tipus = 1
                          AND data >= :min_date
                          AND data <= :max_date
                        """
                    )

                    with db.connect() as conn:
                        result = conn.execute(
                            delete_sql,
                            {
                                "idtag": idtag,
                                "min_date": min_date,
                                "max_date": max_date,
                            },
                        )
                        deleted_count = result.rowcount
                        conn.commit()

                    logging.info(
                        f"Deleted {deleted_count} existing correction(s) for idtag={idtag} "
                        f"between {min_date} and {max_date}"
                    )
                except Exception as exc:
                    logging.error(
                        "Failed to delete existing corrections for %s: %s", csm_tag, exc
                    )
                    error_signals.append(csm_tag)
                    continue

            if correction_records:
                logging.info(
                    "Inserting %d corrected records into ga_datalake.ite_consums_datarect",
                    len(correction_records),
                )

                correction_sql = text(
                    """
                    INSERT INTO ga_datalake.ite_consums_datarect
                        (data, data_insercio, idtag, valor, tipus, descrip)
                    VALUES
                        (:data, :data_insercio, :idtag, :valor, :tipus, :descrip)
                    ON CONFLICT (data, idtag, tipus)
                    DO UPDATE SET
                        valor = EXCLUDED.valor,
                        data_insercio = EXCLUDED.data_insercio,
                        descrip = EXCLUDED.descrip
                    """
                )

                try:
                    with db.connect() as conn:
                        conn.execute(correction_sql, correction_records)
                        conn.commit()
                    total_corrections += len(correction_records)
                except Exception as exc:
                    logging.error(
                        "Failed to insert correction records for %s: %s", csm_tag, exc
                    )
                    error_signals.append(csm_tag)
                    continue

            # Añadir a señales exitosas
            successful_signals.append(csm_tag)

        except Exception as e:
            logging.error(f"❌ Unexpected error processing {base_tag}: {str(e)}")
            error_signals.append(base_tag)
            continue

    # Resumen final
    logging.info("\n" + "=" * 80)
    logging.info("RESUMEN DE INSERCIÓN EN POSTGRESQL")
    logging.info("=" * 80)
    logging.info(f"✓ Señales insertadas correctamente: {len(successful_signals)}")
    logging.info(f"  Total registros: {total_upserts}")
    logging.info(f"  Total correcciones: {total_corrections}")

    if missing_signals:
        logging.warning(
            f"\n⚠️  Señales NO encontradas en cfg_tags ({len(missing_signals)}):"
        )
        for signal in sorted(set(missing_signals)):
            logging.warning(f"   - {signal}")

    if error_signals:
        logging.error(
            f"\n❌ Señales con errores durante inserción ({len(set(error_signals))}):"
        )
        for signal in sorted(set(error_signals)):
            logging.error(f"   - {signal}")

    if not missing_signals and not error_signals:
        logging.info("\n✓ Todas las señales fueron procesadas correctamente")

    logging.info("=" * 80 + "\n")

    logging.info(f"Total records upserted: {total_upserts}")
    logging.info(f"Total corrections upserted: {total_corrections}")
    return total_upserts
    return total_upserts
