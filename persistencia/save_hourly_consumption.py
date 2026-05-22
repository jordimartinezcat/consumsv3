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
from sqlalchemy.exc import IntegrityError, ProgrammingError

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

    # First check if index already exists
    check_stmt = text(
        """
        SELECT COUNT(*)
        FROM pg_indexes
        WHERE schemaname = 'ga_datalake'
        AND tablename = 'ite_consums_data'
        AND indexname = 'ux_ite_consums_data_data_idtag'
        """
    )

    try:
        with engine.connect() as conn:
            result = conn.execute(check_stmt)
            count = result.scalar()
            
            if count > 0:
                logging.info("Index ux_ite_consums_data_data_idtag already exists")
                return
    except Exception as e:
        logging.warning(f"Could not check index existence: {e}")

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
        logging.info("Created unique index ux_ite_consums_data_data_idtag")
    except ProgrammingError as e:
        # Permission error - log and continue
        if "InsufficientPrivilege" in str(e) or "must be owner" in str(e):
            logging.warning("Insufficient privileges to create index (user is not table owner)")
            logging.info("Continuing without index creation - assuming index exists or will be created by admin")
        else:
            logging.warning(f"Could not create index due to programming error: {e}")
        return
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
    except Exception as e:
        # Permission error or other issue - log and continue
        logging.warning(f"Could not create index (may already exist or insufficient permissions): {e}")
        logging.info("Continuing without index creation - assuming index exists")


def ensure_corrections_index(engine):
    """Ensure corrections table also has the needed unique index."""

    # First check if index already exists
    check_stmt = text(
        """
        SELECT COUNT(*)
        FROM pg_indexes
        WHERE schemaname = 'ga_datalake'
        AND tablename = 'ite_consums_datarect'
        AND indexname = 'ux_ite_consums_datarect_data_idtag_tipus'
        """
    )

    try:
        with engine.connect() as conn:
            result = conn.execute(check_stmt)
            count = result.scalar()
            
            if count > 0:
                logging.info("Index ux_ite_consums_datarect_data_idtag_tipus already exists")
                return
    except Exception as e:
        logging.warning(f"Could not check corrections index existence: {e}")

    stmt = text(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_ite_consums_datarect_data_idtag_tipus
        ON ga_datalake.ite_consums_datarect (data, idtag, tipus)
        """
    )

    try:
        with engine.connect() as conn:
            conn.execute(stmt)
            conn.commit()
        logging.info("Created unique index ux_ite_consums_datarect_data_idtag_tipus")
    except ProgrammingError as e:
        # Permission error - log and continue
        if "InsufficientPrivilege" in str(e) or "must be owner" in str(e):
            logging.warning("Insufficient privileges to create corrections index (user is not table owner)")
            logging.info("Continuing without index creation - assuming index exists or will be created by admin")
        else:
            logging.warning(f"Could not create corrections index due to programming error: {e}")
        return
    except Exception as e:
        # Permission error or other issue - log and continue
        logging.warning(f"Could not create corrections index (may already exist or insufficient permissions): {e}")
        logging.info("Continuing without index creation - assuming index exists")


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
    # NOTE: Indices already exist in remote database, skip creation to avoid permission errors
    # ensure_unique_index(db)
    # ensure_corrections_index(db)

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

    # OPTIMIZACIÓN CRÍTICA: Cargar TODOS los tags al inicio (1 query en lugar de 238×2)
    logging.info("🔄 Cargando todos los tags desde ga_landing.ite_consums_tags...")
    tag_cache = {}  # {tag_name: (idtag, per10)}
    try:
        with db.connect() as conn:
            result = conn.execute(text("""
                SELECT tag, "idTag", per10
                FROM ga_landing.ite_consums_tags
                WHERE tag IS NOT NULL
            """))
            for row in result:
                tag_name = row[0]
                idtag = int(row[1])
                per10 = bool(row[2]) if row[2] is not None else False
                tag_cache[tag_name] = (idtag, per10)
        logging.info(f"✓ Cargados {len(tag_cache)} tags en caché")
    except Exception as e:
        logging.error(f"❌ Error cargando tags: {e}")
        raise

    total_upserts = 0
    total_corrections = 0
    insertion_time = datetime.now()

    # Tracking de señales procesadas
    successful_signals = []
    missing_signals = []
    error_signals = []
    
    # OPTIMIZACIÓN: Acumular TODOS los registros de TODAS las señales
    all_records_to_insert = []
    all_correction_records = []
    correction_delete_params = []  # Para DELETE masivo de correcciones

    for base_tag, source_col, corrected_col, flag_col in consumption_sources:
        # Determinar qué columna usar como fuente de datos
        # SIEMPRE preferir corrected_col si existe
        data_col = corrected_col if corrected_col else source_col

        logging.info("Processing consumption column for %s (%s)", base_tag, data_col)

        try:
            # Convert _TOT to _CSM
            csm_tag = convert_tag_name_to_csm(base_tag)
            logging.info(f"Processing tag: {base_tag} → {csm_tag}")

            # Buscar idtag y per10 en caché (OPTIMIZADO: sin query individual)
            if csm_tag not in tag_cache:
                logging.warning(f"⚠️  Tag not found in cache: {csm_tag}")
                missing_signals.append(csm_tag)
                continue
                
            idtag, per10_enabled = tag_cache[csm_tag]
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
                f"Prepared {len(records_to_insert)} records for tag {csm_tag} (idtag={idtag})"
            )

            if negative_consumption_count > 0:
                logging.warning(
                    f"⚠️  Detectados {negative_consumption_count} consumos negativos en {csm_tag} → Corregidos a 0"
                )

            # OPTIMIZACIÓN: Acumular registros en lugar de insertar inmediatamente
            all_records_to_insert.extend(records_to_insert)
            total_upserts += len(records_to_insert)

            # Acumular parámetros para DELETE masivo de correcciones
            if records_to_insert:
                timestamps = [r["data"] for r in records_to_insert]
                min_date = min(timestamps)
                max_date = max(timestamps)
                correction_delete_params.append({
                    "idtag": idtag,
                    "min_date": min_date,
                    "max_date": max_date
                })

            # Acumular correcciones
            if correction_records:
                all_correction_records.extend(correction_records)
                total_corrections += len(correction_records)

            # Añadir a señales exitosas
            successful_signals.append(csm_tag)

        except Exception as e:
            logging.error(f"❌ Unexpected error processing {base_tag}: {str(e)}")
            error_signals.append(base_tag)
            continue

    # ==========================================================================
    # INSERCIÓN MASIVA: Ahora insertamos TODOS los registros acumulados de UNA VEZ
    # ==========================================================================
    
    logging.info("\n" + "=" * 80)
    logging.info("INICIANDO INSERCIÓN MASIVA EN POSTGRESQL")
    logging.info("=" * 80)
    
    # 1. DELETE masivo de correcciones existentes (OPTIMIZADO: por lotes)
    if correction_delete_params:
        logging.info(f"🗑️  Eliminando correcciones existentes para {len(correction_delete_params)} señales...")
        
        try:
            total_deleted = 0
            # Procesar en lotes de 50 para evitar timeouts
            batch_size = 50
            for i in range(0, len(correction_delete_params), batch_size):
                batch = correction_delete_params[i:i+batch_size]
                
                # Crear conexión nueva para cada lote
                with db.connect() as conn:
                    for params in batch:
                        delete_sql = text("""
                            DELETE FROM ga_datalake.ite_consums_datarect
                            WHERE idtag = :idtag AND tipus = 1
                              AND data >= :min_date AND data <= :max_date
                        """)
                        result = conn.execute(delete_sql, params)
                        total_deleted += result.rowcount
                    # Commit cada lote
                    conn.commit()
                logging.info(f"   ✓ Eliminadas {total_deleted} correcciones (lote {i//batch_size + 1})")
                    
            logging.info(f"✓ Total eliminadas: {total_deleted} correcciones antiguas")
        except Exception as e:
            logging.error(f"❌ Error eliminando correcciones: {e}")
    
    # 2. DELETE previo y INSERT masivo de datos de consumo
    if all_records_to_insert:
        total_records = len(all_records_to_insert)
        logging.info(f"📊 Preparando inserción de {total_records:,} registros de consumo...")
        
        try:
            # Convertir a DataFrame
            df_insert = pd.DataFrame(all_records_to_insert)
            
            # Extraer rango de fechas y señales para DELETE previo
            min_data = df_insert['data'].min()
            max_data = df_insert['data'].max()
            unique_idtags = df_insert['idtag'].unique().tolist()
            
            logging.info(f"🗑️  Eliminando datos existentes del periodo {min_data} a {max_data} para {len(unique_idtags)} señales...")
            
            # DELETE previo del mismo periodo y señales
            with db.connect() as conn:
                delete_sql = text("""
                    DELETE FROM ga_datalake.ite_consums_data
                    WHERE data >= :min_data AND data <= :max_data
                    AND idtag = ANY(:idtags)
                """)
                result = conn.execute(delete_sql, {
                    'min_data': min_data,
                    'max_data': max_data,
                    'idtags': unique_idtags
                })
                conn.commit()
                deleted_count = result.rowcount
                logging.info(f"   ✓ Eliminados {deleted_count:,} registros existentes")
            
            # INSERT masivo con pandas (sin UPSERT)
            logging.info(f"📥 Insertando {total_records:,} registros nuevos...")
            df_insert.to_sql(
                name='ite_consums_data',
                schema='ga_datalake',
                con=db,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=500
            )
            
            logging.info(f"✅ COMPLETADO: {total_records:,} registros insertados")
        except Exception as e:
            logging.error(f"❌ Error insertando datos: {e}")
            raise
    
    # 3. DELETE previo y INSERT masivo de correcciones
    if all_correction_records:
        total_corrections_to_insert = len(all_correction_records)
        logging.info(f"🔧 Preparando inserción de {total_corrections_to_insert:,} correcciones...")
        
        try:
            # Convertir a DataFrame
            df_corrections = pd.DataFrame(all_correction_records)
            
            # Extraer rango de fechas y señales para DELETE previo
            min_data_rect = df_corrections['data'].min()
            max_data_rect = df_corrections['data'].max()
            unique_idtags_rect = df_corrections['idtag'].unique().tolist()
            
            logging.info(f"🗑️  Eliminando correcciones existentes del periodo {min_data_rect} a {max_data_rect} para {len(unique_idtags_rect)} señales...")
            
            # DELETE previo del mismo periodo y señales
            with db.connect() as conn:
                delete_sql = text("""
                    DELETE FROM ga_datalake.ite_consums_datarect
                    WHERE data >= :min_data AND data <= :max_data
                    AND idtag = ANY(:idtags)
                """)
                result = conn.execute(delete_sql, {
                    'min_data': min_data_rect,
                    'max_data': max_data_rect,
                    'idtags': unique_idtags_rect
                })
                conn.commit()
                deleted_count_rect = result.rowcount
                logging.info(f"   ✓ Eliminadas {deleted_count_rect:,} correcciones existentes")
            
            # INSERT masivo con pandas
            logging.info(f"📥 Insertando {total_corrections_to_insert:,} correcciones nuevas...")
            df_corrections.to_sql(
                name='ite_consums_datarect',
                schema='ga_datalake',
                con=db,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=500
            )
                
            logging.info(f"✅ COMPLETADO: {total_corrections_to_insert:,} correcciones insertadas")
        except Exception as e:
            logging.error(f"❌ Error insertando correcciones: {e}")
            raise

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
    
    # CRÍTICO: Cerrar todas las conexiones al finalizar
    db.dispose()
    logging.info("✅ Conexiones cerradas correctamente")
    
    return total_upserts
