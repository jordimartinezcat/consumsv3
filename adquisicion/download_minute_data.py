import json
import logging
import os
import sys
from datetime import datetime

import pandas as pd

# Ajustar path para importar submódulo
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
if os.path.join(ROOT, "CAT_Conexions", "src") not in sys.path:
    sys.path.insert(0, os.path.join(ROOT, "CAT_Conexions", "src"))

from conexions import apiSagedCAT

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def check_cached_files_exist(tags, out_dir):
    """Check if all expected CSV files exist for the given tags.

    Args:
        tags: List of tag names (without _H/_L suffix)
        out_dir: Directory where CSV files should be

    Returns:
        tuple: (all_exist: bool, missing_tags: list)
    """
    missing = []
    for tag in tags:
        # Check both _H and _L files (or single file if no suffix)
        h_file = os.path.join(out_dir, f"{tag}_H.csv")
        l_file = os.path.join(out_dir, f"{tag}_L.csv")
        single_file = os.path.join(out_dir, f"{tag}.csv")

        # Tag must have either both H/L files OR a single file
        has_hl = os.path.exists(h_file) and os.path.exists(l_file)
        has_single = os.path.exists(single_file)

        if not (has_hl or has_single):
            missing.append(tag)

    return len(missing) == 0, missing


def download_minute_data(cfg=None):
    """Download minute data according to configuration and return combined DataFrame.

    Returns (combined_df or None, missing list)
    """
    # Load config if not provided
    if cfg is None:
        CONFIG_PATH = os.path.join(ROOT, "consums_config.json")
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)

    api_cfg = cfg.get("api", {})
    base_url = api_cfg.get("base_url")
    if not base_url:
        logging.error("api.base_url not set in config")
        raise SystemExit(1)

    nexustoken = api_cfg.get("nexustoken")
    vista = api_cfg.get("vista")

    headers = (
        {"nexustoken": nexustoken, "Content-Type": "application/json"}
        if nexustoken
        else None
    )
    api = apiSagedCAT(vista=vista, headers=headers)

    period = cfg.get("period", {})
    start = period.get("start") or datetime.now().strftime("%Y-%m-%d 00:00:00")
    end = period.get("end") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    resolution = "RES_1_MIN"

    # Leer señales preparadas
    signals_file = os.path.join(os.path.dirname(__file__), "senales_para_descarga.txt")

    # Determinar comportamiento según filtro en la config
    filter_prefix = None
    use_cached = True  # default
    for task in cfg.get("tasks", []):
        if task.get("name") == "fetch_api_data":
            filter_prefix = task.get("filter")
            use_cached = task.get("cached", True)
            break

    use_all = False
    if filter_prefix is None or str(filter_prefix).strip() == "":
        use_all = True

    if not use_all:
        if not os.path.exists(signals_file):
            logging.error("No existe el fichero de señales: %s", signals_file)
            raise SystemExit(1)

        with open(signals_file, "r", encoding="utf-8") as f:
            tags = [line.strip() for line in f.readlines() if line.strip()]

        if not tags:
            logging.info("No hay tags en %s", signals_file)
            return None, []

        # Aplicar filtro de configuración sobre la lista de señales
        # Soporta: subcadena simple O regex con | para múltiples valores
        if filter_prefix:
            import re

            orig_count = len(tags)

            # Si el filtro contiene |, tratarlo como regex (OR pattern)
            if "|" in filter_prefix:
                pattern = re.compile(filter_prefix)
                tags = [t for t in tags if pattern.search(t)]
            else:
                # Si no, búsqueda simple de subcadena
                tags = [t for t in tags if filter_prefix in t]

            logging.info(
                "Filtro '%s' aplicado a señales: %d -> %d",
                filter_prefix,
                orig_count,
                len(tags),
            )
            if not tags:
                logging.warning(
                    "No hay señales que contengan '%s' en %s",
                    filter_prefix,
                    signals_file,
                )
                return None, []

        # Check if cached files exist when cache is enabled
        out_dir = os.path.join(os.path.dirname(__file__), "minute_data")
        if use_cached:
            all_cached, missing_tags = check_cached_files_exist(tags, out_dir)

            if all_cached:
                logging.info(
                    "[CACHE] Cache valido: todos los archivos CSV del filtro '%s' existen (%d tags)",
                    filter_prefix or "ALL",
                    len(tags),
                )
                logging.info("Cargando datos desde archivos CSV cacheados...")

                # Load individual CSV files and combine them
                combined = []
                for tag in tags:
                    h_file = os.path.join(out_dir, f"{tag}_H.csv")
                    l_file = os.path.join(out_dir, f"{tag}_L.csv")
                    single_file = os.path.join(out_dir, f"{tag}.csv")

                    if os.path.exists(h_file) and os.path.exists(l_file):
                        # Load H and L files
                        df_h = pd.read_csv(
                            h_file, parse_dates=["timeStamp"], index_col="timeStamp"
                        )
                        df_l = pd.read_csv(
                            l_file, parse_dates=["timeStamp"], index_col="timeStamp"
                        )
                        combined.append(df_h)
                        combined.append(df_l)
                        logging.info(f"Cargado desde cache: {tag} (H y L)")
                    elif os.path.exists(single_file):
                        df_single = pd.read_csv(
                            single_file,
                            parse_dates=["timeStamp"],
                            index_col="timeStamp",
                        )
                        combined.append(df_single)
                        logging.info(f"Cargado desde cache: {tag}")

                if combined:
                    combined_df = pd.concat(combined, axis=1)
                    logging.info(
                        f"Datos combinados desde cache: {len(combined_df)} filas, {len(combined_df.columns)} columnas"
                    )
                    return combined_df, []
                else:
                    logging.warning("No se pudo cargar ningún archivo del cache")
                    return None, []
            else:
                logging.warning(
                    "Cache incompleto: faltan %d archivos de %d tags del filtro '%s'",
                    len(missing_tags),
                    len(tags),
                    filter_prefix or "ALL",
                )
                logging.info(
                    "Archivos faltantes: %s",
                    missing_tags[:5] if len(missing_tags) > 5 else missing_tags,
                )
                logging.info("Descargando todos los tags del filtro desde la API...")
        else:
            logging.info("cached=False: descargando datos desde API (ignorando cache)")
    else:
        tags = None

    # Obtener tags disponibles en la vista
    logging.info("Solicitando listado de tags desde la vista %s", vista)
    try:
        uids_df = api.get_Tags_from_vista(vista)
    except Exception as e:
        logging.exception("Error obteniendo tags desde la vista: %s", e)
        raise

    # Construir mapa tag -> uid
    tag_uid_map = {}
    for index, row in uids_df.iterrows():
        for element in row.get("columns", []):
            name = element.get("name")
            uid = element.get("uid")
            if name and uid:
                tag_uid_map[name] = uid

    # Directorio de salida
    out_dir = os.path.join(os.path.dirname(__file__), "minute_data")
    os.makedirs(out_dir, exist_ok=True)

    combined = []
    missing = []

    if use_all:
        logging.info(
            "Filter vacío en config: se descargarán todos los tags de la vista"
        )
        tags = sorted(tag_uid_map.keys())

    for tag in tags:
        # The API stores tag names with prefix 'CL_CAT_'. Try direct match first,
        # then try with the prefix. Keep the original `tag` as label/filename.
        request_name = tag
        uid = tag_uid_map.get(request_name)
        if not uid:
            prefixed = f"CL_CAT_{tag}"
            uid = tag_uid_map.get(prefixed)
            if uid:
                request_name = prefixed

        if not uid:
            # Also handle case where tags file already contains prefixed names
            if tag.startswith("CL_CAT_") and tag in tag_uid_map:
                uid = tag_uid_map.get(tag)
                request_name = tag

        if not uid:
            logging.warning("Tag no encontrado en vista: %s", tag)
            missing.append(tag)
            continue

        logging.info(
            "Descargando datos minutales para %s (request_name=%s uid=%s)",
            tag,
            request_name,
            uid,
        )

        params = {
            "dataSource": "RAW",
            "resolution": resolution,
            "uids": [uid],
            "startTs": datetime.timestamp(
                datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            ),
            "endTs": datetime.timestamp(datetime.strptime(end, "%Y-%m-%d %H:%M:%S")),
        }

        # Use the 'tagviews' historic endpoint: the path expects the view UID
        url = f"{base_url}/Documents/tagviews/{vista}/historic"
        try:
            resp = api.HEADERS and api.HEADERS or headers
            import requests

            response = requests.post(url, json=params, headers=resp, verify=False)
            response.raise_for_status()
            data = pd.json_normalize(response.json())
            if data.empty:
                logging.info("No hay datos para %s", tag)
                continue

            if "timeStamp" in data.columns and "value" in data.columns:
                df = data.set_index("timeStamp")[["value"]]
                df.index = pd.to_datetime(df.index, unit="s")
                df.rename(columns={"value": tag}, inplace=True)
            else:
                # intentar detectar columna de valor
                val_cols = [c for c in data.columns if c.lower() in ("value", "valor")]
                if val_cols:
                    df = data.set_index("timeStamp")[[val_cols[0]]]
                    df.index = pd.to_datetime(df.index, unit="s")
                    df.rename(columns={val_cols[0]: tag}, inplace=True)
                else:
                    logging.warning(
                        "Respuesta inesperada para %s, columnas: %s", tag, data.columns
                    )
                    continue

            # Guardar CSV por tag
            out_file = os.path.join(out_dir, f"{tag}.csv")
            df.to_csv(out_file, index=True)
            combined.append(df)
        except Exception as e:
            logging.exception("Error al descargar datos para %s: %s", tag, e)

    # Combinar y guardar
    combined_df = None
    if combined:
        combined_df = pd.concat(combined, axis=1)
        
        # Limpiar índice antes de guardar
        if isinstance(combined_df.index, pd.DatetimeIndex):
            # Eliminar filas con timestamps inválidos (NaT)
            combined_df = combined_df[combined_df.index.notnull()]
            # Resetear índice a string para evitar problemas de formato
            combined_df = combined_df.reset_index()
            combined_df.rename(columns={'index': 'timeStamp'}, inplace=True)
        
        combined_out = os.path.join(out_dir, "all_minutes.csv")
        combined_df.to_csv(combined_out, index=False)
        logging.info("Datos combinados guardados en %s", combined_out)

    if missing:
        logging.warning("Se encontraron tags faltantes: %s", missing)

    logging.info("Proceso completado.")
    return combined_df, missing


if __name__ == "__main__":
    download_minute_data()
