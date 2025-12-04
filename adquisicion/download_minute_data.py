import os
import sys
import json
import logging
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

    headers = {"nexustoken": nexustoken, "Content-Type": "application/json"} if nexustoken else None
    api = apiSagedCAT(vista=vista, headers=headers)

    period = cfg.get("period", {})
    start = period.get("start") or datetime.now().strftime("%Y-%m-%d 00:00:00")
    end = period.get("end") or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    resolution = "RES_1_MIN"

    # Leer señales preparadas
    signals_file = os.path.join(os.path.dirname(__file__), "senales_para_descarga.txt")

    # Determinar comportamiento según filtro en la config
    filter_prefix = None
    for task in cfg.get('tasks', []):
        if task.get('name') == 'fetch_api_data':
            filter_prefix = task.get('filter')
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

        # Aplicar filtro de configuración sobre la lista de señales (subcadena)
        if filter_prefix:
            orig_count = len(tags)
            tags = [t for t in tags if filter_prefix in t]
            logging.info("Filtro '%s' aplicado a señales: %d -> %d", filter_prefix, orig_count, len(tags))
            if not tags:
                logging.warning("No hay señales que contengan '%s' en %s", filter_prefix, signals_file)
                return None, []
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
        for element in row.get('columns', []):
            name = element.get('name')
            uid = element.get('uid')
            if name and uid:
                tag_uid_map[name] = uid

    # Directorio de salida
    out_dir = os.path.join(os.path.dirname(__file__), "minute_data")
    os.makedirs(out_dir, exist_ok=True)

    combined = []
    missing = []

    if use_all:
        logging.info("Filter vacío en config: se descargarán todos los tags de la vista")
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

        logging.info("Descargando datos minutales para %s (request_name=%s uid=%s)", tag, request_name, uid)

        params = {
            "dataSource": "RAW",
            "resolution": resolution,
            "uids": [uid],
            "startTs": datetime.timestamp(datetime.strptime(start, '%Y-%m-%d %H:%M:%S')),
            "endTs": datetime.timestamp(datetime.strptime(end, '%Y-%m-%d %H:%M:%S')),
        }

        # Use the 'tagviews' historic endpoint: the path expects the view UID
        url = f"{base_url}/Documents/tagviews/{vista}/historic"
        try:
            resp = api.HEADERS and api.HEADERS or headers
            import requests
            response = requests.post(url, json=params, headers=resp)
            response.raise_for_status()
            data = pd.json_normalize(response.json())
            if data.empty:
                logging.info("No hay datos para %s", tag)
                continue

            if 'timeStamp' in data.columns and 'value' in data.columns:
                df = data.set_index('timeStamp')[['value']]
                df.index = pd.to_datetime(df.index, unit='s')
                df.rename(columns={'value': tag}, inplace=True)
            else:
                # intentar detectar columna de valor
                val_cols = [c for c in data.columns if c.lower() in ('value', 'valor')]
                if val_cols:
                    df = data.set_index('timeStamp')[[val_cols[0]]]
                    df.index = pd.to_datetime(df.index, unit='s')
                    df.rename(columns={val_cols[0]: tag}, inplace=True)
                else:
                    logging.warning("Respuesta inesperada para %s, columnas: %s", tag, data.columns)
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
        combined_out = os.path.join(out_dir, "all_minutes.csv")
        combined_df.to_csv(combined_out, index=True)
        logging.info("Datos combinados guardados en %s", combined_out)

    if missing:
        logging.warning("Se encontraron tags faltantes: %s", missing)

    logging.info("Proceso completado.")
    return combined_df, missing


if __name__ == '__main__':
    download_minute_data()
