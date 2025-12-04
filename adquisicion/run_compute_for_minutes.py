import json
import logging
import os
import sys
from datetime import datetime

import pandas as pd

# Ajustar el path para importar el submódulo y utilidades
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Importar la API del submódulo
from CAT_Conexions.src.conexions import apiSagedCAT
from download_minute_data import download_minute_data

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# Leer configuración (ajusta el nombre si usas otro archivo)
CONFIG_PATH = os.path.join(ROOT, "consums_config.json")
with open(CONFIG_PATH, "r") as f:
    cfg = json.load(f)

api_cfg = cfg.get("api", {})
vista = api_cfg.get("vista")
token = api_cfg.get("nexustoken")
filter_prefix = None
for task in cfg.get("tasks", []):
    if task.get("name") == "fetch_api_data":
        filter_prefix = task.get("filter")
        break

headers = {"nexustoken": token, "Content-Type": "application/json"} if token else None
api = apiSagedCAT(vista=vista, headers=headers)

# Parámetros de consulta
start = cfg.get("period", {}).get("start", "2025-01-01 00:00:00")
end = cfg.get("period", {}).get("end", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
resolution = "RES_1_MIN"

logging.info(
    f"Fetching minute totalizer data {start} -> {end} (filter: {filter_prefix})"
)
combined_df, missing = download_minute_data()
if combined_df is None or combined_df.empty:
    logging.error("No minute data downloaded or combined dataframe is empty")
    sys.exit(1)
df = combined_df.copy()

# Asegurar índice datetime
if not isinstance(df.index, pd.DatetimeIndex):
    for cand in ("data", "timeStamp", "timestamp"):
        if cand in df.columns:
            df.index = pd.to_datetime(df[cand], errors="coerce")
            break

print("\nMinute totalizer data:")
print(df.head())

# Aquí puedes continuar con el procesamiento, por ejemplo, combinar 16/32 bits o calcular consumos
# from processing import combine_16_to_32, compute_consumption
# df_comb = combine_16_to_32(df)
# ...
# ...
