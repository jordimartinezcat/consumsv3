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
# Combinar pares TOT_H / TOT_L en una sola columna TOT
def combine_tot_high_low(df):
    cols = list(df.columns)
    combined = df.copy()
    processed = set()
    for col in cols:
        if col in processed:
            continue
        if col.endswith('_TOT_H'):
            base = col[:-6]  # remove _TOT_H
            low_col = base + '_TOT_L'
            tot_col = base + '_TOT'
            if low_col in combined.columns:
                # Combine high and low 16-bit parts into 32-bit unsigned int
                import numpy as np
                high_s = pd.to_numeric(combined[col], errors='coerce').fillna(0)
                low_s = pd.to_numeric(combined[low_col], errors='coerce').fillna(0)
                # Convert to numpy int64 arrays for bit ops
                high_arr = high_s.to_numpy(dtype='int64')
                low_arr = low_s.to_numpy(dtype='int64')
                # Mask low to 16 bits and shift high
                result_arr = (high_arr << 16) | (low_arr & 0xFFFF)
                combined[tot_col] = pd.Series(result_arr, index=combined.index)
                processed.add(col)
                processed.add(low_col)
                # Optionally drop the original H/L columns
                try:
                    combined.drop(columns=[col, low_col], inplace=True)
                except Exception:
                    pass
    return combined


df = combine_tot_high_low(df)

# Regla de calidad: rect_0 -> si el TOT calculado es 0, reemplazar por último valor válido (>0)
def apply_rect_0(df):
    # Remove any previous rect columns to avoid duplicates
    existing_rect_cols = [c for c in df.columns if c.endswith('_rect_0') or c == 'rect_0']
    rected = df.copy()
    if existing_rect_cols:
        rected = rected.drop(columns=existing_rect_cols)

    tot_cols = [c for c in rected.columns if c.endswith('_TOT')]

    for col in tot_cols:
        rect_col = f"{col}_rect_0"
        s = pd.to_numeric(rected[col], errors='coerce')

        # previous and next values
        prev = s.shift(1)
        nxt = s.shift(-1)

        # invalid if exactly zero OR the new rule:
        # if current < previous AND (next < current OR next == 0), then current is invalid
        is_zero = s == 0
        rule = (s < prev) & ((nxt < s) | (nxt == 0))
        invalid = is_zero | rule

        # Mask invalid points to NaN then forward-fill using last valid
        s_masked = s.where(~invalid)
        s_filled = s_masked.ffill()
        s_filled = s_filled.fillna(0).astype('int64')
        rected[rect_col] = s_filled

    return rected


df = apply_rect_0(df)

# Guardar CSV con separador ';' y decimales ',' si está habilitado en config
save_task = next((t for t in cfg.get('tasks', []) if t.get('name') == 'save_to_csv'), None)
if save_task and save_task.get('enabled'):
    out_dir = save_task.get('output_dir') or os.path.join(ROOT, 'adquisicion', 'minute_data')
    os.makedirs(out_dir, exist_ok=True)
    filename = save_task.get('filename') or f"all_minutes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    out_path = os.path.join(out_dir, filename)
    # Pandas accepts decimal=',' and sep=';'
    df.to_csv(out_path, sep=';', decimal=',', index=True)
    logging.info('Saved combined dataset to %s', out_path)
    
