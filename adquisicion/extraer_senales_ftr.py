import json
import logging
import sys
import os
import pandas as pd

# Añadir la raíz del proyecto y CAT_Conexions al path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "CAT_Conexions", "src"))
from conexions import pgDataLake

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# Leer configuración
def get_filter_from_config(cfg):
    for task in cfg.get("tasks", []):
        if task.get("name") == "fetch_api_data":
            return task.get("filter")
    return None

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "consums_config.json")
with open(CONFIG_PATH, "r") as f:
    cfg = json.load(f)

filter_prefix = get_filter_from_config(cfg)
pg = pgDataLake()
descarga_tags = []
if filter_prefix:
    prefix = f'{filter_prefix}_FTR'
else:
    prefix = 'FTR'
if filter_prefix:
    # Buscar directamente TOT_L y TOT_H dentro del prefijo
    query_tot_lh = f"""
    SELECT * FROM ga_landing.ite_sql4_cfg_tags
    WHERE tag LIKE '{prefix}%' AND (tag LIKE '%TOT\\_L' ESCAPE '\\' OR tag LIKE '%TOT\\_H' ESCAPE '\\') AND tag NOT LIKE 'ET%' AND tag NOT LIKE '%\\_LS\\_%' ESCAPE '\\' AND tag NOT LIKE '%\\_P\\_%' ESCAPE '\\'
    """
    # Buscar el resto de TOT (para posibles señales TOT sin sufijos _L/_H)
    query_tot = f"""
    SELECT * FROM ga_landing.ite_sql4_cfg_tags
    WHERE tag LIKE '{prefix}%' AND tag LIKE '%TOT' AND tag NOT LIKE 'ET%' AND tag NOT LIKE '%\\_LS\\_%' ESCAPE '\\' AND tag NOT LIKE '%\\_P\\_%' ESCAPE '\\'
    """
else:
    query_tot_lh = f"""
    SELECT * FROM ga_landing.ite_sql4_cfg_tags
    WHERE tag LIKE '%FTR%' AND (tag LIKE '%TOT\\_L' ESCAPE '\\' OR tag LIKE '%TOT\\_H' ESCAPE '\\') AND tag NOT LIKE 'ET%' AND tag NOT LIKE '%\\_LS\\_%' ESCAPE '\\' AND tag NOT LIKE '%\\_P\\_%' ESCAPE '\\'
    """
    query_tot = f"""
    SELECT * FROM ga_landing.ite_sql4_cfg_tags
    WHERE tag LIKE '%FTR%' AND tag LIKE '%TOT' AND tag NOT LIKE 'ET%' AND tag NOT LIKE '%\\_LS\\_%' ESCAPE '\\' AND tag NOT LIKE '%\\_P\\_%' ESCAPE '\\'
    """

logging.info(f'Ejecutando consulta TOT_L/H: {query_tot_lh}')
df_tot_lh = pg.get_data(query_tot_lh)

logging.info(f'Ejecutando consulta TOT general: {query_tot}')
df_tot = pg.get_data(query_tot)

# Prefijos de las señales que ya tienen TOT_L o TOT_H (primeros 5 caracteres)
tot_lh_prefixes = set(tag[:5] for tag in df_tot_lh['tag']) if not df_tot_lh.empty else set()

# Añadir directamente las señales TOT_L y TOT_H encontradas
if not df_tot_lh.empty:
    print('Señales FTR que terminan en TOT_L o TOT_H:')
    print(df_tot_lh['tag'])
    for tag in df_tot_lh['tag']:
        descarga_tags.append(tag)
else:
    print('No se encontraron señales que terminen en TOT_L o TOT_H.')

# De df_tot (todas las señales %TOT) conservar sólo aquellas cuyos
# primeros 5 caracteres NO estén en tot_lh_prefixes
if not df_tot.empty:
    print('\nSeñales FTR que terminan en TOT (filtradas por prefijo TOT_L/H):')
    filtered_tot = []
    for tag in df_tot['tag']:
        if tag[:5] not in tot_lh_prefixes:
            filtered_tot.append(tag)
    print(filtered_tot)
    for tag in filtered_tot:
        descarga_tags.append(tag)
else:
    print('No se encontraron señales FTR especiales que terminan en TOT.')

# Eliminar duplicados manteniendo el orden
descarga_tags = list(dict.fromkeys(descarga_tags))

output_path = os.path.join(os.path.dirname(__file__), 'senales_para_descarga.txt')
with open(output_path, 'w', encoding='utf-8') as out_f:
    for t in descarga_tags:
        out_f.write(f"{t}\n")

print(f"\nSe han escrito {len(descarga_tags)} señales en: {output_path}")
