"""
Script alternativo para guardar en MSSQL usando DELETE + INSERT
en lugar de MERGE para evitar problemas de claves duplicadas.
"""
import sys
import os
import logging
from datetime import datetime, timedelta

ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

from persistencia.save_daily_to_sqlserver import (
    get_db_connection,
    get_sqlserver_connection,
    extract_counter_id,
    get_all_consumptions_batch,
    get_all_totalizers_batch
)
import json
import math
from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(message)s"
)

def save_with_delete_insert(target_date='2026-07-01'):
    """
    Actualizar campo Totalitz en SQL Server para contadores del día especificado.
    Los consumos ya están insertados, solo se actualiza el totalizador.
    EXCLUYE los 10 contadores especiales que ya fueron procesados por update_from_old.
    """
    print(f"\n=== Actualizando Totalitz para: {target_date} ===\n")
    
    # Los 10 especiales que NO deben insertarse aquí
    CONTADORES_ESPECIALES = ['K3DP', 'LTT07', 'PAT09', 'K1T', 'LTT04', 
                             'PAT03', 'VVD02', 'VVT02', 'PBT09', 'RRD02']
    
    # Cargar configuración
    CONFIG_PATH = os.path.join(ROOT, "consums_config.json")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    
    # Conectar
    logging.info("Conectando a PostgreSQL...")
    pg_engine = get_db_connection(cfg)
    
    logging.info("Conectando a SQL Server...")
    sqlserver_conn = get_sqlserver_connection(cfg)
    cursor = sqlserver_conn.cursor()
    
    # Obtener señales y mapeo
    logging.info("Obteniendo lista de señales...")
    query_signals = text("""
        SELECT DISTINCT t.tag
        FROM ga_landing.ite_consums_tags t
        WHERE t.tag LIKE '%_CSM'
        ORDER BY t.tag
    """)
    
    with pg_engine.connect() as conn:
        result = conn.execute(query_signals)
        signals = [row[0] for row in result.fetchall()]
    
    logging.info(f"Señales encontradas: {len(signals)}")
    
    # Mapeo via tagOld: tag_csm -> id_contador
    query_mapeo = text("""
        SELECT t.tag, ic."Id"
        FROM ga_landing.ite_consums_tags t
        INNER JOIN ga_landing.ite_comptadors ic ON t."tagOld" = ic."Id"
        WHERE t."tagOld" IS NOT NULL
          AND t."tagOld" != ''
          AND t.tag LIKE '%_CSM'
    """)
    
    with pg_engine.connect() as conn:
        result = conn.execute(query_mapeo)
        comptadors_mapeo = {row[0]: row[1] for row in result.fetchall()}
    
    logging.info(f"Mapeo Comptadors: {len(comptadors_mapeo)} registros")
    
    # Filtrar señales con mapeo
    signals_with_mapping = [s for s in signals if s in comptadors_mapeo]
    
    logging.info(f"Señales con mapeo: {len(signals_with_mapping)}")
    
    # Obtener datos en batch
    logging.info("Obteniendo consumos...")
    consumptions = get_all_consumptions_batch(pg_engine, signals_with_mapping, target_date)
    logging.info(f"Consumos obtenidos: {len(consumptions)}")
    
    # Obtener totalizadores - Mapear CSM -> TOT via idTag
    logging.info("Obteniendo totalizadores...")
    
    # Paso 1: Obtener idTag para cada signal_csm
    query_idtag_csm = text("""
        SELECT tag, "idTag"
        FROM ga_landing.ite_consums_tags
        WHERE tag = ANY(:tags)
    """)
    
    csm_to_idtag = {}
    with pg_engine.connect() as conn:
        result = conn.execute(query_idtag_csm, {'tags': signals_with_mapping})
        for row in result:
            csm_to_idtag[row[0]] = row[1]
    
    logging.info(f"Mapeo CSM -> idTag: {len(csm_to_idtag)} registros")
    
    # Paso 2: Obtener tag TOT para cada idTag (sin prefijo CL_CAT_)
    idtags_to_search = list(csm_to_idtag.values())
    query_idtag_tot = text("""
        SELECT "idTag", tag
        FROM ga_landing.cfg_tags
        WHERE "idTag" = ANY(:idtags)
          AND tag LIKE '%_TOT'
          AND tag NOT LIKE '%_CSM'
    """)
    
    idtag_to_tot = {}
    with pg_engine.connect() as conn:
        result = conn.execute(query_idtag_tot, {'idtags': idtags_to_search})
        for row in result:
            idtag_to_tot[row[0]] = row[1]
    
    logging.info(f"Mapeo idTag -> TOT: {len(idtag_to_tot)} registros")
    
    # Paso 3: Construir mapeo CSM -> TOT (añadiendo prefijo CL_CAT_)
    csm_to_tot = {}
    for signal_csm, idtag in csm_to_idtag.items():
        tot_signal = idtag_to_tot.get(idtag)
        if tot_signal:
            # Añadir prefijo CL_CAT_ para buscar en ite_consums_totalitzadors
            tot_signal_with_prefix = f"CL_CAT_{tot_signal}"
            csm_to_tot[signal_csm] = tot_signal_with_prefix
    
    logging.info(f"Mapeo CSM -> TOT: {len(csm_to_tot)} registros")
    
    # Paso 4: Buscar totalizadores por nombre TOT (con prefijo CL_CAT_)
    tot_signals = list(csm_to_tot.values())
    query_totalizers = text("""
        SELECT tag, totalizador_00h
        FROM ga_landing.ite_consums_totalitzadors
        WHERE tag = ANY(:tags) AND fecha = :fecha
    """)
    
    totalizers_by_tot = {}
    with pg_engine.connect() as conn:
        result = conn.execute(query_totalizers, {
            'tags': tot_signals,
            'fecha': target_date
        })
        for row in result:
            totalizers_by_tot[row[0]] = float(row[1]) if row[1] is not None else None
    
    logging.info(f"Totalizadores obtenidos: {len(totalizers_by_tot)} registros")
    
    # Paso 5: Crear mapeo final CSM -> totalizador_valor
    totalizers_by_csm = {}
    for signal_csm, tot_signal in csm_to_tot.items():
        totalizer_value = totalizers_by_tot.get(tot_signal)
        if totalizer_value is not None:
            totalizers_by_csm[signal_csm] = totalizer_value
    
    logging.info(f"Totalizadores mapeados a CSM: {len(totalizers_by_csm)} registros")
    
    # Obtener niveles de Consums_dia_old
    logging.info("Obteniendo niveles de Consums_dia_old...")
    niveles_query = """
        SELECT Id, Nivell
        FROM Consums_dia_old
        WHERE Data = ?
    """
    cursor.execute(niveles_query, (target_date,))
    niveles_by_id = {row[0]: row[1] for row in cursor.fetchall()}
    logging.info(f"Niveles obtenidos: {len(niveles_by_id)}")
    
    # Preparar registros
    records_to_insert = []
    date_obj = datetime.strptime(target_date, '%Y-%m-%d')
    
    for signal_csm in signals_with_mapping:
        counter_id = comptadors_mapeo.get(signal_csm)
        
        if not counter_id:
            continue
        
        consumption = consumptions.get(signal_csm)
        
        # Obtener totalizador usando mapeo CSM -> TOT -> valor
        totalizer = totalizers_by_csm.get(signal_csm)
        
        # Obtener nivel de Consums_dia_old
        nivel = niveles_by_id.get(counter_id)
        
        # Usar 0 si None
        if totalizer is None:
            totalizer = 0.0
        if consumption is None:
            consumption = 0.0
        if nivel is None:
            nivel = 0.0
        
        # Validar NaN
        if math.isnan(totalizer):
            totalizer = 0.0
        if math.isnan(consumption):
            consumption = 0.0
        if math.isnan(nivel):
            nivel = 0.0
        
        records_to_insert.append({
            'id': counter_id,
            'date': date_obj,
            'consumption': consumption,
            'totalizer': totalizer,
            'nivel': nivel
        })
    
    # Filtrar los 10 especiales
    records_to_insert = [rec for rec in records_to_insert 
                        if rec['id'] not in CONTADORES_ESPECIALES]
    
    logging.info(f"Registros a guardar (sin especiales): {len(records_to_insert)}")
    
    # Verificar duplicados en los datos a insertar
    counter_ids = [rec['id'] for rec in records_to_insert]
    if len(counter_ids) != len(set(counter_ids)):
        logging.warning("¡ADVERTENCIA! Hay IDs duplicados en los datos a insertar")
        # Eliminar duplicados manteniendo el primero
        seen = set()
        records_to_insert = [rec for rec in records_to_insert 
                            if not (rec['id'] in seen or seen.add(rec['id']))]
        logging.info(f"Después de eliminar duplicados: {len(records_to_insert)} registros")
    
    # UPDATE del campo Totalitz (no borrar, los consumos ya están)
    if not records_to_insert:
        logging.warning("No hay registros para actualizar")
        cursor.close()
        sqlserver_conn.close()
        pg_engine.dispose()
        return 0
    
    update_sql = """
        UPDATE Consums_dia
        SET Totalitz = ?
        WHERE Id = ? AND Data = ?
    """
    
    logging.info(f"Actualizando campo Totalitz para {len(records_to_insert)} contadores del {target_date}...")
    data_tuples = [
        (rec['totalizer'], rec['id'], rec['date'])
        for rec in records_to_insert
    ]
    
    cursor.executemany(update_sql, data_tuples)
    sqlserver_conn.commit()
    
    updated = cursor.rowcount
    logging.info(f"✓ Registros actualizados: {updated}")
    
    cursor.close()
    sqlserver_conn.close()
    pg_engine.dispose()
    
    print(f"\n=== Resumen ===")
    print(f"Fecha: {target_date}")
    print(f"Actualizados: {updated}")
    print("===============\n")
    
    return updated

if __name__ == "__main__":
    save_with_delete_insert('2026-07-01')
