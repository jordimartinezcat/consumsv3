# -*- coding: utf-8 -*-
"""
Modulo para guardar datos diarios en SQL Server.

Guarda resumen diario de cada contador en la tabla Consums_dia:
- Id: Codigo corto del contador (ej: B11, BPD06)
- Data: Fecha del dia
- Consum: Consumo total del dia desde ite_v_consums_24h
- Totalitz: Valor del totalizador a las 00:00 desde API
- Validat, Nivell, especial: NULL (campos opcionales)

Estrategia: DELETE + INSERT (borra registros del día y los vuelve a insertar)
Nota: Anteriormente usaba MERGE pero causaba conflictos de clave duplicada.
Se ejecuta despues de la validacion diaria.
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import pyodbc  # pip install pyodbc
from sqlalchemy import text

# Ajustar path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

# Import CAT_Conexions
sys.path.insert(0, os.path.join(ROOT, "CAT_Conexions", "src"))
from conexions import apiSagedCAT

from persistencia.db_connection import get_db_connection
from persistencia.save_totalizers import get_totalizer_for_date

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def get_sqlserver_connection(cfg=None):
    """
    Crear conexion a SQL Server usando autenticacion Windows (Trusted Connection).
    
    Configuracion en consums_config.json:
    {
      "sqlserver": {
        "host": "servercmp",
        "port": 1433,
        "database": "Consums",
        "table": "Consums_dia"
      }
    }
    
    Args:
        cfg (dict): Configuracion con seccion 'sqlserver'
    
    Returns:
        pyodbc.Connection: Conexion SQL Server
    """
    if cfg is None:
        CONFIG_PATH = os.path.join(ROOT, "consums_config.json")
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    
    sqlserver_config = cfg.get("sqlserver", {})
    
    if not sqlserver_config:
        raise ValueError("Falta configuracion 'sqlserver' en consums_config.json")
    
    host = sqlserver_config.get("host", "servercmp")
    port = sqlserver_config.get("port", 1433)
    database = sqlserver_config.get("database", "Consums")
    
    logging.info(f"Conectando a SQL Server: {host}:{port}/{database}")
    logging.info("Usando autenticacion Windows (Trusted Connection)")
    
    # Connection string con autenticacion Windows
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"  # Autenticacion Windows
    )
    
    try:
        conn = pyodbc.connect(conn_str)
        logging.info("[OK] Conexion SQL Server exitosa")
        return conn
    except pyodbc.Error as e:
        # Intentar con driver alternativo si falla
        logging.warning(f"Error con ODBC Driver 17, intentando con ODBC Driver 18...")
        conn_str_alt = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"  # Para SQL Server 2022+
        )
        try:
            conn = pyodbc.connect(conn_str_alt)
            logging.info("[OK] Conexion SQL Server exitosa (ODBC Driver 18)")
            return conn
        except pyodbc.Error as e2:
            raise Exception(f"No se pudo conectar a SQL Server: {e2}")


def extract_counter_id(tag_name):
    """
    Extraer el codigo corto del contador desde el tag completo.
    
    Logica: Extraer el codigo entre CL_CAT_ y _FTR_
    
    Ejemplos:
    - CL_CAT_B11_FTR_G01_TOT -> B11
    - CL_CAT_BPD06_FTR_T01_TOT -> BPD06
    - CL_CAT_ATD02_FTR_T01_TOT -> ATD02
    
    Args:
        tag_name (str): Nombre completo del tag
    
    Returns:
        str: ID corto del contador
    """
    parts = tag_name.split('_')
    
    # Buscar el codigo entre CL_CAT_ y _FTR_
    if len(parts) >= 4 and parts[0] == 'CL' and parts[1] == 'CAT':
        try:
            ftr_index = parts.index('FTR')
            # Todo entre CAT y FTR
            counter_parts = parts[2:ftr_index]
            counter_id = '_'.join(counter_parts)
            return counter_id
        except ValueError:
            # FTR no encontrado, tomar solo el siguiente a CAT
            return parts[2]
    
    # Fallback: intentar buscar patron sin CL_CAT_
    if '_FTR_' in tag_name:
        # Extraer todo antes de _FTR_
        pre_ftr = tag_name.split('_FTR_')[0]
        # Quitar prefijos comunes
        for prefix in ['CL_CAT_', 'CL_', 'CAT_']:
            if pre_ftr.startswith(prefix):
                return pre_ftr[len(prefix):]
        return pre_ftr
    
    # Si no sigue el patron esperado, devolver el tag completo
    logging.warning(f"No se pudo extraer ID de '{tag_name}', usando tag completo")
    return tag_name


def get_totalizer_from_pg(pg_engine, tag_name, date):
    """
    Obtener totalizador a las 00:00 del dia desde PostgreSQL.
    
    Los totalizadores se guardan durante la validacion diaria en la tabla
    ga_datalake.ite_totalizadores_diarios.
    
    Args:
        pg_engine: Engine de SQLAlchemy para PostgreSQL
        tag_name (str): Nombre de la señal (con o sin CL_CAT_)
        date (str): Fecha en formato 'YYYY-MM-DD'
    
    Returns:
        float: Valor del totalizador a las 00:00, o None si no existe
    """
    # Intentar con y sin prefijo CL_CAT_
    tags_to_try = [tag_name]
    if tag_name.startswith('CL_CAT_'):
        tags_to_try.append(tag_name.replace('CL_CAT_', ''))
    else:
        tags_to_try.append(f'CL_CAT_{tag_name}')
    
    for tag in tags_to_try:
        result = get_totalizer_for_date(pg_engine, tag, date)
        if result and result['totalizador_00h'] is not None:
            return result['totalizador_00h']
    
    return None


def get_all_consumptions_batch(pg_engine, tags_csm, date):
    """
    Obtener consumos de TODAS las señales en una sola query.
    
    Args:
        pg_engine: Engine de SQLAlchemy para PostgreSQL
        tags_csm: Lista de tags en formato CSM
        date: Fecha en formato 'YYYY-MM-DD'
    
    Returns:
        dict: {tag_csm: consumption_value}
    """
    if not tags_csm:
        return {}
    
    # Quitar prefijo CL_CAT_ de todos los tags
    tags_clean = [tag.replace('CL_CAT_', '') for tag in tags_csm]
    
    query = text("""
        SELECT 
            t.tag,
            SUM(c.valor) as total_consumption
        FROM ga_datalake.ite_v_consums_24h c
        INNER JOIN ga_landing.ite_consums_tags t ON c.idtag = t."idTag"
        WHERE t.tag = ANY(:tags)
          AND c.data >= (CAST(:date AS timestamp) AT TIME ZONE 'Europe/Madrid' AT TIME ZONE 'UTC')
          AND c.data < (CAST(:date AS timestamp) AT TIME ZONE 'Europe/Madrid' AT TIME ZONE 'UTC') + INTERVAL '1 day'
        GROUP BY t.tag
    """)
    
    result_dict = {}
    try:
        with pg_engine.connect() as conn:
            result = conn.execute(query, {
                'tags': tags_clean,
                'date': date
            })
            
            for row in result:
                result_dict[row[0]] = float(row[1]) if row[1] is not None else None
        
        return result_dict
    
    except Exception as e:
        logging.error(f"Error obteniendo consumos batch: {e}")
        return {}


def get_all_totalizers_batch(pg_engine, tags, date):
    """
    Obtener totalizadores de TODAS las señales en una sola query.
    
    Args:
        pg_engine: Engine de SQLAlchemy para PostgreSQL
        tags: Lista de tags
        date: Fecha en formato 'YYYY-MM-DD'
    
    Returns:
        dict: {tag: totalizador_00h}
    """
    if not tags:
        return {}
    
    query = text("""
        SELECT tag, totalizador_00h
        FROM ga_landing.ite_consums_totalitzadors
        WHERE tag = ANY(:tags) AND fecha = :fecha
    """)
    
    result_dict = {}
    try:
        with pg_engine.connect() as conn:
            result = conn.execute(query, {
                'tags': tags,
                'fecha': date
            })
            
            for row in result:
                result_dict[row[0]] = float(row[1]) if row[1] is not None else None
        
        return result_dict
    
    except Exception as e:
        logging.error(f"Error obteniendo totalizadores batch: {e}")
        return {}


def get_daily_consumption_from_pg(pg_engine, tag_csm, date):
    """
    Obtener consumo diario desde la vista ga_datalake.ite_v_consums_24h.
    
    Esta vista contiene los consumos diarios consolidados (24 horas).
    
    Args:
        pg_engine: Engine de SQLAlchemy para PostgreSQL
        tag_csm (str): Nombre del tag en formato CSM (puede tener prefijo CL_CAT_)
        date (str): Fecha en formato 'YYYY-MM-DD'
    
    Returns:
        float: Consumo del dia, o None si no existe
    """
    # Quitar prefijo CL_CAT_ si existe, ya que ite_consums_tags no lo tiene
    tag_csm_clean = tag_csm.replace('CL_CAT_', '')
    
    # Los datos están en UTC. Para buscar un día en Madrid (UTC+2 en verano):
    # 2026-06-30 en Madrid = 2026-06-29 22:00 UTC a 2026-06-30 22:00 UTC
    query = text("""
        SELECT SUM(c.valor) as total_consumption
        FROM ga_datalake.ite_v_consums_24h c
        INNER JOIN ga_landing.ite_consums_tags t ON c.idtag = t."idTag"
        WHERE t.tag = :tag_csm
          AND c.data >= (CAST(:date AS timestamp) AT TIME ZONE 'Europe/Madrid' AT TIME ZONE 'UTC')
          AND c.data < (CAST(:date AS timestamp) AT TIME ZONE 'Europe/Madrid' AT TIME ZONE 'UTC') + INTERVAL '1 day'
    """)
    
    try:
        with pg_engine.connect() as conn:
            result = conn.execute(query, {
                'tag_csm': tag_csm_clean,
                'date': date
            })
            row = result.fetchone()
            
            if row and row[0] is not None:
                return float(row[0])
            
            return None
    
    except Exception as e:
        logging.error(f"Error obteniendo consumo de PostgreSQL para {tag_csm} en {date}: {e}")
        return None


def get_comptadors_mapping(pg_engine):
    """
    Obtener mapeo tag_name -> Id desde PostgreSQL usando JOIN entre
    ite_consums_tags (tagOld) e ite_comptadors (Id).
    
    Para cada señal _TOT, busca su correspondiente _CSM en ite_consums_tags,
    obtiene el tagOld, y lo mapea con el Id de ite_comptadors.
    
    Args:
        pg_engine: SQLAlchemy engine para PostgreSQL
    
    Returns:
        dict: Mapeo {tag_name_TOT: Id_comptador}
    """
    query = text("""
        SELECT 
            REPLACE(tags.tag, '_CSM', '_TOT') as tag_tot,
            comp."Id" as id_comptador
        FROM ga_landing.ite_consums_tags tags
        INNER JOIN ga_landing.ite_comptadors comp 
            ON tags."tagOld" = comp."Id"
        WHERE tags.tag LIKE '%_CSM'
          AND tags."tagOld" IS NOT NULL
    """)
    
    try:
        mapping = {}
        with pg_engine.connect() as conn:
            result = conn.execute(query)
            for row in result.fetchall():
                tag_tot = row[0]
                id_comptador = row[1]
                mapping[tag_tot] = id_comptador
        
        logging.info(f"Mapeo Comptadors cargado desde PostgreSQL (via tagOld): {len(mapping)} registros")
        return mapping
    except Exception as e:
        logging.error(f"Error obteniendo mapeo de Comptadors: {e}")
        return {}


def get_tag_name_from_pg(pg_engine, tag_search):
    """
    Obtener el nombre del tag desde ga_landing.ite_consums_tags.
    
    Args:
        pg_engine: SQLAlchemy engine
        tag_search (str): Tag a buscar (con o sin CL_CAT_)
    
    Returns:
        str or None: Nombre del tag encontrado
    """
    query = text("""
        SELECT tag
        FROM ga_landing.ite_consums_tags
        WHERE tag = :tag
        LIMIT 1
    """)
    
    try:
        with pg_engine.connect() as conn:
            result = conn.execute(query, {'tag': tag_search})
            row = result.fetchone()
            if row:
                return row[0]
            return None
    except Exception as e:
        logging.error(f"Error obteniendo nombre de tag {tag_search}: {e}")
        return None


def normalize_tag_name(tag_name):
    """
    Normalizar nombre de tag para coincidir con IdMaximo de Comptadors.
    
    Proceso:
    1. Quitar sufijo _TOT o _CSM
    2. Quitar prefijo CL_CAT_ si existe
    3. Quitar todos los guiones bajos
    
    Ejemplo: "CL_CAT_B11_FTR_G01_TOT" -> "B11FTRG01"
    
    Args:
        tag_name (str): Nombre completo del tag
    
    Returns:
        str: Nombre normalizado
    """
    # Quitar prefijo CL_CAT_
    normalized = tag_name.replace('CL_CAT_', '')
    
    # Quitar sufijos _TOT, _CSM
    normalized = normalized.replace('_TOT', '').replace('_CSM', '')
    
    # Quitar todos los guiones bajos
    normalized = normalized.replace('_', '')
    
    return normalized


def save_daily_data_to_sqlserver(date=None, cfg=None):
    """
    Guardar datos diarios en SQL Server tabla Consums_dia.
    
    Para cada señal procesada:
    1. Obtener Id (codigo corto del contador)
    2. Obtener totalizador a las 00:00 desde API
    3. Obtener consumo del dia desde PostgreSQL (ite_v_consums_24h)
    4. DELETE registros existentes del día + INSERT nuevos
    
    Columnas de la tabla Consums_dia:
    - Id: Codigo contador (B11, BPD06, etc.)
    - Data: Fecha del dia
    - Consum: Consumo total del dia
    - Totalitz: Totalizador a las 00:00
    - Validat: NULL (no usado)
    - Nivell: NULL (no usado)
    - especial: NULL (no usado)
    
    Args:
        date (str): Fecha en formato 'YYYY-MM-DD'. Si None, usa ayer.
        cfg (dict): Configuracion
    
    Returns:
        int: Numero de registros insertados/actualizados
    """
    # Si no se especifica fecha, usar ayer
    if date is None:
        yesterday = datetime.now() - timedelta(days=1)
        date = yesterday.strftime('%Y-%m-%d')
    
    logging.info(f"=== Guardado SQL Server para fecha: {date} ===")
    
    # Cargar configuracion
    if cfg is None:
        CONFIG_PATH = os.path.join(ROOT, "consums_config.json")
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    
    # Obtener configuracion API
    api_config = cfg.get("api", {})
    vista = api_config.get("vista")
    nexustoken = api_config.get("nexustoken")
    base_url = api_config.get("base_url")
    
    if not all([vista, nexustoken, base_url]):
        raise ValueError("Falta configuracion API en consums_config.json")
    
    # Obtener configuracion SQL Server
    sqlserver_config = cfg.get("sqlserver", {})
    table_name = sqlserver_config.get("table", "Consums_dia")
    
    # Conectar a PostgreSQL
    logging.info("Conectando a PostgreSQL...")
    pg_engine = get_db_connection(cfg)
    
    # Conectar a SQL Server
    logging.info("Conectando a SQL Server...")
    sqlserver_conn = get_sqlserver_connection(cfg)
    cursor = sqlserver_conn.cursor()
    
    # Obtener lista de señales procesadas desde PostgreSQL
    logging.info("Obteniendo lista de señales...")
    query_signals = text("""
        SELECT DISTINCT t.tag
        FROM ga_landing.ite_sql4_cfg_tags t
        WHERE t.tag LIKE '%_TOT'
          AND t.tag NOT LIKE 'ET%'
          AND t.tag NOT LIKE '%_LS_%'
          AND t.tag NOT LIKE '%_P_%'
        ORDER BY t.tag
    """)
    
    signals = []
    with pg_engine.connect() as conn:
        result = conn.execute(query_signals)
        signals = [row[0] for row in result.fetchall()]
    
    logging.info(f"Senales encontradas: {len(signals)}")
    
    # Obtener mapeo IdMaximo -> Id de la tabla ite_comptadors de PostgreSQL
    comptadors_mapping = get_comptadors_mapping(pg_engine)
    if not comptadors_mapping:
        logging.warning("No se encontro mapeo en ite_comptadors. No se procesara ninguna señal.")
        sqlserver_conn.close()
        return 0
    
    # Filtrar señales que tienen mapeo
    signals_with_mapping = [s for s in signals if s in comptadors_mapping]
    signals_no_mapping = len(signals) - len(signals_with_mapping)
    
    logging.info(f"Señales con mapeo: {len(signals_with_mapping)}")
    logging.info(f"Obteniendo datos en BATCH...")
    
    # CORRECCIÓN: Los totalizadores se guardaron con prefijo CL_CAT_, pero el mapeo no lo tiene
    # Agregar el prefijo antes de buscar los totalizadores
    signals_with_prefix = ['CL_CAT_' + tag for tag in signals_with_mapping]
    
    # OPTIMIZACIÓN: Obtener TODOS los totalizadores en una sola query
    totalizers_batch = get_all_totalizers_batch(pg_engine, signals_with_prefix, date)
    
    # Crear diccionario sin prefijo para facilitar el acceso posterior
    totalizers_batch_no_prefix = {
        tag.replace('CL_CAT_', ''): value 
        for tag, value in totalizers_batch.items()
    }
    
    # OPTIMIZACIÓN: Obtener TODOS los consumos en una sola query
    tags_csm = [tag.replace('_TOT', '_CSM') for tag in signals_with_mapping]
    consumptions_batch = get_all_consumptions_batch(pg_engine, tags_csm, date)
    
    logging.info(f"Totalizadores obtenidos: {len(totalizers_batch_no_prefix)}")
    logging.info(f"Consumos obtenidos: {len(consumptions_batch)}")
    
    # Preparar datos para insert batch
    records_to_insert = []
    records_processed = 0
    records_skipped = 0
    
    import math
    from datetime import datetime as dt
    date_obj = dt.strptime(date, '%Y-%m-%d')
    
    for tag_name in signals_with_mapping:
        counter_id = comptadors_mapping[tag_name]
        tag_csm = tag_name.replace('_TOT', '_CSM')
        
        # Obtener datos del batch
        totalizer = totalizers_batch_no_prefix.get(tag_name)
        consumption = consumptions_batch.get(tag_csm)
        
        # Verificar si tenemos datos
        if totalizer is None and consumption is None:
            records_skipped += 1
            continue
        
        # Usar 0 si alguno es None
        if totalizer is None:
            totalizer = 0.0
        if consumption is None:
            consumption = 0.0
        
        # Validar NaN
        if math.isnan(totalizer):
            totalizer = 0.0
        if math.isnan(consumption):
            consumption = 0.0
        
        records_to_insert.append({
            'id': counter_id,
            'date': date_obj,
            'consumption': consumption,
            'totalizer': totalizer
        })
        records_processed += 1
    
    logging.info(f"Preparados {len(records_to_insert)} registros para insertar")
    
    # OPTIMIZACIÓN: Insertar todos en batch usando executemany
    if records_to_insert:
        # PASO 1: DELETE preventivo para evitar conflictos de clave duplicada
        # Eliminar registros existentes del día antes de hacer MERGE
        delete_sql = f"""
        DELETE FROM {table_name}
        WHERE CAST(Data AS DATE) = ?
        """
        
        try:
            # Borrar registros del día
            cursor.execute(delete_sql, (date_obj.strftime('%Y-%m-%d'),))
            deleted_count = cursor.rowcount
            sqlserver_conn.commit()
            logging.info(f"Borrados {deleted_count} registros existentes del {date_obj.strftime('%Y-%m-%d')}")
        except Exception as e:
            logging.warning(f"No se pudieron borrar registros existentes: {e}")
            sqlserver_conn.rollback()
        
        # PASO 2: INSERT directo (ya no necesitamos MERGE porque borramos antes)
        insert_sql = f"""
        INSERT INTO {table_name} (Id, Data, Consum, Totalitz, Validat, Nivell, Especial)
        VALUES (?, ?, ?, ?, 0, 0, 0)
        """
        
        try:
            # Preparar los datos para executemany
            data_tuples = []
            for rec in records_to_insert:
                data_tuples.append((
                    rec['id'],
                    rec['date'].strftime('%Y-%m-%d %H:%M:%S'),
                    rec['consumption'],
                    rec['totalizer']
                ))
            
            # Ejecutar batch
            cursor.executemany(insert_sql, data_tuples)
            sqlserver_conn.commit()
            
            records_inserted = len(records_to_insert)
            records_updated = 0
            
            logging.info(f"✓ Guardados {records_inserted} registros en SQL Server")
            
        except Exception as e:
            logging.error(f"Error en insert batch a SQL Server: {e}")
            sqlserver_conn.rollback()
            raise
    
    # Cerrar conexion SQL Server
    cursor.close()
    sqlserver_conn.close()
    
    # Log final
    logging.info(f"\n=== Resumen SQL Server ===")
    logging.info(f"Registros procesados: {records_processed}")
    logging.info(f"  - Insertados/Actualizados: {records_inserted}")
    logging.info(f"  - Saltados (sin datos): {records_skipped}")
    logging.info(f"  - Saltados (sin mapeo via tagOld): {signals_no_mapping}")
    logging.info(f"=========================")
    
    return records_processed

