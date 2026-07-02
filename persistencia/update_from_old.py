"""
Módulo para actualizar contadores específicos desde Consums_dia_old.

Estos contadores tienen correcciones manuales en Consums_dia_old que deben
prevalecer sobre los valores calculados automáticamente.

Lista de contadores: K3DP, LTT07, PAT09, K1T, LTT04, PAT03, VVD02, VVT02, PBT09, RRD02

Actualiza:
- MSSQL: Consums_dia (desde Consums_dia_old)
- PostgreSQL: ga_datalake.ite_consums_datarect (desde Consums_dia_old vía MSSQL)
"""
import logging
import os
import sys
import json
import pyodbc
import pytz
from datetime import datetime, timedelta
from sqlalchemy import text

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from persistencia.db_connection import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


# Lista de contadores que deben actualizarse desde Consums_dia_old
CONTADORES_FROM_OLD = [
    'K3DP', 'LTT07', 'PAT09', 'K1T', 'LTT04', 
    'PAT03', 'VVD02', 'VVT02', 'PBT09', 'RRD02'
]


def get_sqlserver_connection():
    """
    Crear conexión a SQL Server usando configuración del proyecto.
    """
    config_path = os.path.join(ROOT, "consums_config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    
    sqlserver_config = cfg.get("sqlserver", {})
    host = sqlserver_config.get("host", "servercmp")
    port = sqlserver_config.get("port", 1433)
    database = sqlserver_config.get("database", "Consums")
    
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
    )
    
    try:
        return pyodbc.connect(conn_str)
    except:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
            f"TrustServerCertificate=yes;"
        )
        return pyodbc.connect(conn_str)


def get_idtag_mappings():
    """
    Obtener mappings de contador → idTag desde ite_consums_tags.
    
    Returns:
        dict: {contador_id: idtag}
    """
    config_path = os.path.join(ROOT, "consums_config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    
    pg_engine = get_db_connection(cfg)
    idtag_map = {}
    
    with pg_engine.connect() as conn:
        placeholders = ','.join([f':c{i}' for i in range(len(CONTADORES_FROM_OLD))])
        query = text(f"""
            SELECT "tagOld", "idTag"
            FROM ga_landing.ite_consums_tags
            WHERE "tagOld" IN ({placeholders})
        """)
        
        params = {f'c{i}': cont for i, cont in enumerate(CONTADORES_FROM_OLD)}
        result = conn.execute(query, params)
        
        for row in result:
            tag_old, idtag = row
            idtag_map[tag_old] = idtag
    
    pg_engine.dispose()
    return idtag_map


def update_datarect_from_old(target_date=None, idtag_map=None):
    """
    Actualizar ga_datalake.ite_consums_datarect desde Consums_dia_old.
    
    IMPORTANTE: También borra los datos horarios de ite_consums_data para estos
    contadores, ya que datarect contiene el valor diario consolidado.
    
    Args:
        target_date (str): Fecha en formato 'YYYY-MM-DD'. Si None, usa el día anterior.
        idtag_map (dict): Mapping contador → idTag. Si None, se obtiene automáticamente.
    
    Returns:
        int: Número de registros insertados
    """
    if target_date is None:
        yesterday = datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime('%Y-%m-%d')
    
    if idtag_map is None:
        idtag_map = get_idtag_mappings()
    
    logging.info(f"Actualizando ite_consums_datarect para fecha: {target_date}")
    
    # Leer datos de MSSQL
    mssql_conn = get_sqlserver_connection()
    mssql_cursor = mssql_conn.cursor()
    
    placeholders_mssql = ','.join(['?'] * len(CONTADORES_FROM_OLD))
    mssql_cursor.execute(f"""
        SELECT Id, Data, Consum
        FROM Consums_dia_old
        WHERE Id IN ({placeholders_mssql})
          AND Data = CONVERT(date, ?)
    """, CONTADORES_FROM_OLD + [target_date])
    
    mssql_data = mssql_cursor.fetchall()
    
    if not mssql_data:
        logging.warning(f"No se encontraron datos en Consums_dia_old para {target_date}")
        mssql_cursor.close()
        mssql_conn.close()
        return 0
    
    logging.info(f"Datos leídos de Consums_dia_old: {len(mssql_data)} registros")
    
    # Preparar datos para PostgreSQL
    madrid_tz = pytz.timezone('Europe/Madrid')
    insert_data = []
    
    for row in mssql_data:
        contador_id, fecha, consumo = row
        
        if contador_id not in idtag_map:
            logging.warning(f"Saltando {contador_id}: no tiene idTag mapping")
            continue
        
        idtag = idtag_map[contador_id]
        
        # Convertir fecha a timestamp UTC
        dt_naive = datetime.combine(fecha, datetime.min.time())
        dt_madrid = madrid_tz.localize(dt_naive)
        dt_utc = dt_madrid.astimezone(pytz.UTC)
        
        insert_data.append({
            'idtag': idtag,
            'data': dt_utc,
            'valor': float(consumo) if consumo else 0.0,
            'tipus': 1,
            'descrip': 'Dada diària corregida des del sistema antic (Consums_dia_old)'
        })
    
    mssql_cursor.close()
    mssql_conn.close()
    
    if not insert_data:
        logging.warning("No hay datos para insertar en PostgreSQL")
        return 0
    
    # Insertar en PostgreSQL
    config_path = os.path.join(ROOT, "consums_config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    
    pg_engine = get_db_connection(cfg)
    
    with pg_engine.connect() as conn:
        # 1. BORRAR datos horarios de ite_consums_data para estos contadores
        idtags = list(set([d['idtag'] for d in insert_data]))
        placeholders_pg = ','.join([f':id{i}' for i in range(len(idtags))])
        
        # Calcular inicio y fin del día en UTC
        dt_start = madrid_tz.localize(datetime.strptime(target_date, '%Y-%m-%d'))
        dt_end = dt_start + timedelta(days=1)
        dt_start_utc = dt_start.astimezone(pytz.UTC)
        dt_end_utc = dt_end.astimezone(pytz.UTC)
        
        # Borrar todos los registros horarios del día en ite_consums_data
        delete_data_query = text(f"""
            DELETE FROM ga_datalake.ite_consums_data
            WHERE idtag IN ({placeholders_pg})
              AND data >= :start_date
              AND data < :end_date
        """)
        
        params_data = {f'id{i}': idtag for i, idtag in enumerate(idtags)}
        params_data['start_date'] = dt_start_utc
        params_data['end_date'] = dt_end_utc
        
        result_data = conn.execute(delete_data_query, params_data)
        deleted_data_count = result_data.rowcount
        
        if deleted_data_count > 0:
            logging.info(f"Registros horarios eliminados de ite_consums_data: {deleted_data_count}")
        
        # 2. Eliminar correcciones existentes del día en ite_consums_datarect
        delete_rect_query = text(f"""
            DELETE FROM ga_datalake.ite_consums_datarect
            WHERE idtag IN ({placeholders_pg})
              AND data >= :start_date
              AND data < :end_date
              AND EXTRACT(hour FROM data AT TIME ZONE 'Europe/Madrid') = 0
              AND EXTRACT(minute FROM data AT TIME ZONE 'Europe/Madrid') = 0
        """)
        
        params_rect = {f'id{i}': idtag for i, idtag in enumerate(idtags)}
        params_rect['start_date'] = dt_start_utc
        params_rect['end_date'] = dt_end_utc
        
        result_rect = conn.execute(delete_rect_query, params_rect)
        deleted_rect_count = result_rect.rowcount
        
        if deleted_rect_count > 0:
            logging.info(f"Registros diarios eliminados de ite_consums_datarect: {deleted_rect_count}")
        
        # 3. Insertar nuevos datos diarios en datarect
        insert_query = text("""
            INSERT INTO ga_datalake.ite_consums_datarect
                (data, idtag, valor, tipus, descrip)
            VALUES
                (:data, :idtag, :valor, :tipus, :descrip)
        """)
        
        for data in insert_data:
            conn.execute(insert_query, data)
        
        conn.commit()
    
    pg_engine.dispose()
    
    logging.info(f"Registros insertados en ite_consums_datarect: {len(insert_data)}")
    return len(insert_data)


def update_counters_from_old(target_date=None, update_postgresql=True):
    """
    Actualizar contadores específicos desde Consums_dia_old.
    Actualiza tanto MSSQL (Consums_dia) como PostgreSQL (ite_consums_datarect).
    
    Args:
        target_date (str): Fecha en formato 'YYYY-MM-DD'. Si None, usa el día anterior.
        update_postgresql (bool): Si True, también actualiza ite_consums_datarect en PostgreSQL.
    
    Returns:
        tuple: (registros_mssql, registros_postgresql)
    """
    if target_date is None:
        # Por defecto, procesar el día anterior
        yesterday = datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime('%Y-%m-%d')
    
    logging.info(f"Actualizando contadores desde Consums_dia_old para fecha: {target_date}")
    logging.info(f"Contadores a actualizar: {', '.join(CONTADORES_FROM_OLD)}")
    
    # 1. Actualizar MSSQL Consums_dia
    conn = get_sqlserver_connection()
    cursor = conn.cursor()
    
    # Verificar si hay datos en Consums_dia_old para esa fecha
    contadores_str = "','".join(CONTADORES_FROM_OLD)
    
    cursor.execute(f"""
        SELECT 
            Id,
            COUNT(*) as num_registros
        FROM Consums_dia_old
        WHERE Id IN ('{contadores_str}')
          AND Data = CONVERT(date, ?)
        GROUP BY Id
    """, target_date)
    
    datos_old = {}
    for row in cursor.fetchall():
        datos_old[row[0]] = row[1]
    
    if not datos_old:
        logging.warning(f"No se encontraron datos en Consums_dia_old para {target_date}")
        cursor.close()
        conn.close()
        return (0, 0)
    
    logging.info(f"Encontrados {len(datos_old)} contadores con datos en Consums_dia_old")
    
    # Actualizar usando MERGE
    total_actualizados = 0
    
    for contador in CONTADORES_FROM_OLD:
        if contador not in datos_old:
            continue
        
        cursor.execute(f"""
            MERGE Consums_dia AS target
            USING (
                SELECT Id, Data, Consum, Totalitz
                FROM Consums_dia_old
                WHERE Id = ?
                  AND Data = CONVERT(date, ?)
            ) AS source
            ON target.Id = source.Id AND target.Data = source.Data
            WHEN MATCHED THEN
                UPDATE SET 
                    target.Consum = source.Consum,
                    target.Totalitz = source.Totalitz
            WHEN NOT MATCHED THEN
                INSERT (Id, Data, Consum, Totalitz, Nivell)
                VALUES (source.Id, source.Data, source.Consum, source.Totalitz, NULL);
        """, contador, target_date)
        
        rows_affected = cursor.rowcount
        total_actualizados += rows_affected
        
        if rows_affected > 0:
            logging.info(f"  ✓ {contador}: {rows_affected} registro(s) actualizado(s) en Consums_dia")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logging.info(f"Total registros actualizados en MSSQL: {total_actualizados}")
    
    # 2. Actualizar PostgreSQL ite_consums_datarect
    total_postgresql = 0
    if update_postgresql:
        try:
            idtag_map = get_idtag_mappings()
            total_postgresql = update_datarect_from_old(target_date, idtag_map)
        except Exception as e:
            logging.error(f"Error actualizando PostgreSQL: {e}")
    
    return (total_actualizados, total_postgresql)
