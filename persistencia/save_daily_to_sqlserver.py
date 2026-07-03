"""
Módulo para guardar datos diarios en SQL Server.

Guarda resumen diario de cada contador en la tabla Consums_dia:
- Id: Código corto del contador (ej: B11, BPD06)
- Data: Fecha del día
- Consum: Consumo total del día desde ite_v_consums_24h
- Totalitz: Valor del totalizador a las 00:00 desde API
- Validat, Nivell, especial: NULL (campos opcionales)

Estrategia: DELETE+INSERT (borra y vuelve a insertar cada registro)
Nota: Cambio desde MERGE que causaba conflictos de clave duplicada.
Se ejecuta después de la validación diaria.
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

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def get_sqlserver_connection(cfg=None):
    """
    Crear conexión a SQL Server usando autenticación Windows (Trusted Connection).
    
    Configuración en consums_config.json:
    {
      "sqlserver": {
        "host": "servercmp",
        "port": 1433,
        "database": "Consums",
        "table": "Consums_dia"
      }
    }
    
    Args:
        cfg (dict): Configuración con sección 'sqlserver'
    
    Returns:
        pyodbc.Connection: Conexión SQL Server
    """
    if cfg is None:
        CONFIG_PATH = os.path.join(ROOT, "consums_config.json")
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    
    sqlserver_config = cfg.get("sqlserver", {})
    
    if not sqlserver_config:
        raise ValueError("Falta configuración 'sqlserver' en consums_config.json")
    
    host = sqlserver_config.get("host", "servercmp")
    port = sqlserver_config.get("port", 1433)
    database = sqlserver_config.get("database", "Consums")
    
    logging.info(f"Conectando a SQL Server: {host}:{port}/{database}")
    logging.info("Usando autenticación Windows (Trusted Connection)")
    
    # Connection string con autenticación Windows
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"  # Autenticación Windows
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
    # Ejemplo: CL_CAT_B11_FTR_... -> parts[2]
    if len(parts) >= 4 and parts[0] == 'CL' and parts[1] == 'CAT':
        # Puede ser un solo elemento (B11) o compuesto (BPD06)
        # Buscar dónde está FTR para saber cuántos elementos tomar
        try:
            ftr_index = parts.index('FTR')
            # Todo entre CAT y FTR
            counter_parts = parts[2:ftr_index]
            counter_id = '_'.join(counter_parts)
            return counter_id
        except ValueError:
            # FTR no encontrado, tomar solo el siguiente a CAT
            return parts[2]
    
    # Fallback: intentar buscar patrón sin CL_CAT_
    if '_FTR_' in tag_name:
        # Extraer todo antes de _FTR_
        pre_ftr = tag_name.split('_FTR_')[0]
        # Quitar prefijos comunes
        for prefix in ['CL_CAT_', 'CL_', 'CAT_']:
            if pre_ftr.startswith(prefix):
                return pre_ftr[len(prefix):
        str: ID corto del contador
    """
    # LÓGICA TEMPORAL - VERIFICAR Y AJUSTAR
    parts = tag_name.split('_')
    
    # Buscar el código entre CL_CAT_ y _FTR_
    # Ejemplo: CL_CAT_B11_FTR_... → B11
    if len(parts) >= 3 and parts[0] == 'CL' and parts[1] == 'CAT':
        return parts[2]
    
    # Si no sigue el patrón esperado, devolver el tag completo
    logging.warning(f"No se pudo extraer ID de '{tag_name}', usando tag completo")
    return tag_name


def get_totalizer_at_midnight(api, tag_uid, date, vista):
    """
    Obtener valor del totalizador a las 00:00 del día especificado desde API.
    
    Args:
        api: Instancia de apiSagedCAT
        tag_uid (str): UID de la señal en la API
        date (str): Fecha en formato 'YYYY-MM-DD'
        vista (str): Vista de la API
    
    Returns:
        float: Valor del totalizador a las 00:00, o None si no existe
    """
    timestamp_midnight = f"{date} 00:00:00"
    
    try:
        # Consultar API
        response = api.get_tag_historic(
            uid=tag_uid,
            start_time=timestamp_midnight,
            end_time=timestamp_midnight,
            vista=vista
        )
        
        if response and len(response) > 0:
            # Tomar el primer valor
            value = response[0].get('value')
            if value is not None:
                return float(value)
        
        return None
    
    except Exception as e:
        logging.error(f"Error obteniendo totalizador para {tag_uid} a las {timestamp_midnight}: {e}")
        return None


def get_daily_consumption_from_pg(pg_engine, tag_csm, date):
    """
    Obtener consumo diario desde la vista ga_datalake.ite_v_consums_24h.
    
    Esta vista contiene los consumos diarios consolidados (24 horas).
    
    Args:
        pg_engine: Engine de SQLAlchemy para PostgreSQL
        tag_csm (str): Nombre del tag en formato CSM (sin prefijo CL_CAT_)
        date (str): Fecha en formato 'YYYY-MM-DD'
    
    Returns:
        float: Consumo del día, o None si no existe
    """
    query = text("""
        SELECT SUM(c.consum) as total_consumption
        FROM ga_datalake.ite_v_consums_24h c
        INNER JOIN ga_datalake.cfg_tags t ON c.id_tag = t.id_tag
        WHERE t.tag_name LIKE :tag_pattern
          AND c.timestamp_utc::date = :date::date
    """)
    
    try:
        # Convertir tag CSM a patrón de búsqueda
        # Ej: B11_FTR_G01_CSM → %B11_FTR_G01%
        tag_pattern = f"%{tag_csm.replace('_CSM', '')}%"
        
        with pg_engine.connect() as conn:
            result = conn.execute(query, {
                'tag_pattern': tag_pattern,
                'date': date
            })
            row = result.fetchone()
            
            if row and row[0] is not None:
                return float(row[0])
            
            return None
    
    except Exception as e:
        logging.error(f"Error obteniendo consumo de PostgreSQL para {tag_csm} en {date}: {e}")
        return None


def save_daily_data_to_sqlserver(date=None, cfg=None):
    """
    Guardar datos diarios en SQL Server tabla Consums_dia.
    
    Para cada señal procesada:
    1. Obtener Id (código corto del contador)
    2. Obtener totalizador a las 00:00 desde API
    3. Obtener consumo del día desde PostgreSQL (ite_v_consums_24h)
    4. MERGE en SQL Server: UPDATE si existe, INSERT si no
    
    Columnas de la tabla Consums_dia:
    - Id: Código contador (B11, BPD06, etc.)
    - Data: Fecha del día
    - Consum: Consumo total del día
    - Totalitz: Totalizador a las 00:00
    - Validat: NULL (no usado)
    - Nivell: NULL (no usado)
    - especial: NULL (no usado)
    
    Args:
        date (str): Fecha en formato 'YYYY-MM-DD'. Si None, usa ayer.
        cfg (dict): Configuración
    
    Returns:
        int: Número de registros insertados/actualizados
    """
    # Si no se especifica fecha, usar ayer
    if date is None:
        yesterday = datetime.now() - timedelta(days=1)
        date = yesterday.strftime('%Y-%m-%d')
    
    logging.info(f"=== Guardado SQL Server para fecha: {date} ===")
    
    # Cargar configuración
    if cfg is None:
        CONFIG_PATH = os.path.join(ROOT, "consums_config.json")
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    
    # Obtener configuración API
    api_config = cfg.get("api", {})
    vista = api_config.get("vista")
    nexustoken = api_config.get("nexustoken")
    base_url = api_config.get("base_url")
    
    if not all([vista, nexustoken, base_url]):
        raise ValueError("Falta configuración API en consums_config.json")
    
    # Obtener configuración SQL Server
    sqlserver_config = cfg.get("sqlserver", {})
    table_name = sqlserver_config.get("table", "Consums_dia")
    
    # Conectar a PostgreSQL
    logging.info("Conectando a PostgreSQL...")
    pg_engine = get_db_connection(cfg)
    
    # Conectar a SQL Server
    logging.info("Conectando a SQL Server...")
    sqlserver_conn = get_sqlserver_connection(cfg)
    cursor = sqlserver_conn.cursor()
    
    # Conectar a API
    logging.info("Conectando a API SagedCAT...")
    api = apiSagedCAT(token=nexustoken, base_url=base_url)
    
    # Obtener lista de señales procesadas desde PostgreSQL
    logging.info("Obteniendo lista de señales...")
    query_signals = text("""
        SELECT DISTINCT t.tag_name, t.uid
        FROM ga_datalake.cfg_tags t
        WHERE t.tag_name LIKE '%_TOT'
          AND t.tag_name LIKE 'CL_CAT%'
          AND t.tag_name NOT LIKE 'CL_CAT_ET%'
          AND t.tag_name NOT LIKE '%_LS_%'
          AND t.tag_name NOT LIKE '%_P_%'
        ORDER BY t.tag_name
    """)
    
    signals = []
    with pg_engine.connect() as conn:
        result = conn.execute(query_signals)
        signals = result.fetchall()
    
    logging.info(f"Procesando {len(signals)} señales...")
    
    # Procesar cada señal
    records_processed = 0
    records_inserted = 0
    records_updated = 0
    records_skipped = 0
    
    for tag_name, tag_uid in signals:
        try:
            # 1. Extraer ID del contador
            counter_id = extract_counter_id(tag_name)
            
            # 2. Obtener totalizador a las 00:00 desde API
            totalizer = get_totalizer_at_midnight(api, tag_uid, date, vista)
            
            # 3. Obtener consumo desde PostgreSQL
            # Convertir tag TOT a CSM para búsqueda
            tag_csm = tag_name.replace('_TOT', '_CSM')
            consumption = get_daily_consumption_from_pg(pg_engine, tag_csm, date)
            
            # Verificar si tenemos datos
            if totalizer is None and consumption is None:
                logging.debug(f"Saltando {counter_id}: Sin datos de totalizador ni consumo")
                records_skipped += 1
                continue
            
            # Usar 0 si alguno es None pero el otro tiene valor
            if totalizer is None:
                totalizer = 0.0
            if consumption is None:
                consumption = 0.0
            
            # 4. DELETE+INSERT en SQL Server (más robusto que MERGE)
            # Primero intentar DELETE
            delete_sql = f"DELETE FROM {table_name} WHERE Id = ? AND Data = ?"
            cursor.execute(delete_sql, (counter_id, date))
            
            # Luego INSERT
            insert_sql = f"""
            INSERT INTO {table_name} (Id, Data, Consum, Totalitz, Validat, Nivell, especial)
            VALUES (?, ?, ?, ?, NULL, NULL, NULL)
            """
            
            cursor.execute(insert_sql, (counter_id, date, consumption, totalizer))
            
            # Verificar resultado
            if cursor.rowcount > 0:
                records_inserted += 1
                records_processed += 1
                
                logging.debug(
                    f"[OK] {counter_id}: Consum={consumption:.2f}L, Totalitz={totalizer:.2f}L"
                )
            
        except Exception as e:
            logging.error(f"Error procesando señal {tag_name}: {e}")
            continue
    
    # Commit de todas las transacciones
    sqlserver_conn.commit()
    
    # Cerrar conexiones
    cursor.close()
    sqlserver_conn.close()
    
    # Log final
    logging.info(f"\n=== Resumen SQL Server ===")
    logging.info(f"Registros procesados: {records_processed}")
    logging.info(f"  - Insertados: {records_inserted}")
    logging.info(f"  - Actualizados: {records_updated}")
    logging.info(f"  - Saltados (sin datos): {records_skipped}")
    logging.info(f"=========================")
    
    return records_processed


def main():
    """
    Ejecutar guardado en SQL Server (script standalone).
    """
    logging.info("=== Guardado de Datos Diarios en SQL Server ===")
    
    try:
        # PENDIENTE: Completar implementación
        total_records = save_daily_data_to_sqlserver()
        
        logging.info(f"✓ Proceso completado: {total_records} registros guardados")
        return 0
    
    except Exception as e:
        logging.error(f"❌ Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
