# -*- coding: utf-8 -*-
"""
Modulo para guardar totalizadores diarios en PostgreSQL.

Los totalizadores se obtienen durante la validacion diaria y se almacenan
para uso posterior (SQL Server, reportes, etc.)

Tabla: ga_landing.ite_consums_totalitzadors
Columnas:
- tag: Nombre de la señal
- fecha: Fecha del dia (date)
- totalizador_00h: Totalizador a las 00:00 del dia
- totalizador_24h: Totalizador a las 24:00 (00:00 del dia siguiente)
- fecha_actualizacion: Timestamp de insercion
"""

import logging
from datetime import datetime
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def create_totalizers_table_if_not_exists(engine):
    """
    Crear tabla de totalizadores si no existe.
    
    Args:
        engine: SQLAlchemy engine para PostgreSQL
    """
    create_table_sql = text("""
        CREATE TABLE IF NOT EXISTS ga_landing.ite_consums_totalitzadors (
            tag VARCHAR(255) NOT NULL,
            fecha DATE NOT NULL,
            totalizador_00h NUMERIC(18, 2),
            totalizador_24h NUMERIC(18, 2),
            fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (tag, fecha)
        );
        
        CREATE INDEX IF NOT EXISTS idx_consums_totalitzadors_fecha 
        ON ga_landing.ite_consums_totalitzadors(fecha);
        
        CREATE INDEX IF NOT EXISTS idx_consums_totalitzadors_tag 
        ON ga_landing.ite_consums_totalitzadors(tag);
    """)
    
    try:
        with engine.connect() as conn:
            conn.execute(create_table_sql)
            conn.commit()
        logging.info("Tabla ite_consums_totalitzadors verificada/creada")
    except Exception as e:
        logging.warning(f"No se pudo crear tabla (puede que ya exista o falten permisos): {e}")


def save_totalizers_batch(engine, totalizers_data):
    """
    Guardar totalizadores en batch usando UPSERT (INSERT ON CONFLICT).
    
    Args:
        engine: SQLAlchemy engine para PostgreSQL
        totalizers_data: Lista de dicts con keys: tag, fecha, totalizador_00h, totalizador_24h
    
    Returns:
        int: Numero de registros insertados/actualizados
    """
    if not totalizers_data:
        logging.info("No hay totalizadores para guardar")
        return 0
    
    upsert_sql = text("""
        INSERT INTO ga_landing.ite_consums_totalitzadors 
            (tag, fecha, totalizador_00h, totalizador_24h, fecha_actualizacion)
        VALUES 
            (:tag, :fecha, :tot_00h, :tot_24h, CURRENT_TIMESTAMP)
        ON CONFLICT (tag, fecha) 
        DO UPDATE SET
            totalizador_00h = EXCLUDED.totalizador_00h,
            totalizador_24h = EXCLUDED.totalizador_24h,
            fecha_actualizacion = CURRENT_TIMESTAMP;
    """)
    
    try:
        with engine.connect() as conn:
            for data in totalizers_data:
                conn.execute(upsert_sql, {
                    'tag': data['tag'],
                    'fecha': data['fecha'],
                    'tot_00h': data['totalizador_00h'],
                    'tot_24h': data['totalizador_24h']
                })
            conn.commit()
        
        logging.info(f"Guardados {len(totalizers_data)} totalizadores en PostgreSQL")
        return len(totalizers_data)
    
    except Exception as e:
        logging.error(f"Error guardando totalizadores: {e}")
        raise


def get_totalizer_for_date(engine, tag, fecha):
    """
    Obtener totalizador guardado para una señal y fecha.
    
    Args:
        engine: SQLAlchemy engine para PostgreSQL
        tag: Nombre de la señal
        fecha: Fecha en formato 'YYYY-MM-DD' o date
    
    Returns:
        dict con 'totalizador_00h' y 'totalizador_24h', o None si no existe
    """
    query = text("""
        SELECT totalizador_00h, totalizador_24h
        FROM ga_landing.ite_consums_totalitzadors
        WHERE tag = :tag AND fecha = :fecha
    """)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(query, {'tag': tag, 'fecha': fecha})
            row = result.fetchone()
            
            if row:
                return {
                    'totalizador_00h': float(row[0]) if row[0] is not None else None,
                    'totalizador_24h': float(row[1]) if row[1] is not None else None
                }
            return None
    
    except Exception as e:
        logging.error(f"Error obteniendo totalizador para {tag} en {fecha}: {e}")
        return None
