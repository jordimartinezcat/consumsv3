"""
Módulo para agregar consumos minutales a resolución horaria.

Este módulo procesa datos de consumo minutos corregidos y genera:
1. Suma horaria directa de consumos (_cons)
2. Suma horaria aplicando correcciones de anomalías (_anom)
"""

import json
import os
from datetime import datetime

import numpy as np
import pandas as pd


def aggregate_to_hourly(df_minutes):
    """
    Agrega datos minutales a resolución horaria.

    Parameters:
    -----------
    df_minutes : pd.DataFrame
        DataFrame con datos minutales que debe incluir:
        - timeStamp como índice o columna
        - [Tag]_rect_0_cons : columna de consumo
        - [Tag]_rect_0_anom : columna de anomalías (opcional)

    Returns:
    --------
    pd.DataFrame
        DataFrame con agregación horaria que incluye:
        - [Tag]_hourly_cons : suma horaria directa de consumos (puede incluir valores negativos)
        - [Tag]_hourly_cons_corrected : suma horaria corregida (consumo real total de la hora)
    """
    # Asegurar que timeStamp sea el índice
    df = df_minutes.copy()
    if "timeStamp" in df.columns:
        df["timeStamp"] = pd.to_datetime(df["timeStamp"])
        df = df.set_index("timeStamp")
    elif df.index.name in ("timestamp", "timeStamp"):
        df.index = pd.to_datetime(df.index)

    # Identificar columnas de consumo y anomalía
    cons_cols = [c for c in df.columns if c.endswith("_cons")]
    if not cons_cols:
        raise ValueError("No se encontraron columnas de consumo (_cons)")

    print(f"Procesando agregación horaria para columnas: {cons_cols}")

    # Crear DataFrame resultado
    result_data = {}

    for cons_col in cons_cols:
        # Identificar la columna de anomalías correspondiente
        tag_base = cons_col.replace("_cons", "")
        anom_col = tag_base + "_anom"

        print(f"Procesando {cons_col} con anomalías en {anom_col}")

        # Generar nombres para las columnas de salida
        tag_name = tag_base.replace("_rect_0", "")  # Remover _rect_0 para nombre limpio
        hourly_cons_col = f"{tag_name}_hourly_cons"
        hourly_cons_corrected_col = (
            f"{tag_name}_hourly_cons_corrected"  # Consumo total corregido
        )
        hourly_has_corrections_col = (
            f"{tag_name}_hourly_has_corrections"  # Indicador de correcciones
        )

        # Agrupar por hora y sumar consumos
        df_hourly = (
            df.resample("h", label="left", closed="left")
            .agg({cons_col: "sum"})
            .rename(columns={cons_col: hourly_cons_col})
        )

        # Calcular consumo corregido si existe columna de anomalías
        if anom_col in df.columns:
            print(f"  Aplicando correcciones de anomalías desde {anom_col}")

            # Para cada hora, calcular el consumo corregido e indicador
            corrected_hourly = []
            has_corrections = []

            for hour_start in df_hourly.index:
                hour_end = hour_start + pd.Timedelta(hours=1)
                hour_data = df.loc[hour_start : hour_end - pd.Timedelta(minutes=1)]

                if hour_data.empty:
                    corrected_hourly.append(0.0)
                    has_corrections.append(False)
                    continue

                # Verificar si la hora tiene alguna anomalía (corrección aplicada)
                has_anomalies = hour_data[anom_col].notna().any()
                has_corrections.append(has_anomalies)

                if has_anomalies:
                    # Si hay anomalías, calcular consumo minuto a minuto con correcciones
                    real_consumption = 0.0
                    for idx, row in hour_data.iterrows():
                        if pd.notna(row[anom_col]) and row[anom_col] != 0:
                            # La columna _anom contiene el consumo CORREGIDO para este minuto
                            # (reemplaza el valor de cons_col que puede ser negativo)
                            real_consumption += row[anom_col]
                        else:
                            # Usar consumo original
                            real_consumption += row[cons_col]
                    corrected_hourly.append(real_consumption)
                else:
                    # Sin anomalías, usar suma directa
                    corrected_hourly.append(hour_data[cons_col].sum())

            df_hourly[hourly_cons_corrected_col] = corrected_hourly
            df_hourly[hourly_has_corrections_col] = has_corrections
        else:
            print(
                f"  No se encontró columna de anomalías {anom_col}, usando consumo directo"
            )
            df_hourly[hourly_cons_corrected_col] = df_hourly[hourly_cons_col]
            df_hourly[hourly_has_corrections_col] = False

        # Agregar al resultado
        for col in df_hourly.columns:
            result_data[col] = df_hourly[col]

    # Crear DataFrame final
    result_df = pd.DataFrame(result_data)
    result_df.index.name = "timeStamp"

    print(f"Agregación completada. Shape: {result_df.shape}")
    print(f"Columnas generadas: {list(result_df.columns)}")

    return result_df


def process_latest_minute_data(root_path=None):
    """
    Procesa el archivo más reciente de datos minutales y genera agregación horaria.

    Parameters:
    -----------
    root_path : str, optional
        Ruta raíz del proyecto. Si no se especifica, se detecta automáticamente.

    Returns:
    --------
    str
        Ruta del archivo generado
    """
    if root_path is None:
        root_path = os.path.dirname(os.path.dirname(__file__))

    # Buscar el archivo más reciente - primero en adquisicion/minute_data, luego en procesado/Data
    import glob

    # Priorizar archivos de procesado/Data (CON detección de anomalías)
    data_dir = os.path.join(root_path, "procesado", "Data")
    pattern1 = os.path.join(data_dir, "consumption_minutes_with_anom_*.csv")
    files = glob.glob(pattern1)

    # Fallback a adquisicion/minute_data (sin anomalías)
    if not files:
        minute_data_dir = os.path.join(root_path, "adquisicion", "minute_data")
        pattern2 = os.path.join(minute_data_dir, "all_minutes_*.csv")
        files = glob.glob(pattern2)

    if not files:
        raise FileNotFoundError(
            f"No se encontraron archivos de consumo minutos en {data_dir} ni en adquisicion/minute_data"
        )

    # Obtener el más reciente
    latest_file = max(files, key=os.path.getmtime)
    print(f"Procesando archivo: {latest_file}")

    # Cargar datos minutales intentando primero el formato europeo que usa el pipeline
    loaders = [
        ("europeo", {"sep": ";", "decimal": ","}),
        ("estándar", {}),
        ("autodetectado", {"sep": None, "engine": "python"}),
    ]

    last_error = None
    for label, kwargs in loaders:
        try:
            df_minutes = pd.read_csv(latest_file, **kwargs)
            print(f"Cargado CSV con formato {label}")
            break
        except Exception as exc:
            last_error = exc
    else:  # no break
        raise RuntimeError(
            f"No se pudo cargar {latest_file} con ningún formato conocido: {last_error}"
        )

    print(f"Datos minutales cargados. Shape: {df_minutes.shape}")
    print(f"Columnas: {list(df_minutes.columns)}")

    # Procesar agregación horaria
    df_hourly = aggregate_to_hourly(df_minutes)
    
    print(f"Datos horarios generados. Shape: {df_hourly.shape}")
    print(f"Primeras 3 horas: {df_hourly.index[:3].tolist()}")
    print(f"Últimas 3 horas: {df_hourly.index[-3:].tolist()}")

    # Leer configuración para filtrar por periodo
    config_path = os.path.join(root_path, "consums_config.json")
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        
        # Parsear period_end
        period_start = pd.to_datetime(config["period"]["start"])
        period_end = pd.to_datetime(config["period"]["end"])
        
        print(f"Periodo configurado (local Madrid): {period_start} a {period_end}")
        
        # Los timestamps en el índice son naive pero representan UTC
        # Necesitamos convertir period_end de Madrid a UTC
        print(f"Índice tiene timezone: {df_hourly.index.tz}")
        
        # Convertir period_end de Madrid a UTC
        period_end_madrid = period_end.tz_localize("Europe/Madrid")
        period_end_utc = period_end_madrid.tz_convert("UTC").tz_localize(None)  # Quitar tz para comparar con naive
        
        print(f"Periodo ajustado (UTC naive): hasta {period_end_utc}")
        
        # Filtrar para mantener solo las horas completas dentro del periodo
        # Excluimos la hora que coincide con period_end_utc
        print(f"Filtrando horas: mantener index < {period_end_utc}")
        df_hourly = df_hourly[df_hourly.index < period_end_utc]
        
        print(f"Datos filtrados por periodo. Shape: {df_hourly.shape}")
        if len(df_hourly) > 0:
            print(f"Rango temporal (UTC): {df_hourly.index.min()} a {df_hourly.index.max()}")

    # Generar archivo de salida en procesado/Data
    output_dir = os.path.join(root_path, "procesado", "Data")
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"consumption_hourly_{timestamp}.csv")

    # Guardar con formato europeo
    df_hourly.to_csv(output_file, sep=";", decimal=",", index=True)
    print(f"Archivo horario guardado: {output_file}")

    return output_file


if __name__ == "__main__":
    try:
        output_file = process_latest_minute_data()
        print(f"Procesamiento completado exitosamente: {output_file}")
    except Exception as e:
        print(f"Error en el procesamiento: {e}")
        raise SystemExit(1)
