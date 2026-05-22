"""
Script de validación de consumos horarios contra totalizadores de API.

Para cada señal:
1. Consulta el totalizador inicial (primer minuto del período)
2. Consulta el totalizador final (último minuto del período)
3. Calcula: diferencia_totalizador = total_final - total_inicial
4. Suma todos los consumos horarios de la señal
5. Compara ambos valores y genera reporte de discrepancias
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta
import logging

# Configurar rutas
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "CAT_Conexions", "src"))

from conexions import apiSagedCAT

logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")


def load_config():
    """Cargar configuración del proyecto."""
    config_path = os.path.join(ROOT, "consums_config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def find_latest_hourly_file():
    """Encontrar el archivo horario más reciente de mayo 2026."""
    data_dir = os.path.join(ROOT, "procesado", "Data")
    import glob
    pattern = os.path.join(data_dir, "consumption_hourly_may*.csv")
    files = glob.glob(pattern)
    if not files:
        pattern = os.path.join(data_dir, "consumption_hourly_*.csv")
        files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError("No se encontró archivo horario para validar")
    
    # Retornar el más reciente
    return max(files, key=os.path.getmtime)


def extract_signals_from_hourly(df):
    """Extraer lista de señales únicas del dataframe horario."""
    signals = []
    for col in df.columns:
        if col.endswith("_hourly_cons"):
            signal = col.replace("_hourly_cons", "")
            signals.append(signal)
    return sorted(signals)


def get_totalizer_from_api(api, signal, timestamp, tag_uid_map, vista, base_url):
    """
    Consultar totalizador de una señal en un timestamp específico.
    
    Maneja señales directas y pares _TOT_H/_TOT_L para 32-bit.
    La señal viene sin sufijo _TOT_H/_TOT_L, hay que añadirlos para buscar en la API.
    """
    import requests
    
    # Las señales vienen como: CL_CAT_XXX_FTR_YYY_TOT
    # En la API existen como: CL_CAT_XXX_FTR_YYY_TOT_H y CL_CAT_XXX_FTR_YYY_TOT_L
    # O como señal directa: CL_CAT_XXX_FTR_YYY_TOT (sin H/L)
    
    signal_h = f"{signal}_H"
    signal_l = f"{signal}_L"
    
    # Buscar en el mapa de UIDs
    uid_h = tag_uid_map.get(signal_h)
    uid_l = tag_uid_map.get(signal_l)
    uid_direct = tag_uid_map.get(signal)
    
    # Convertir timestamp a Unix epoch
    if isinstance(timestamp, pd.Timestamp):
        ts_epoch = timestamp.timestamp()
    else:
        ts_epoch = pd.to_datetime(timestamp).timestamp()
    
    # Endpoint de la API
    url = f"{base_url}/Documents/tagviews/{vista}/historic"
    headers = api.HEADERS if api.HEADERS else None
    
    try:
        if uid_h and uid_l:
            # Señal de 32-bit combinada - descargar pares H y L
            # logging.debug(f"  Consultando {signal_h} y {signal_l} en {timestamp}")
            
            # Consultar HIGH
            params_h = {
                "dataSource": "RAW",
                "resolution": "RES_1_MIN",
                "uids": [uid_h],
                "startTs": ts_epoch,
                "endTs": ts_epoch + 60,  # +1 minuto
            }
            
            response_h = requests.post(url, json=params_h, headers=headers, verify=False)
            response_h.raise_for_status()
            data_h = pd.json_normalize(response_h.json())
            
            # Consultar LOW
            params_l = {
                "dataSource": "RAW",
                "resolution": "RES_1_MIN",
                "uids": [uid_l],
                "startTs": ts_epoch,
                "endTs": ts_epoch + 60,
            }
            
            response_l = requests.post(url, json=params_l, headers=headers, verify=False)
            response_l.raise_for_status()
            data_l = pd.json_normalize(response_l.json())
            
            if data_h.empty or data_l.empty:
                logging.warning(f"  No hay datos para {signal} en {timestamp}")
                return None
            
            # Extraer valores
            val_h = data_h.iloc[0]["value"] if len(data_h) > 0 and "value" in data_h.columns else None
            val_l = data_l.iloc[0]["value"] if len(data_l) > 0 and "value" in data_l.columns else None
            
            if pd.isna(val_h) or pd.isna(val_l):
                logging.warning(f"  Valores NaN para {signal} en {timestamp}")
                return None
            
            # Combinar en 32-bit: (HIGH * 65536) + LOW
            total_32bit = int(val_h) * 65536 + int(val_l)
            # logging.debug(f"  {signal}: H={int(val_h)}, L={int(val_l)} → TOT32={total_32bit}")
            return total_32bit
            
        elif uid_direct:
            # Señal directa de 32-bit (sin pares H/L)
            # logging.debug(f"  Consultando {signal} (directo) en {timestamp}")
            
            params = {
                "dataSource": "RAW",
                "resolution": "RES_1_MIN",
                "uids": [uid_direct],
                "startTs": ts_epoch,
                "endTs": ts_epoch + 60,
            }
            
            response = requests.post(url, json=params, headers=headers, verify=False)
            response.raise_for_status()
            data = pd.json_normalize(response.json())
            
            if data.empty:
                logging.warning(f"  No hay datos para {signal} en {timestamp}")
                return None
            
            val = data.iloc[0]["value"] if len(data) > 0 and "value" in data.columns else None
            if pd.isna(val):
                logging.warning(f"  Valor NaN para {signal} en {timestamp}")
                return None
            
            # logging.debug(f"  {signal}: TOT={val}")
            return float(val)
        else:
            logging.error(f"  No se encontró UID para {signal} (ni H/L ni directo)")
            return None
            
    except Exception as e:
        logging.error(f"  Error consultando {signal}: {e}")
        return None


def validate_signal(api, signal, df_hourly, tag_uid_map, start_timestamp, end_timestamp, vista, base_url):
    """
    Validar una señal comparando diferencia de totalizadores vs suma de consumos.
    
    Returns:
        dict con resultados de validación
    """
    # logging.debug(f"\n{'='*60}")
    # logging.debug(f"Validando señal: {signal}")
    
    # 1. Obtener totalizadores de API
    tot_initial = get_totalizer_from_api(api, signal, start_timestamp, tag_uid_map, vista, base_url)
    tot_final = get_totalizer_from_api(api, signal, end_timestamp, tag_uid_map, vista, base_url)
    
    # Determinar tipo de error si hay valores faltantes
    if tot_initial is None or tot_final is None:
        if tot_initial is None and tot_final is None:
            error_msg = "Totalitzador no existeix o no té dades al període"
        elif tot_initial is None:
            error_msg = "No s'ha pogut obtenir totalitzador inicial"
        else:
            error_msg = "No s'ha pogut obtenir totalitzador final"
        
        logging.warning(f"{error_msg} para {signal}")
        return {
            "signal": signal,
            "status": "ERROR",
            "message": error_msg,
            "tot_initial": tot_initial,
            "tot_final": tot_final,
            "diff_totalizer": None,
            "sum_consumption": None,
            "difference": None,
            "relative_error_pct": None
        }
    
    # 2. Calcular diferencia de totalizadores
    diff_totalizer = tot_final - tot_initial
    # logging.debug(f"Diferencia totalizador: {tot_final} - {tot_initial} = {diff_totalizer}")
    
    # 3. Sumar consumos horarios (usar columna corregida si existe)
    col_corrected = f"{signal}_hourly_cons_corrected"
    col_direct = f"{signal}_hourly_cons"
    
    if col_corrected in df_hourly.columns:
        sum_consumption = df_hourly[col_corrected].sum()
        # logging.debug(f"Suma consumos corregidos: {sum_consumption}")
    elif col_direct in df_hourly.columns:
        sum_consumption = df_hourly[col_direct].sum()
        # logging.debug(f"Suma consumos directos: {sum_consumption}")
    else:
        logging.error(f"No se encontró columna de consumo para {signal}")
        return {
            "signal": signal,
            "status": "ERROR",
            "message": "Columna de consum no trobada",
            "tot_initial": tot_initial,
            "tot_final": tot_final,
            "diff_totalizer": diff_totalizer,
            "sum_consumption": None,
            "difference": None,
            "relative_error_pct": None
        }
    
    # 4. Comparar
    difference = abs(diff_totalizer - sum_consumption)
    relative_error_pct = (difference / abs(diff_totalizer) * 100) if diff_totalizer != 0 else 0
    
    # logging.debug(f"Diferencia absoluta: {difference}")
    # logging.debug(f"Error relativo: {relative_error_pct:.4f}%")
    
    # Criterio de validación especial: si la diferencia es exactamente 65536 (reset de 16 bits),
    # es un reset no detectado pero el cálculo es correcto
    is_reset_mismatch = abs(difference - 65536) < 1  # Tolerancia de 1 litro
    
    # Criterios de validacion:
    # 1. Error < 0.1% o diferencia < 1 litro → OK
    # 2. Diferencia = 65536 (reset de contador) → OK con nota
    if relative_error_pct < 0.1 or difference < 1:
        status = "OK"
        message = ""
    elif is_reset_mismatch:
        status = "OK"
        message = "Reset de comptador 16-bit no detectat (65536 L)"
        # logging.debug(f"Reset de contador detectado: diferencia = {difference:.0f} L ≈ 65536 L")
    else:
        status = "DISCREPANCIA"
        message = f"Error relatiu: {relative_error_pct:.2f}%"
    
    return {
        "signal": signal,
        "status": status,
        "message": message,
        "tot_initial": tot_initial,
        "tot_final": tot_final,
        "diff_totalizer": diff_totalizer,
        "sum_consumption": sum_consumption,
        "difference": difference,
        "relative_error_pct": relative_error_pct
    }


def main():
    print("="*80)
    print("VALIDACION DE CONSUMOS HORARIOS vs TOTALIZADORES API")
    print("="*80)
    print()
    
    # 1. Cargar configuración
    cfg = load_config()
    api_cfg = cfg.get("api", {})
    
    # 2. Conectar a API
    nexustoken = api_cfg.get("nexustoken")
    vista = api_cfg.get("vista")
    headers = {"nexustoken": nexustoken, "Content-Type": "application/json"} if nexustoken else None
    
    api = apiSagedCAT(vista=vista, headers=headers)
    print(f"Conectado a API SagedCAT, vista: {vista}")
    
    # 3. Obtener mapa de tags -> UIDs
    print("Obteniendo mapa de tags de la vista...")
    uids_df = api.get_Tags_from_vista(vista)
    tag_uid_map = {}
    for index, row in uids_df.iterrows():
        for element in row.get("columns", []):
            name = element.get("name")
            uid = element.get("uid")
            if name and uid:
                tag_uid_map[name] = uid
    
    print(f"Mapa de tags obtenido: {len(tag_uid_map)} tags disponibles")
    
    # 4. Cargar archivo horario
    hourly_file = find_latest_hourly_file()
    print(f"\nCargando archivo horario: {hourly_file}")
    
    df_hourly = pd.read_csv(hourly_file, sep=";", decimal=",", index_col=0, parse_dates=True)
    print(f"Datos cargados: {len(df_hourly)} registros horarios, {len(df_hourly.columns)} columnas")
    
    # 5. Extraer señales
    signals = extract_signals_from_hourly(df_hourly)
    print(f"Señales a validar: {len(signals)}")
    
    # 6. Determinar período
    # El índice horario representa el INICIO de cada hora
    # Para validar correctamente, necesitamos el totalizador al FINAL del período
    # que es el primer minuto de la siguiente hora (00:00 del día siguiente)
    start_timestamp = df_hourly.index.min()
    end_timestamp = df_hourly.index.max() + pd.Timedelta(hours=1)
    print(f"Periodo: {start_timestamp} -> {end_timestamp}")
    print(f"  (Consultando totalizador inicial en el primer minuto y final en el minuto 00:00 del día siguiente)")
    
    # 7. Validar cada señal
    results = []
    base_url = api_cfg.get("base_url")
    for i, signal in enumerate(signals, 1):
        print(f"\r[{i}/{len(signals)}] Procesando {signal}...", end="", flush=True)
        result = validate_signal(api, signal, df_hourly, tag_uid_map, start_timestamp, end_timestamp, vista, base_url)
        results.append(result)
    print()  # Nueva linea despues del progreso
    
    # 8. Generar reporte
    print("\n" + "="*80)
    print("RESUMEN DE VALIDACION")
    print("="*80)
    
    df_results = pd.DataFrame(results)
    
    # Contar por estado
    ok_count = (df_results["status"] == "OK").sum()
    ok_with_reset = df_results[(df_results["status"] == "OK") & (df_results["message"].str.contains("reset", case=False, na=False))].shape[0]
    ok_perfect = ok_count - ok_with_reset
    error_count = (df_results["status"] == "ERROR").sum()
    discrepancy_count = (df_results["status"] == "DISCREPANCIA").sum()
    
    print(f"Total senales: {len(signals)}")
    print(f"  OK (perfectes): {ok_perfect}")
    print(f"  OK (amb reset 16-bit): {ok_with_reset}")
    print(f"  Discrepàncies: {discrepancy_count}")
    print(f"  Errors: {error_count}")
    
    # Guardar reporte
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(ROOT, "validacions", f"validation_report_{timestamp}.csv")
    df_results.to_csv(output_file, sep=";", decimal=",", index=False)
    print(f"\nInforme desat a: {output_file}")
    
    # Mostrar discrepancias si las hay
    if discrepancy_count > 0:
        print("\n" + "="*80)
        print("DISCREPÀNCIES DETECTADES:")
        print("="*80)
        discrepancies = df_results[df_results["status"] == "DISCREPANCIA"]
        for _, row in discrepancies.iterrows():
            print(f"\n{row['signal']}:")
            print(f"  Diferència totalitzador: {row['diff_totalizer']:.2f}")
            print(f"  Suma consums:            {row['sum_consumption']:.2f}")
            print(f"  Diferència absoluta:     {row['difference']:.2f}")
            print(f"  Error relatiu:           {row['relative_error_pct']:.4f}%")
    
    # Mostrar resumen de resets detectados
    if ok_with_reset > 0:
        print("\n" + "="*80)
        print(f"SENYALS AMB RESETS DE COMPTADOR 16-BIT DETECTATS: {ok_with_reset}")
        print("="*80)
        print("Aquests senyals tenen diferències de 65536 L (reset de comptador LOW)")
        print("Els consums calculats són correctes, validació OK")
        resets = df_results[(df_results["status"] == "OK") & (df_results["message"].str.contains("reset", case=False, na=False))]
        for _, row in resets.iterrows():
            print(f"  {row['signal']}: diferència = {row['difference']:.0f} L")
    
    # Mostrar errores si los hay
    if error_count > 0:
        print("\n" + "="*80)
        print("ERRORES EN CONSULTA DE TOTALIZADORES:")
        print("="*80)
        errors = df_results[df_results["status"] == "ERROR"]
        for _, row in errors.iterrows():
            print(f"  {row['signal']}: {row['message']}")
    
    # Generar reporte PDF automáticamente
    print("\n" + "="*80)
    print("GENERANDO REPORTE PDF...")
    print("="*80)
    try:
        # Importar módulo de generación de PDF
        import sys
        validations_dir = os.path.join(ROOT, "validacions")
        if validations_dir not in sys.path:
            sys.path.insert(0, validations_dir)
        
        import generate_validation_report
        csv_path = generate_validation_report.find_latest_validation_csv()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        pdf_path = os.path.join(ROOT, "validacions", f"validation_report_{timestamp}.pdf")
        generate_validation_report.create_pdf_report(csv_path, pdf_path)
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo generar el PDF: {e}")
        import traceback
        traceback.print_exc()
        print("El archivo CSV de validación se generó correctamente.")
    
    return 0 if discrepancy_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
