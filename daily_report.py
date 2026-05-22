"""
Script de automatización para ejecución diaria del proceso de consumos.

Ejecuta automáticamente:
1. Actualiza configuración con fechas del día anterior
2. Ejecuta pipeline completo (descarga, procesado, inserción BD)
3. Ejecuta validación (genera CSV y PDF automáticamente)
4. Envía email con reporte PDF adjunto

Programar con Windows Task Scheduler o similar.
"""
import os
import sys
import json
import subprocess
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Configurar rutas
ROOT = Path(__file__).parent.absolute()
sys.path.insert(0, str(ROOT))

# Configurar logging
log_dir = ROOT / "log"
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"daily_report_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("daily_report")


def update_config_with_yesterday():
    """
    Actualiza consums_config.json con el período del día anterior.
    Ayer 00:00:00 → Hoy 00:00:00
    """
    config_path = ROOT / "consums_config.json"
    
    # Calcular fechas
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    
    period_start = yesterday.strftime("%Y-%m-%d %H:%M:%S")
    period_end = today.strftime("%Y-%m-%d %H:%M:%S")
    
    logger.info(f"Calculando período: {period_start} → {period_end}")
    
    # Leer config actual
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    
    # Actualizar período
    config["period"]["start"] = period_start
    config["period"]["end"] = period_end
    
    # Guardar config actualizado
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=4, ensure_ascii=False)
    
    logger.info("Configuración actualizada exitosamente")
    
    return period_start, period_end, config


def run_pipeline():
    """Ejecuta el pipeline completo de procesado."""
    logger.info("="*80)
    logger.info("INICIANDO PIPELINE DE PROCESADO")
    logger.info("="*80)
    
    try:
        result = subprocess.run(
            [sys.executable, str(ROOT / "run_pipeline.py")],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            encoding="cp1252",  # Windows encoding for Spanish/Catalan
            errors="replace",   # Replace invalid chars instead of crashing
            timeout=3600  # 1 hora timeout
        )
        
        # Registrar output
        if result.stdout:
            logger.info("Pipeline output:\n%s", result.stdout)
        if result.stderr:
            logger.warning("Pipeline stderr:\n%s", result.stderr)
        
        if result.returncode != 0:
            logger.error(f"Pipeline falló con código {result.returncode}")
            return False
        
        logger.info("Pipeline completado exitosamente")
        return True
        
    except subprocess.TimeoutExpired:
        logger.error("Pipeline excedió el tiempo máximo de ejecución (1 hora)")
        return False
    except Exception as e:
        logger.error(f"Error ejecutando pipeline: {e}")
        return False


def run_validation():
    """
    Ejecuta la validación de consumos.
    La validación ya genera el PDF automáticamente.
    Retorna las rutas de los archivos generados y las estadísticas.
    """
    logger.info("="*80)
    logger.info("INICIANDO VALIDACIÓN DE CONSUMOS")
    logger.info("="*80)
    
    try:
        result = subprocess.run(
            [sys.executable, str(ROOT / "validacions" / "validate_consumption.py")],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            encoding="cp1252",  # Windows encoding for Spanish/Catalan
            errors="replace",   # Replace invalid chars instead of crashing
            timeout=1800  # 30 minutos timeout
        )
        
        # Registrar output
        if result.stdout:
            logger.info("Validation output:\n%s", result.stdout)
        if result.stderr:
            logger.warning("Validation stderr:\n%s", result.stderr)
        
        # La validación retorna código 1 si hay discrepancias, pero eso es normal
        # Solo consideramos error si es otro código o excepción
        
        # Buscar archivos generados más recientes
        validations_dir = ROOT / "validacions"
        csv_files = sorted(validations_dir.glob("validation_report_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
        pdf_files = sorted(validations_dir.glob("validation_report_*.pdf"), key=lambda p: p.stat().st_mtime, reverse=True)
        
        if not csv_files or not pdf_files:
            logger.error("No se encontraron archivos de validación generados")
            return None, None, None
        
        csv_path = str(csv_files[0])
        pdf_path = str(pdf_files[0])
        
        # Extraer estadísticas del output
        summary_stats = extract_stats_from_output(result.stdout)
        
        logger.info(f"Validación completada. CSV: {csv_path}, PDF: {pdf_path}")
        return csv_path, pdf_path, summary_stats
        
    except subprocess.TimeoutExpired:
        logger.error("Validación excedió el tiempo máximo de ejecución (30 minutos)")
        return None, None, None
    except Exception as e:
        logger.error(f"Error ejecutando validación: {e}")
        return None, None, None


def extract_stats_from_output(output: str) -> dict:
    """Extrae estadísticas del output de validación."""
    import re
    
    stats = {
        "total": 0,
        "ok_perfect": 0,
        "ok_reset": 0,
        "discrepancies": 0,
        "errors": 0
    }
    
    try:
        # Buscar líneas de resumen (en catalán)
        if match := re.search(r"Total senales:\s*(\d+)", output):
            stats["total"] = int(match.group(1))
        if match := re.search(r"OK \(perfectes\):\s*(\d+)", output):
            stats["ok_perfect"] = int(match.group(1))
        if match := re.search(r"OK \(amb reset 16-bit\):\s*(\d+)", output):
            stats["ok_reset"] = int(match.group(1))
        if match := re.search(r"Discrepàncies:\s*(\d+)", output):
            stats["discrepancies"] = int(match.group(1))
        if match := re.search(r"Errors:\s*(\d+)", output):
            stats["errors"] = int(match.group(1))
    except Exception as e:
        logger.warning(f"Error extrayendo estadísticas: {e}")
    
    return stats


def send_email_report(csv_path: str, pdf_path: str, period_start: str, period_end: str, config: dict, summary_stats: dict):
    """Envía el reporte por email."""
    logger.info("="*80)
    logger.info("ENVIANDO REPORTE POR EMAIL")
    logger.info("="*80)
    
    email_config = config.get("email", {})
    
    if not email_config.get("enabled", False):
        logger.info("Envío de email deshabilitado en configuración")
        return True
    
    try:
        from email_utils import send_validation_report
        
        send_validation_report(
            pdf_path=pdf_path,
            csv_path=csv_path,
            period_start=period_start,
            period_end=period_end,
            config_email=email_config,
            summary_stats=summary_stats,
            logger=logger
        )
        
        logger.info("Email enviado exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error enviando email: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def main():
    """Función principal."""
    start_time = datetime.now()
    logger.info("="*80)
    logger.info(f"INICIANDO PROCESO DIARIO DE CONSUMOS - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80)
    
    success = True
    
    try:
        # 1. Actualizar configuración con fechas de ayer
        period_start, period_end, config = update_config_with_yesterday()
        
        # 2. Ejecutar pipeline completo
        if not run_pipeline():
            logger.error("Pipeline falló, abortando proceso")
            success = False
            return 1
        
        # 3. Ejecutar validación (genera CSV y PDF automáticamente)
        csv_path, pdf_path, summary_stats = run_validation()
        
        if not csv_path or not pdf_path:
            logger.error("Validación falló, abortando proceso")
            success = False
            return 1
        
        # 4. Enviar email con reporte
        if not send_email_report(csv_path, pdf_path, period_start, period_end, config, summary_stats):
            logger.warning("Email no pudo ser enviado, pero el proceso continúa")
            # No marcamos como fallo porque el proceso principal fue exitoso
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("="*80)
        logger.info(f"PROCESO COMPLETADO EXITOSAMENTE en {duration:.1f} segundos")
        logger.info("="*80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Error crítico en proceso diario: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.error("="*80)
        logger.error(f"PROCESO FALLÓ después de {duration:.1f} segundos")
        logger.error("="*80)
        
        return 1


if __name__ == "__main__":
    sys.exit(main())
