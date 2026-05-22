"""
Script para enviar el email del último reporte generado.
Útil cuando el proceso se completó pero el email no se envió.
"""
import os
import sys
import json
import glob
import logging

# Configurar rutas
ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("send_email")

# Cargar config
config_path = os.path.join(ROOT, "consums_config.json")
with open(config_path, "r", encoding="utf-8") as f:
    config = json.load(f)

# Buscar archivos más recientes
validations_dir = os.path.join(ROOT, "validacions")
csv_files = sorted(glob.glob(os.path.join(validations_dir, "validation_report_*.csv")), 
                   key=os.path.getmtime, reverse=True)
pdf_files = sorted(glob.glob(os.path.join(validations_dir, "validation_report_*.pdf")), 
                   key=os.path.getmtime, reverse=True)

if not csv_files or not pdf_files:
    print("ERROR: No s'han trobat arxius de validació")
    sys.exit(1)

csv_path = csv_files[0]
pdf_path = pdf_files[0]

print(f"\nArxius a enviar:")
print(f"  CSV: {os.path.basename(csv_path)}")
print(f"  PDF: {os.path.basename(pdf_path)}")

# Extraer estadísticas del CSV
import pandas as pd
df = pd.read_csv(csv_path, sep=';', decimal=',', encoding='utf-8')
total = len(df)
ok = len(df[df['status'] == 'OK'])
errors = len(df[df['status'] == 'ERROR'])
discrepancies = len(df[df['status'] == 'DISCREPANCIA'])

summary_stats = {
    "total": total,
    "ok_perfect": ok,
    "ok_reset": 0,  # Se calcula en validación
    "discrepancies": discrepancies,
    "errors": errors
}

print(f"\nEstadístiques:")
print(f"  Total: {total}")
print(f"  OK: {ok}")
print(f"  Discrepàncies: {discrepancies}")
print(f"  Errors: {errors}")

# Obtener período del config
period_start = config["period"]["start"]
period_end = config["period"]["end"]

print(f"\nPeríode: {period_start} → {period_end}")
print(f"\nEnviant email a: {', '.join(config['email']['recipients'])}")

# Enviar email
try:
    from email_utils import send_validation_report
    
    send_validation_report(
        pdf_path=pdf_path,
        csv_path=csv_path,
        period_start=period_start,
        period_end=period_end,
        config_email=config["email"],
        summary_stats=summary_stats,
        logger=logger
    )
    
    print("\n✓ Email enviat correctament")
    
except Exception as e:
    print(f"\n✗ Error enviant email: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
