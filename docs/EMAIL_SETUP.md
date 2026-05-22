# Configuración de Email con OAuth2

## Requisitos

El sistema usa **OAuth2** para autenticación con Microsoft Outlook/Hotmail. Necesitas registrar una aplicación en Azure Portal.

## Pasos de Configuración

### 1. Registrar Aplicación en Azure Portal

1. Ve a [Azure Portal](https://portal.azure.com)
2. Busca "Registros de aplicaciones" (App registrations)
3. Click en **"Nueva registro"**
4. Configura:
   - **Nombre**: `Consums Email Service` (o el que prefieras)
   - **Tipos de cuenta admitidos**: "Cuentas en cualquier directorio de organización y cuentas Microsoft personales"
   - **URI de redirección**: Plataforma "Móvil y escritorio" → URL: `https://login.microsoftonline.com/common/oauth2/nativeclient`
5. Click en **"Registrar"**

### 2. Anotar el Client ID

1. En la página de tu aplicación, ve a **"Información general"**
2. Copia el **"Id. de aplicación (cliente)"** - es un GUID tipo `XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX`
3. Guárdalo, lo necesitarás para el config

### 3. Configurar Permisos

1. Ve a **"Permisos de API"**
2. Click en **"Agregar un permiso"**
3. Selecciona **"APIs de Microsoft"** → **"Microsoft Graph"** → **NO**, mejor **"Office 365 Exchange Online"** o busca **"Outlook"**
   - En realidad debes buscar: **APIs que usa mi organización** → `Office 365 Exchange Online`
   - O directamente en la sección **"APIs de Microsoft"**, busca los scopes:
     - `SMTP.Send`
4. Agrega permisos **Delegados** (no de aplicación):
   - `SMTP.Send`
5. Click en **"Agregar permisos"**
6. **IMPORTANTE**: Si tu organización requiere consentimiento del administrador, pide al admin que lo apruebe

### 4. Actualizar `consums_config.json`

Edita el archivo `consums_config.json` y actualiza la sección `email`:

```json
{
  "email": {
    "enabled": true,
    "smtp_server": "smtp.office365.com",
    "smtp_port": 587,
    "smtp_user": "tu_correo@outlook.com",
    "smtp_tls": true,
    "oauth2_client_id": "TU-CLIENT-ID-AQUI",
    "oauth2_token_cache": "token_cache.json",
    "from_addr": "tu_correo@outlook.com",
    "recipients": [
      "destinatario1@example.com",
      "destinatario2@example.com"
    ]
  }
}
```

### 5. Primera Autenticación (Interactiva)

La **primera vez** que ejecutes el sistema, se te pedirá que autorices la aplicación:

```powershell
python daily_report.py
```

Verás algo como:

```
============================================================
  AUTORIZACIÓN OAUTH2 REQUERIDA (solo la primera vez)
============================================================
  1. Abre en el navegador: https://microsoft.com/devicelogin
  2. Introduce el código:  ABCD-EFGH
============================================================
```

1. Abre el navegador en la URL indicada
2. Introduce el código mostrado
3. Autoriza con tu cuenta de Outlook
4. El sistema guardará el **refresh token** en `token_cache.json`

### 6. Ejecuciones Posteriores (Automáticas)

Después de la primera autorización, el sistema se ejecutará **desatendido** sin intervención humana. El refresh token se renovará automáticamente.

⚠️ **IMPORTANTE**: El archivo `token_cache.json` contiene credenciales sensibles. Añádelo a `.gitignore`.

## Programación Automática

### Windows Task Scheduler

1. Abre **Programador de tareas** (Task Scheduler)
2. Crea tarea básica:
   - **Nombre**: `Consums Daily Report`
   - **Desencadenador**: Diario, a las 02:00 AM
   - **Acción**: Iniciar programa
     - **Programa**: `C:\Python313\python.exe` (ruta a tu Python)
     - **Argumentos**: `daily_report.py`
     - **Iniciar en**: `D:\Projects\Python\Consums_v3`
3. Configura para ejecutar aunque el usuario no haya iniciado sesión
4. Ejecutar con privilegios más altos si es necesario

### Linux/Mac (Cron)

```bash
# Editar crontab
crontab -e

# Añadir línea (ejecutar a las 2 AM diariamente)
0 2 * * * cd /path/to/Consums_v3 && /usr/bin/python3 daily_report.py >> /path/to/log/cron.log 2>&1
```

## Verificación

### Test Manual del Email

Puedes probar el envío de email sin ejecutar todo el pipeline:

```python
import json
import logging
from email_utils import send_validation_report

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test")

# Cargar config
with open("consums_config.json", "r") as f:
    config = json.load(f)

# Usar archivos existentes
csv_path = "validacions/validation_report_YYYYMMDD_HHMMSS.csv"
pdf_path = "validacions/validation_report_YYYYMMDD_HHMMSS.pdf"

summary_stats = {
    "total": 238,
    "ok_perfect": 204,
    "ok_reset": 21,
    "discrepancies": 1,
    "errors": 12
}

send_validation_report(
    pdf_path=pdf_path,
    csv_path=csv_path,
    period_start="2026-05-19 00:00:00",
    period_end="2026-05-20 00:00:00",
    config_email=config["email"],
    summary_stats=summary_stats,
    logger=logger
)
```

## Logs

Los logs de cada ejecución se guardan en:

```
log/daily_report_YYYYMMDD.log
```

Revisa estos archivos para diagnosticar problemas.

## Troubleshooting

### Error: "Token OAuth2 no encontrado"

**Solución**: Ejecuta manualmente `python daily_report.py` una vez para completar el device code flow.

### Error: "XOAUTH2 failed"

**Causas posibles**:
1. Client ID incorrecto
2. Permisos no configurados correctamente en Azure
3. Token expirado sin refresh token válido

**Solución**: Elimina `token_cache.json` y vuelve a autorizar.

### Error: "SMTP Connection refused"

**Solución**: Verifica que `smtp_server` y `smtp_port` sean correctos para tu proveedor.

### Email no llega

1. Verifica la configuración de `recipients`
2. Revisa la carpeta de spam/correo no deseado
3. Verifica los logs en `log/daily_report_*.log`

## Seguridad

- ✅ `consums_config.json` - Añadir a `.gitignore` (contiene credenciales DB)
- ✅ `token_cache.json` - Añadir a `.gitignore` (contiene refresh token)
- ✅ No compartir el Client ID públicamente si es de producción
- ✅ Rotar credenciales periódicamente

## Estructura del Email Enviado

**Asunto**: `[Consums] Reporte de Validación - YYYY-MM-DD`

**Cuerpo**:
```
Reporte de Validación de Consumos - 2026-05-19

Período: 2026-05-19 00:00:00 → 2026-05-20 00:00:00

======================================================================
RESUMEN DE VALIDACIÓN
======================================================================
Total señales procesadas: 238
  ✓ OK (perfectas):         204 (85.7%)
  ✓ OK (con reset 16-bit):  21 (8.8%)
  ⚠ Discrepancias:          1 (0.4%)
  ✗ Errores (sin datos):    12 (5.0%)

El reporte detallado se adjunta en formato PDF.
Los datos completos están disponibles en el archivo CSV adjunto.

---
Sistema automático de validación de consumos
Generado: 2026-05-22 10:30:15
```

**Adjuntos**:
- `validation_report_YYYYMMDD_HHMMSS.pdf` - Reporte visual con tablas
- `validation_report_YYYYMMDD_HHMMSS.csv` - Datos completos para análisis
