# Sistema de Automatización de Reportes Diarios - Consums v3

## 📋 Descripción

Sistema completo para la ejecución automática diaria del procesamiento de consumos de agua industrial, con validación y envío de reportes por email.

## 🔄 Flujo Automático

El script `daily_report.py` ejecuta automáticamente:

1. **Actualización de Configuración** ✏️
   - Calcula fechas del día anterior (ayer 00:00 → hoy 00:00)
   - Actualiza `consums_config.json` con el período

2. **Ejecución del Pipeline** ⚙️
   - Descarga datos de la API SagedCAT
   - Procesa totalizadores (16-bit → 32-bit)
   - Calcula consumos con detección de anomalías
   - Inserta en base de datos PostgreSQL

3. **Validación de Resultados** ✅
   - Compara consumos calculados vs. totalizadores API
   - Genera archivo CSV con todos los resultados
   - Genera PDF con reporte visual categorizado:
     * Señales correctas (OK)
     * Resets de contador detectados
     * Discrepancias a revisar
     * Errores (sin datos en API)

4. **Envío por Email** 📧
   - Envía email con PDF y CSV adjuntos
   - Resumen ejecutivo en el cuerpo
   - OAuth2 para autenticación segura

## 🚀 Uso

### Ejecución Manual

```powershell
# Activar entorno virtual
.\.venv\Scripts\Activate.ps1

# Ejecutar proceso diario
python daily_report.py
```

El script:
- Procesa automáticamente el día anterior
- Genera logs en `log/daily_report_YYYYMMDD.log`
- Retorna código de salida 0 si todo OK, 1 si error

### Ejecución Programada (Automática)

Configura Windows Task Scheduler para ejecutar diariamente:

**Configuración recomendada**:
- **Hora**: 02:00 AM (cuando los datos del día anterior están completos)
- **Programa**: `C:\Python313\python.exe`
- **Argumentos**: `daily_report.py`
- **Iniciar en**: `D:\Projects\Python\Consums_v3`
- **Ejecutar**: Aunque el usuario no haya iniciado sesión

Ver [documentación detallada](docs/EMAIL_SETUP.md#programación-automática) para más opciones.

## ⚙️ Configuración

### 1. Base de Datos y API

Ya configurado en `consums_config.json`:
- PostgreSQL en Azure: `40.85.79.213:5432`
- SagedCAT API: `https://sagedcat-nex0-vm.xylemvue.goaigua.com:56443/api`

### 2. Email OAuth2

**⚠️ IMPORTANTE**: Requiere configuración inicial interactiva (solo una vez)

Sigue la guía completa: [docs/EMAIL_SETUP.md](docs/EMAIL_SETUP.md)

**Pasos rápidos**:

1. Registra una aplicación en [Azure Portal](https://portal.azure.com)
2. Obtén el **Client ID**
3. Configura permisos: `SMTP.Send` (delegado)
4. Actualiza `consums_config.json`:

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

5. Primera ejecución manual (device code flow):
```powershell
python daily_report.py
```
Sigue las instrucciones en pantalla para autorizar.

6. ✅ Ejecuciones posteriores serán automáticas (sin intervención)

### 3. Dependencias

Instala todas las dependencias necesarias:

```powershell
pip install msal reportlab pandas sqlalchemy psycopg2
```

## 📊 Estructura del Reporte

### Email Enviado

**Asunto**: `[Consums] Reporte de Validación - 2026-05-19`

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
```

**Adjuntos**:
- `validation_report_YYYYMMDD_HHMMSS.pdf` - Reporte visual
- `validation_report_YYYYMMDD_HHMMSS.csv` - Datos completos

### PDF Generado

El PDF incluye:

**📄 Página 1 - Resumen**
- Logo de la empresa
- Estadísticas generales
- Tabla resumen con porcentajes

**🔴 Sección 1 - Errores**
- Señales sin datos en API
- Mensaje de error por señal

**🟠 Sección 2 - Resets Detectados**
- Resets de 65,536L (contador LOW)
- **Ya corregidos** en el cálculo horario
- Tabla con valores y errores relativos

**🔵 Sección 3 - Otras Discrepancias**
- Señales con diferencias no categorizadas
- Requieren análisis individual

## 📁 Estructura de Archivos

```
Consums_v3/
├── daily_report.py                 # Script principal de automatización
├── run_pipeline.py                 # Pipeline de procesado
├── consums_config.json             # Configuración central
├── token_cache.json                # Token OAuth2 (generado automáticamente)
│
├── email_utils/                    # Módulo de email
│   ├── __init__.py
│   ├── oauth2.py                   # Autenticación OAuth2
│   └── sender.py                   # Envío de emails
│
├── validacions/                    # Validación y reportes
│   ├── validate_consumption.py     # Script de validación
│   ├── generate_validation_report.py  # Generador de PDF
│   ├── validation_report_*.csv     # Resultados CSV
│   └── validation_report_*.pdf     # Reportes PDF
│
├── log/                            # Logs de ejecución
│   └── daily_report_YYYYMMDD.log
│
└── docs/                           # Documentación
    └── EMAIL_SETUP.md              # Guía de configuración email
```

## 🔍 Logs y Monitoreo

### Logs Diarios

Cada ejecución genera un log detallado:

```
log/daily_report_20260522.log
```

Contiene:
- Timestamps de cada etapa
- Output de pipeline y validación
- Errores y warnings
- Confirmación de envío de email

### Monitoreo de Estado

Verifica el código de salida para automatización:

```powershell
python daily_report.py
echo $LASTEXITCODE
# 0 = Éxito
# 1 = Error
```

## 🛠️ Troubleshooting

### Pipeline Falla

**Síntomas**: Error en ejecución de `run_pipeline.py`

**Revisiones**:
1. Verificar conectividad con BD: `telnet 40.85.79.213 5432`
2. Verificar API disponible: `curl https://sagedcat-nex0-vm.xylemvue.goaigua.com:56443/api`
3. Revisar logs en `log/daily_report_*.log`

### Validación con Discrepancias

**Síntomas**: Muchas señales con discrepancias en validación

**Posibles causas**:
1. Resets no detectados (revisar umbral en `compute_consumption.py`)
2. Datos faltantes en período consultado
3. Cambio en formato de datos API

**Acción**: Revisar sección de discrepancias en PDF y analizar patrones

### Email No Llega

**Síntomas**: Proceso completa OK pero no se recibe email

**Revisiones**:
1. Verificar `email.enabled = true` en config
2. Verificar recipients correctos
3. Revisar carpeta spam/correo no deseado
4. Verificar token OAuth2: `ls token_cache.json`
5. Si token expirado: eliminar y volver a autorizar

### Token OAuth2 Expirado

**Síntomas**: Error "XOAUTH2 failed"

**Solución**:
```powershell
rm token_cache.json
python daily_report.py  # Reautorizar interactivamente
```

## 🔒 Seguridad

### Archivos Sensibles

Añadidos a `.gitignore`:
- ✅ `token_cache.json` - Contiene refresh token
- ✅ `consums_config.json` - Contiene credenciales BD y API

### Buenas Prácticas

1. **No compartir** token_cache.json
2. **Rotar credenciales** periódicamente
3. **Monitorear logs** para detectar intentos de acceso no autorizado
4. **Backups** de `consums_config.json` en ubicación segura
5. **Permisos** del archivo config solo para usuario de ejecución

## 📞 Soporte

Para problemas o consultas:

1. Revisar logs en `log/daily_report_*.log`
2. Consultar [docs/EMAIL_SETUP.md](docs/EMAIL_SETUP.md)
3. Verificar estado de servicios externos (BD, API)

## 📝 Changelog

### v3.1 - 2026-05-22
- ✅ Sistema de automatización completo
- ✅ Envío de email con OAuth2
- ✅ Generación automática de PDF
- ✅ Validación con timestamp corregido (00:00 día siguiente)
- ✅ Fix: Prioridad de archivos con detección de anomalías
- ✅ Documentación completa

---

**Sistema listo para producción** ✨
