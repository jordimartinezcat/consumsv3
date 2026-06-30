# Estado del Proyecto - Consums_v3

**Última actualización**: 30 de junio de 2026  
**Versión actual**: v3.1  
**Estado**: ✅ **SISTEMA EN PRODUCCIÓN**

---

## 🎯 Estado Actual

El proyecto Consums_v3 está **completamente funcional y desplegado en producción** en el servidor Windows del CAT (Consorci d'Aigües de Tarragona).

### Características en Producción

**Pipeline Diario Automatizado**:
- ✅ Ejecuta diariamente a las 02:00 AM vía Windows Task Scheduler
- ✅ Procesa automáticamente el día anterior (ayer 00:00 → hoy 00:00)
- ✅ Descarga datos de ~238 señales desde API SagedCAT
- ✅ Procesa totalizadores 16-bit → 32-bit con per10 multiplier
- ✅ Calcula consumos con detección automática de anomalías y resets
- ✅ Inserta resultados en PostgreSQL (Azure)
- ✅ Genera validación diaria con PDF/CSV
- ✅ Envía email automático con informes adjuntos

**Validación Mensual Automática** (Nuevo - día 1 de cada mes):
- ✅ Se ejecuta automáticamente integrada en daily_report.py
- ✅ Valida el mes anterior completo contra vista PostgreSQL (fuente de verdad)
- ✅ Genera PDF + CSV mensuales
- ✅ Incluye adjuntos mensuales en email del día 1

**Calidad de Procesamiento**:
- Tasa de éxito típica: ~94.5% señales OK
- ~5.5% señales con errores (sin datos en API)
- Detección automática de resets de contador (65.536L)
- Corrección automática de anomalías

---

## 🚀 Últimos Desarrollos Completados

### Validación Mensual con PDF (Unreleased → pendiente versionar)
- **Qué**: Sistema completo de validación mensual automática
- **Cuándo**: Integrado recientemente, pendiente de versionar en CHANGELOG
- **Cómo funciona**:
  - Se ejecuta solo el día 1 de cada mes
  - Compara Tot(01/MM 00:00) vs Tot(01/MM+1 00:00) contra vista PG
  - Fuente: `ga_datalake.ite_v_consums_24h` (incluye rectificaciones manuales)
  - Genera PDF con estructura idéntica al diario (logo, estadísticas, secciones)
  - Adjunta CSV + PDF mensual en email del día 1
- **Archivos**:
  - `validacions/validate_monthly_consumption.py`
  - `run_monthly_validation.py`
  - `docs/VALIDACION_MENSUAL.md`
  - `docs/DIAGRAMA_VALIDACION_MENSUAL.md`

### per10 Multiplier (v0.4.0+)
- **Qué**: Aplicación automática de multiplicador x10 a 18 señales específicas
- **Dónde**: `adquisicion/run_compute_for_minutes.py`
- **Cuándo aplicar**: DESPUÉS de combinar TOT_L/TOT_H → TOT32, ANTES de calcular consumo
- **Fuente**: Flag `per10=True` en tabla `cfg_tags` (PostgreSQL)

### Sistema de Detección de Resets (v0.4.0)
- **Qué**: Detección y corrección automática de resets de contador
- **Dónde**: `procesado/compute_consumption.py` → `detect_counter_resets()`
- **Threshold**: Consumo < -1.000.000 (reset completo) o ≈-60.000L (reset 16-bit)
- **Acción**: Marca en columna `anomaly`, preserva valor original
- **Resultado**: Validación identifica resets de 65.536L o múltiplos

---

## 📋 Trabajo en Curso

**Ninguno actualmente** - el sistema está estable y operativo.

### Posibles mejoras futuras (no urgentes):
- [ ] Versionar validación mensual en CHANGELOG (mover [Unreleased] a v0.5.0)
- [ ] Crear tag Git v3.1 para versión actual
- [ ] Organizar scripts de análisis/debug (50+ archivos en root) → mover a `scripts/debug/`
- [ ] Dashboard web para visualización de validaciones (opcional)
- [ ] Alertas proactivas si tasa de errores > 10% (opcional)

---

## 🔍 Monitoreo y Mantenimiento

### Archivos a Revisar Regularmente

**Logs diarios**:
```
log/daily_report_YYYYMMDD.log
```
Revisar si hay errores en la ejecución del pipeline.

**Validaciones diarias**:
```
validacions/validation_report_YYYYMMDD_HHMMSS.pdf
validacions/validation_report_YYYYMMDD_HHMMSS.csv
```
Revisar % de señales OK vs errores.

**Validaciones mensuales** (día 1):
```
validacions/validation_monthly_YYYYMM_HHMMSS.pdf
validacions/validation_monthly_YYYYMM_HHMMSS.csv
```
Revisar discrepancias y resets acumulados del mes.

**Emails recibidos**:
- Verificar recepción diaria (02:00 AM aprox)
- Verificar día 1 incluye adjuntos mensuales (4 archivos: 2 diarios + 2 mensuales)

### Indicadores de Salud del Sistema

**✅ Sistema saludable**:
- Email recibido diariamente
- Tasa de éxito > 90%
- Logs sin errores críticos
- Validación mensual (día 1) con % OK alto

**⚠️ Requiere atención**:
- Tasa de éxito < 90%
- Aumento significativo de discrepancias
- Errores de conexión BD o API en logs
- Emails no recibidos

**❌ Problema crítico**:
- Pipeline no ejecutado (no hay log del día)
- Error en inserción PostgreSQL
- Token OAuth2 expirado (email falla)
- Más del 50% de señales con errores

---

## 🛠️ Operaciones Comunes

### Reprocesar un día específico

1. Editar `consums_config.json`:
```json
{
  "period": {
    "start": "2026-06-29 00:00:00",
    "end": "2026-06-30 00:00:00"
  }
}
```

2. Ejecutar pipeline:
```powershell
.\.venv\Scripts\Activate.ps1
python run_pipeline.py
```

3. Validar resultados:
```powershell
python validacions\validate_consumption.py
```

### Ejecutar validación mensual manualmente

```powershell
.\.venv\Scripts\Activate.ps1
python run_monthly_validation.py
```

### Reenviar último informe

```powershell
python send_last_report.py
```

### Extraer nuevas señales

```powershell
python adquisicion\extraer_senales_ftr.py
# Revisa: adquisicion/senales_para_descarga.txt
```

---

## 📊 Métricas de Producción

- **Señales procesadas**: ~238
- **Frecuencia**: Diaria (02:00 AM)
- **Tiempo ejecución típico**: 15-30 minutos
- **Tasa éxito**: ~94.5%
- **Uptime**: 100% (automatizado vía Task Scheduler)
- **Destino datos**: PostgreSQL Azure (`ga_datalake` schema)
- **Notificaciones**: Email automático con OAuth2

---

## 🔐 Seguridad y Credenciales

**Archivos sensibles** (nunca versionar):
- `consums_config.json` - Credenciales BD + API
- `token_cache.json` - Refresh token OAuth2

**Ubicación**: `.gitignore` configurado correctamente

**Token OAuth2**:
- Renovación automática vía MSAL
- Si expira: ejecutar `daily_report.py` manualmente para reautenticar
- Autorización interactiva solo necesaria la primera vez

---

## 📝 Próxima Sesión de Trabajo

Cuando trabajes en este proyecto en el futuro:

1. Lee `context/RULES.md` para recordar las 7 reglas de negocio críticas
2. Revisa `context/ARCHITECTURE.md` para entender el flujo de datos
3. Lee este archivo para conocer el estado actual
4. Revisa `CHANGELOG.md` para ver qué cambió recientemente
5. Consulta `.github/copilot-instructions.md` para patrones de código

**Antes de modificar código**:
- Asegúrate de entender qué regla de negocio afectas
- Revisa dónde vive la lógica exacta en `context/RULES.md`
- Actualiza CHANGELOG.md con el cambio (formato Keep a Changelog)
- Si afecta reglas de negocio, actualiza `context/RULES.md`
- Actualiza este archivo con el nuevo estado

---

## ✅ Resumen Ejecutivo

**Estado**: Sistema en producción, completamente funcional, sin problemas conocidos.

**Última característica agregada**: Validación mensual automática con PDF (pendiente versionar).

**Mantenimiento requerido**: Revisar logs y emails periódicamente. Sistema estable.

**Próximo hito sugerido**: Versionar v0.5.0 con validación mensual, crear tag Git v3.1.
