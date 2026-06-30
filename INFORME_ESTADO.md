# INFORME DE ESTADO - Consums_v3
## Fecha de análisis: 30 de junio de 2026

---

## 📋 RESUMEN EJECUTIVO

**Estado actual**: ✅ **SISTEMA EN PRODUCCIÓN (v3.1)**

El proyecto Consums_v3 es un sistema de monitorización de consumos de agua industrial del CAT (Consorci d'Aigües de Tarragona) que está completamente funcional y desplegado en producción.

### Características principales implementadas:
- ✅ Pipeline diario automatizado (Windows Task Scheduler @ 02:00 AM)
- ✅ Validación diaria de consumos
- ✅ Validación mensual automática (día 1 de cada mes)
- ✅ Generación de informes PDF/CSV en català
- ✅ Envío automático por email con OAuth2 (Microsoft)
- ✅ Detección y corrección de resets de contadores (65.536L)
- ✅ Persistencia en PostgreSQL (Azure)
- ✅ Procesamiento per10 multiplier (18 señales)

---

## 📚 DOCUMENTACIÓN EXISTENTE

### ✅ Documentación completa y actualizada:

1. **README.md** (380+ líneas)
   - Descripción completa del sistema
   - Arquitectura y flujo de datos
   - Componentes principales
   - Guía de automatización
   - Instrucciones de configuración
   - Sistema de validación (diaria y mensual)
   - Ejemplos de uso

2. **CHANGELOG.md** (150+ líneas)
   - Formato: Keep a Changelog
   - Historial completo desde v0.1.0 hasta v0.4.0
   - Sección [Unreleased] con validación mensual
   - Cambios documentados: Added, Changed, Fixed
   - Referencias a módulos específicos

3. **.github/copilot-instructions.md** (100+ líneas)
   - Descripción del proyecto
   - Arquitectura y flujo de datos
   - Patrones críticos (imports, configuración)
   - Workflow de desarrollo
   - Operaciones comunes
   - Anti-patterns a evitar

4. **docs/VALIDACION_MENSUAL.md**
   - Guía completa de validación mensual
   - Ejemplos prácticos de uso
   - Ejecución automática y manual
   - Descripción de archivos generados

5. **docs/DIAGRAMA_VALIDACION_MENSUAL.md**
   - Diagrama ASCII completo del flujo
   - Arquitectura de validación mensual
   - Consultas API y PostgreSQL
   - Criterios de validación

6. **docs/AUTOMATIZACION.md**
   - Configuración Windows Task Scheduler
   - Parámetros de ejecución
   - Troubleshooting

7. **docs/EMAIL_SETUP.md**
   - Configuración OAuth2 Microsoft
   - Azure Portal setup
   - Troubleshooting email

8. **docs/per10_multiplier.md**
   - Documentación técnica del multiplicador per10
   - Lógica de aplicación
   - Señales afectadas

---

## 🏗️ ARQUITECTURA DEL SISTEMA

### Pipeline de procesamiento:

```
1. EXTRACCIÓN (adquisicion/)
   └─ extraer_senales_ftr.py       → Extrae señales TOT_L/TOT_H desde PostgreSQL
   └─ download_minute_data.py      → Descarga datos minutales desde API SagedCAT
   └─ run_compute_for_minutes.py   → Combina 16-bit → 32-bit + aplica per10

2. PROCESAMIENTO (procesado/)
   └─ compute_consumption.py       → Calcula consumo + detecta anomalías/resets
   └─ run_compute_consumption.py   → Script ejecución (output: Data/)
   └─ compute_hourly_consumption.py → Agregación horaria
   └─ run_hourly_aggregation.py    → Script agregación

3. PERSISTENCIA (persistencia/)
   └─ save_hourly_consumption.py   → DELETE + INSERT bulk a PostgreSQL

4. VALIDACIÓN (validacions/)
   └─ validate_consumption.py      → Validación diaria vs API
   └─ validate_monthly_consumption.py → Validación mensual vs vista PostgreSQL
   └─ generate_validation_report.py → Generación PDF (català)

5. EMAIL (email_utils/)
   └─ oauth2.py                    → Auth OAuth2 Microsoft (MSAL)
   └─ sender.py                    → Envío HTML + firma corporativa

6. ORQUESTACIÓN
   └─ daily_report.py              → Script principal automatizado
   └─ run_pipeline.py              → Pipeline completo manual
   └─ run_monthly_validation.py    → Validación mensual manual
```

### Flujo de datos:
```
API SagedCAT → adquisicion/minute_data/ → procesado/Data/ → PostgreSQL (Azure)
                                                           ↘
                                                            validacions/ → PDF/CSV → Email
```

---

## 🔑 REGLAS DE NEGOCIO CRÍTICAS

### 1. **per10 multiplier** (18 señales)
   - **Ubicación**: `adquisicion/run_compute_for_minutes.py`
   - **Lógica**: Multiplica totalizadores x10 DESPUÉS de combinar H/L y ANTES de calcular consumo
   - **Fuente**: Flag `per10=True` en tabla `cfg_tags` (PostgreSQL)

### 2. **Detección de resets**
   - **Ubicación**: `procesado/compute_consumption.py` → `detect_counter_resets()`
   - **Threshold**: Consumo < -1.000.000 (reset completo) o ≈-60.000L (reset 16-bit = 65.536L)
   - **Acción**: Marca en columna `anomaly`, preserva valor original

### 3. **Última hora del día**
   - Requiere totalizador del minuto 00:00 del día SIGUIENTE para calcular consumo 23:00
   - El período se extiende pero se filtra (timezone-aware) para no insertar la hora 00:00 extra

### 4. **Timestamps de validación**
   - Consultan totalizador a las 00:00 del día siguiente (no 23:59:00)
   - Consistente con cómo se calculó el consumo

### 5. **Encoding Windows Server**
   - `subprocess` debe usar `cp1252` con `errors='replace'`
   - Caracteres Unicode (✓✗→) evitarse en consola → usar [OK]/[ERROR] ASCII

### 6. **Permisos PostgreSQL**
   - Usuario de app puede no ser owner de tablas
   - Capturar `ProgrammingError` / `InsufficientPrivilege` al crear índices
   - Continuar ejecución sin fallar el pipeline

### 7. **Idioma català**
   - TODO output orientado a usuario (PDF, email, logs validación) en català
   - Código y comentarios técnicos pueden estar en castellano/inglés

---

## 📊 VALIDACIÓN

### Validación DIARIA (todos los días)
- **Período**: Ayer 00:00 → Hoy 00:00
- **Comparación**: Tot(día+1 00:00) - Tot(día 00:00) vs Σ(consumos 24h)
- **Salida**: PDF + CSV diarios
- **Ejecución**: Automática vía `daily_report.py` @ 02:00 AM

### Validación MENSUAL (día 1 de cada mes)
- **Período**: 01/MM 00:00 → 01/(MM+1) 00:00
- **Fuente de verdad**: Vista PostgreSQL `ga_datalake.ite_v_consums_24h` (incluye rectificaciones)
- **Comparación**: Tot(01/mes+1) - Tot(01/mes) vs Σ(consumos del mes desde vista PG)
- **Criterios**:
  - ✅ OK perfecto: error < 0.5% O diferencia < 100L
  - ✅ OK con resets: diferencia ≈ múltiplo de 65.536L
  - ⚠️ Discrepancia: resto de casos
  - ❌ Error: señal sin datos en API
- **Salida**: CSV + PDF mensuales (`validation_monthly_YYYYMM_*.csv` y `.pdf`)
- **Ejecución**: Automática integrada en `daily_report.py` (solo día 1)

---

## 🔧 STACK TECNOLÓGICO

- **Python**: 3.13
- **Base de datos**: PostgreSQL (Azure) vía psycopg v3
- **API**: SagedCAT REST API (autenticación nexustoken)
- **Procesamiento**: pandas
- **PDF**: ReportLab
- **Email**: smtplib + MSAL (OAuth2 Microsoft)
- **Automatización**: Windows Task Scheduler
- **Submódulo**: CAT_Conexions (branch `wip/consums-mods`) para `pgDataLake` y `apiSagedCAT`

---

## 📂 ESTRUCTURA DE DIRECTORIOS

```
Consums_v3/
├── .github/
│   ├── agents/              # Agentes Copilot personalizados
│   ├── copilot-instructions.md  # ✅ Instrucciones generales
│   └── skills/              # Skills FastAPI
│
├── docs/                    # ✅ Documentación completa
│   ├── AUTOMATIZACION.md
│   ├── DIAGRAMA_VALIDACION_MENSUAL.md
│   ├── EMAIL_SETUP.md
│   ├── per10_multiplier.md
│   └── VALIDACION_MENSUAL.md
│
├── adquisicion/             # Extracción y descarga
├── procesado/               # Cálculo consumos + anomalías
├── persistencia/            # Inserción PostgreSQL
├── validacions/             # Validación + informes PDF
├── email_utils/             # OAuth2 + envío email
│
├── CAT_Conexions/           # Submódulo Git (conexiones)
├── assets/                  # Logo corporativo
├── log/                     # Logs de ejecución
│
├── daily_report.py          # ✅ Script principal automatizado
├── run_pipeline.py          # ✅ Pipeline completo manual
├── run_monthly_validation.py # ✅ Validación mensual manual
│
├── consums_config.json      # ⚠️ Config (NO versionar - .gitignore)
├── token_cache.json         # ⚠️ OAuth2 token (NO versionar)
├── README.md                # ✅ Documentación principal
└── CHANGELOG.md             # ✅ Historial de cambios
```

---

## ❌ DOCUMENTACIÓN FALTANTE

Según las instrucciones del agente `Consums-agent`, se debería crear la siguiente estructura de contexto persistente:

### Carpeta `context/` (NO EXISTE)

```
context/
├── PROJECT.md       # Estado actual, qué se está trabajando
├── RULES.md         # Las 7 reglas de negocio en detalle
├── ARCHITECTURE.md  # Diagrama flujo + responsabilidad módulos
└── DECISIONS.md     # Decisiones técnicas relevantes
```

**Nota**: Esta documentación adicional es para mantener contexto persistente entre sesiones de trabajo con Copilot, pero el sistema está completamente documentado en los archivos existentes.

---

## ✅ ESTADO DE PRODUCCIÓN

### Características en producción:
1. ✅ Pipeline diario automatizado (Task Scheduler @ 02:00 AM)
2. ✅ Descarga automática desde API SagedCAT
3. ✅ Procesamiento 16→32 bit con per10 multiplier
4. ✅ Cálculo de consumos con detección de anomalías
5. ✅ Detección y corrección automática de resets (65.536L)
6. ✅ Agregación horaria con correcciones
7. ✅ Inserción bulk a PostgreSQL (Azure)
8. ✅ Validación diaria automática
9. ✅ Validación mensual automática (día 1)
10. ✅ Generación PDF/CSV en català
11. ✅ Envío email automático OAuth2
12. ✅ Logging completo

### Versión actual: **v3.1** (según documentación)

### Última actualización CHANGELOG:
- **[Unreleased]**: Validación mensual con PDF + integración email
- **[0.4.0]**: Sistema de detección y corrección de resets
- **[0.3.0]**: Agregación horaria
- **[0.2.1]**: Limpieza de código
- **[0.1.0]**: Extracción de señales TOT_L/TOT_H

---

## 🎯 PRÓXIMOS PASOS SUGERIDOS

### 1. Crear estructura `context/` (opcional pero recomendado)
   - `context/PROJECT.md`: Estado actual post-producción
   - `context/RULES.md`: Detalle de las 7 reglas con código exacto
   - `context/ARCHITECTURE.md`: Diagrama + tabla responsabilidades
   - `context/DECISIONS.md`: Decisiones técnicas (psycopg v3, OAuth2, etc.)

### 2. Actualizar CHANGELOG.md
   - Mover [Unreleased] a versión específica (ej: [0.5.0])
   - Documentar fecha de release de validación mensual

### 3. Crear tag Git para versión actual
   ```bash
   git tag -a v3.1 -m "Release v3.1 - Sistema en producción con validación mensual"
   git push origin v3.1
   ```

### 4. Monitorización y mantenimiento
   - Revisar logs diarios en `log/daily_report_YYYYMMDD.log`
   - Verificar emails recibidos
   - Monitorear validaciones mensuales (día 1 de cada mes)

---

## 🔍 ANÁLISIS DE CALIDAD DEL CÓDIGO

### Patrones consistentes encontrados:
- ✅ Logging extensivo en todos los módulos
- ✅ Manejo de errores con try/except
- ✅ Encoding Windows (cp1252) en subprocess
- ✅ Timezone-aware timestamps
- ✅ Configuración centralizada (consums_config.json)
- ✅ Separación de responsabilidades por módulos
- ✅ Scripts de ejecución independientes
- ✅ Documentación en código

### Archivos de análisis/debug (alta cantidad):
- ⚠️ 50+ archivos `analyze_*.py`, `check_*.py`, `verify_*.py`, `debug_*.py` en root
- **Recomendación**: Mover a carpeta `scripts/debug/` o `scripts/analysis/` para mantener limpio el root

---

## 📈 MÉTRICAS DEL PROYECTO

- **Módulos principales**: 6 (adquisicion, procesado, persistencia, validacions, email_utils, CAT_Conexions)
- **Scripts de ejecución**: 3 principales (daily_report, run_pipeline, run_monthly_validation)
- **Documentación**: 8+ archivos markdown
- **Líneas de documentación**: 1000+ (README + docs)
- **Señales procesadas**: ~238 (según ejemplos en README)
- **Tasa de éxito típica**: ~94.5% OK, ~5.5% errores (sin datos API)

---

## ✅ CONCLUSIÓN

El proyecto **Consums_v3** está en un estado **excelente**:

1. ✅ **Completamente funcional** en producción
2. ✅ **Documentación exhaustiva** y bien estructurada
3. ✅ **Código bien organizado** con separación clara de responsabilidades
4. ✅ **Automatización completa** (diaria + mensual)
5. ✅ **Validación robusta** con informes profesionales
6. ✅ **Mantenibilidad alta** gracias a logs y configuración centralizada

### Únicos puntos de mejora sugeridos:
- Crear carpeta `context/` para documentación persistente de Copilot (opcional)
- Organizar scripts de debug/análisis en subcarpeta
- Versionar release actual en CHANGELOG (mover [Unreleased] a v0.5.0 o similar)

**El sistema no requiere ningún cambio urgente y está listo para continuar operando en producción.**
