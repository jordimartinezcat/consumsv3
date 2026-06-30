# Arquitectura del Sistema - Consums_v3

**Sistema**: Monitorización de Consumos de Agua Industrial CAT  
**Arquitectura**: Pipeline ETL (Extract, Transform, Load) con validación y notificación

---

## 🏗️ Diagrama de Flujo General

```
┌────────────────────────────────────────────────────────────────────────┐
│                         CONSUMS_V3 PIPELINE                             │
│                    (Ejecuta diariamente @ 02:00 AM)                     │
└────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  1. CONFIGURACIÓN (daily_report.py)                                     │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────┐         │
│  │  • Calcula período: ayer 00:00 → hoy 00:00               │         │
│  │  • Actualiza consums_config.json                          │         │
│  │  • Detecta si es día 1 del mes                            │         │
│  └───────────────────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  2. EXTRACCIÓN (adquisicion/)                                           │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────┐         │
│  │  extraer_senales_ftr.py                                    │         │
│  │  • Consulta PostgreSQL: cfg_tags                           │         │
│  │  • Filtra: *_TOT_L y *_TOT_H                              │         │
│  │  • Excluye: ET*, *_LS_*, *_P_*                            │         │
│  │  • Output: senales_para_descarga.txt (~238 señales)       │         │
│  └───────────────────────────────────────────────────────────┘         │
│                            │                                             │
│                            ▼                                             │
│  ┌───────────────────────────────────────────────────────────┐         │
│  │  download_minute_data.py                                   │         │
│  │  • Lee: senales_para_descarga.txt                          │         │
│  │  • API SagedCAT: GET historic data (1-minute resolution)   │         │
│  │  • Descarga TOT_L (16-bit low) y TOT_H (16-bit high)      │         │
│  │  • Output: minute_data/*.csv (archivos por señal)         │         │
│  └───────────────────────────────────────────────────────────┘         │
│                            │                                             │
│                            ▼                                             │
│  ┌───────────────────────────────────────────────────────────┐         │
│  │  run_compute_for_minutes.py                                │         │
│  │  • Combina: TOT_L + TOT_H → TOT32 (32-bit total)          │         │
│  │  • Consulta per10: cfg_tags.per10                          │         │
│  │  • Aplica multiplicador x10 (18 señales)                  │         │
│  │  • Output: minute_data/combined_minute_data.csv           │         │
│  └───────────────────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  3. PROCESAMIENTO (procesado/)                                          │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────┐         │
│  │  run_compute_consumption.py                                │         │
│  │  ├─ compute_consumption.py                                 │         │
│  │  │   ├─ append_minute_consumption()                        │         │
│  │  │   │   • Calcula: TOT32[t+1] - TOT32[t] = consumo       │         │
│  │  │   │   • Output columna: *_cons                          │         │
│  │  │   │                                                      │         │
│  │  │   ├─ attach_anomalies_to_df()                          │         │
│  │  │   │   • Detecta pares: consumo negativo + positivo     │         │
│  │  │   │   • Redistribuye en minutos con TOT=0              │         │
│  │  │   │   • Output columna: *_anom                         │         │
│  │  │   │                                                      │         │
│  │  │   └─ detect_counter_resets()                           │         │
│  │  │       • Detecta consumos < -1M (reset completo)        │         │
│  │  │       • Detecta ≈-65.536L (reset 16-bit)               │         │
│  │  │       • Marca corrección en *_anom                     │         │
│  │  │                                                          │         │
│  │  • Output: consumption_minutes_YYYYMMDD_HHMMSS.csv        │         │
│  └───────────────────────────────────────────────────────────┘         │
│                            │                                             │
│                            ▼                                             │
│  ┌───────────────────────────────────────────────────────────┐         │
│  │  run_hourly_aggregation.py                                 │         │
│  │  ├─ compute_hourly_consumption.py                          │         │
│  │  │   • Resamplea: 1min → 1hour                            │         │
│  │  │   • Suma consumos originales (*_cons)                  │         │
│  │  │   • Suma correcciones (*_anom)                         │         │
│  │  │   • Combina: *_cons_sum + *_anom_sum = consumo final  │         │
│  │  │   • Genera: *_has_corrections flag                     │         │
│  │  │                                                          │         │
│  │  • Output: consumption_hourly_YYYYMMDD_HHMMSS.csv         │         │
│  └───────────────────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  4. PERSISTENCIA (persistencia/)                                        │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────┐         │
│  │  save_hourly_consumption.py                                │         │
│  │  • Lee: consumption_hourly_*.csv                           │         │
│  │  • PostgreSQL Azure:                                       │         │
│  │    ├─ DELETE: período específico                          │         │
│  │    └─ INSERT: bulk (psycopg v3 COPY)                      │         │
│  │  • Tabla: ga_datalake.ite_tags_consums                     │         │
│  │  • Columnas: timestamp_utc, id_tag, consum                 │         │
│  └───────────────────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  5. VALIDACIÓN DIARIA (validacions/)                                    │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────┐         │
│  │  validate_consumption.py                                   │         │
│  │  Para cada señal:                                          │         │
│  │    1. API: Tot(día 00:00) y Tot(día+1 00:00)             │         │
│  │    2. diff_api = Tot_final - Tot_initial                  │         │
│  │    3. PostgreSQL: Σ(consumos 24h) desde ite_tags_consums  │         │
│  │    4. Compara: diff_api vs sum_consumption                │         │
│  │    5. Clasifica:                                           │         │
│  │       • OK perfecte (diff < 0.01% o < 1L)                 │         │
│  │       • OK amb reset (diff ≈ 65.536L o múltiplos)         │         │
│  │       • Discrepància (resto)                              │         │
│  │       • Error (sin datos API)                             │         │
│  │                                                            │         │
│  │  • Output: validation_report_YYYYMMDD_HHMMSS.csv          │         │
│  └───────────────────────────────────────────────────────────┘         │
│                            │                                             │
│                            ▼                                             │
│  ┌───────────────────────────────────────────────────────────┐         │
│  │  generate_validation_report.py                             │         │
│  │  • Lee: validation_report_*.csv                            │         │
│  │  • Genera PDF (ReportLab):                                │         │
│  │    ├─ Logo corporativo (assets/logo.jpg)                  │         │
│  │    ├─ Estadísticas resumen (% OK, errores, etc.)          │         │
│  │    ├─ Sección 1: Errors (sin datos API)                   │         │
│  │    ├─ Sección 2: Resets detectats (65.536L)               │         │
│  │    └─ Sección 3: Discrepàncies                            │         │
│  │  • Formato: A4 horizontal, català                         │         │
│  │                                                            │         │
│  │  • Output: validation_report_YYYYMMDD_HHMMSS.pdf          │         │
│  └───────────────────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                         ┌─────────┴─────────┐
                         │  ¿Es día 1 mes?   │
                         └─────────┬─────────┘
                                   │
                         ┌─────────┴─────────┐
                    NO ──┤                   ├── SÍ
                         │                   │
                         ▼                   ▼
              ┌──────────────────┐  ┌──────────────────────────────┐
              │  Saltar validación│  │  6. VALIDACIÓN MENSUAL       │
              │  mensual          │  │                              │
              └──────────────────┘  │  validate_monthly_consumption│
                         │          │  • Período: 01/MM → 01/MM+1  │
                         │          │  • API: Tot inicial y final  │
                         │          │  • PostgreSQL: vista         │
                         │          │    ite_v_consums_24h         │
                         │          │    (con rectificaciones)     │
                         │          │  • Suma todo el mes          │
                         │          │  • Compara y clasifica       │
                         │          │                              │
                         │          │  Output:                     │
                         │          │  • validation_monthly_*.csv  │
                         │          │  • validation_monthly_*.pdf  │
                         │          └──────────────────────────────┘
                         │                   │
                         └─────────┬─────────┘
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  7. NOTIFICACIÓN (email_utils/)                                         │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────┐         │
│  │  sender.py                                                 │         │
│  │  ├─ oauth2.py                                              │         │
│  │  │   • MSAL: Autenticación Microsoft OAuth2               │         │
│  │  │   • Token refresh automático                           │         │
│  │  │   • Cache: token_cache.json                            │         │
│  │  │                                                          │         │
│  │  • Genera email HTML:                                      │         │
│  │    ├─ Cuerpo: Estadísticas resumen en català              │         │
│  │    ├─ Firma: firma.html (corporativa)                     │         │
│  │    └─ Adjuntos:                                            │         │
│  │       • validation_report_*.pdf (diario)                   │         │
│  │       • validation_report_*.csv (diario)                   │         │
│  │       • validation_monthly_*.pdf (si día 1)                │         │
│  │       • validation_monthly_*.csv (si día 1)                │         │
│  │                                                            │         │
│  │  • SMTP: smtp-mail.outlook.com:587 (TLS)                  │         │
│  │  • Destinatarios: jmartinez@ccaait.cat, etc.              │         │
│  └───────────────────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
                         ┌─────────────────┐
                         │  ✅ FIN PROCESO │
                         │  Log guardado   │
                         └─────────────────┘
```

---

## 📊 Tabla de Responsabilidades por Módulo

| Módulo | Archivo Principal | Entrada | Salida | Dependencias | Responsabilidad |
|--------|------------------|---------|--------|--------------|-----------------|
| **Configuración** | `daily_report.py` | Fecha sistema | `consums_config.json` actualizado | - | Calcula período, detecta día 1, orquesta pipeline |
| **Extracción Señales** | `adquisicion/extraer_senales_ftr.py` | PostgreSQL `cfg_tags` | `senales_para_descarga.txt` | `CAT_Conexions.pgDataLake` | Identifica señales TOT_L/TOT_H a descargar |
| **Descarga API** | `adquisicion/download_minute_data.py` | API SagedCAT + lista señales | `minute_data/*.csv` | `CAT_Conexions.apiSagedCAT` | Descarga datos minutales TOT_L/TOT_H |
| **Combinación 32-bit** | `adquisicion/run_compute_for_minutes.py` | CSVs individuales | `combined_minute_data.csv` | `db_connection.py` (per10) | Combina TOT_L+TOT_H→TOT32, aplica per10 |
| **Cálculo Consumo** | `procesado/compute_consumption.py` | CSV combinado | `consumption_minutes_*.csv` | - | Calcula consumo, detecta anomalías y resets |
| **Agregación Horaria** | `procesado/compute_hourly_consumption.py` | CSV minutos | `consumption_hourly_*.csv` | pandas | Agrega minutos→horas con correcciones |
| **Persistencia BD** | `persistencia/save_hourly_consumption.py` | CSV horario | PostgreSQL `ite_tags_consums` | psycopg v3 | DELETE + INSERT bulk a Azure |
| **Validación Diaria** | `validacions/validate_consumption.py` | API + PostgreSQL | `validation_report_*.csv` | `CAT_Conexions.apiSagedCAT` | Compara consumo calculado vs API |
| **Validación Mensual** | `validacions/validate_monthly_consumption.py` | API + vista PG | `validation_monthly_*.csv` | Vista `ite_v_consums_24h` | Valida mes completo vs fuente verdad |
| **Generación PDF** | `validacions/generate_validation_report.py` | CSV validación | PDF català | ReportLab, assets/logo.jpg | Genera informe visual profesional |
| **OAuth2** | `email_utils/oauth2.py` | Azure app config | Access token | MSAL | Autenticación Microsoft, token refresh |
| **Envío Email** | `email_utils/sender.py` | PDFs + CSVs | Email enviado | SMTP + OAuth2 | Envía informe con firma HTML |

---

## 🔄 Flujo de Datos Detallado

### Transformación de Datos

```
Paso 1: API SagedCAT → Descarga
┌──────────────────────────────────────────────┐
│  CL_CAT_BPD01_FTR_G02_TOT_L: [0...65535]    │ 16-bit LOW
│  CL_CAT_BPD01_FTR_G02_TOT_H: [0...65535]    │ 16-bit HIGH
└──────────────────────────────────────────────┘
                 │
                 ▼
Paso 2: Combinación 32-bit
┌──────────────────────────────────────────────┐
│  CL_CAT_BPD01_FTR_G02_TOT32 =               │ 32-bit
│    TOT_H * 65536 + TOT_L                    │ [0...4.294.967.295]
└──────────────────────────────────────────────┘
                 │
                 ▼
Paso 3: Aplicar per10 (si aplica)
┌──────────────────────────────────────────────┐
│  IF per10=True:                             │
│    TOT32 = TOT32 * 10                       │
└──────────────────────────────────────────────┘
                 │
                 ▼
Paso 4: Calcular Consumo
┌──────────────────────────────────────────────┐
│  _cons = TOT32[t+1] - TOT32[t]              │ Diferencia entre minutos
└──────────────────────────────────────────────┘
                 │
                 ▼
Paso 5: Detectar Anomalías
┌──────────────────────────────────────────────┐
│  IF _cons < 0 AND next_cons > 0:            │ Par negativo/positivo
│    Redistribuir en minutos con TOT=0        │
│    _anom = corrección                       │
└──────────────────────────────────────────────┘
                 │
                 ▼
Paso 6: Detectar Resets
┌──────────────────────────────────────────────┐
│  IF _cons < -1.000.000:                     │ Reset contador
│    _anom = counter_max + _cons              │
│  ELSE IF _cons ≈ -65.536:                   │ Reset 16-bit
│    _anom = 65.536 + _cons                   │
└──────────────────────────────────────────────┘
                 │
                 ▼
Paso 7: Agregación Horaria
┌──────────────────────────────────────────────┐
│  _cons_hourly = Σ(_cons per hour)           │ Suma por hora
│  _anom_hourly = Σ(_anom per hour)           │
│  _csm = _cons_hourly + _anom_hourly         │ Consumo final corregido
└──────────────────────────────────────────────┘
                 │
                 ▼
Paso 8: PostgreSQL
┌──────────────────────────────────────────────┐
│  ite_tags_consums:                          │
│  (timestamp_utc, id_tag, consum)            │
│  = (hora, 5423, _csm)                       │
└──────────────────────────────────────────────┘
```

---

## 🗂️ Estructura de Archivos y Directorios

```
Consums_v3/
│
├── adquisicion/                      # 📥 EXTRACCIÓN
│   ├── extraer_senales_ftr.py       #   Consulta PostgreSQL señales
│   ├── download_minute_data.py      #   Descarga API SagedCAT
│   ├── run_compute_for_minutes.py   #   Combina 32-bit + per10
│   ├── minute_data/                 #   📁 Datos crudos minutales
│   │   ├── CL_CAT_*_TOT_L.csv      #      Señales individuales
│   │   ├── CL_CAT_*_TOT_H.csv      #
│   │   └── combined_minute_data.csv #      CSV combinado TOT32
│   └── senales_para_descarga.txt    #   Lista señales a descargar
│
├── procesado/                        # ⚙️ PROCESAMIENTO
│   ├── compute_consumption.py       #   Módulo cálculo consumo
│   ├── run_compute_consumption.py   #   Script ejecución minutos
│   ├── compute_hourly_consumption.py#   Módulo agregación horaria
│   ├── run_hourly_aggregation.py    #   Script ejecución horaria
│   └── Data/                        #   📁 Resultados procesados
│       ├── consumption_minutes_*.csv#      Consumos minutales
│       └── consumption_hourly_*.csv #      Consumos horarios
│
├── persistencia/                     # 💾 PERSISTENCIA
│   └── save_hourly_consumption.py   #   INSERT bulk a PostgreSQL
│
├── validacions/                      # ✅ VALIDACIÓN
│   ├── validate_consumption.py      #   Validación diaria
│   ├── validate_monthly_consumption.py # Validación mensual
│   ├── generate_validation_report.py#   Generación PDF
│   ├── firma.html                   #   Firma corporativa email
│   ├── validation_report_*.csv      #   📁 Resultados diarios
│   ├── validation_report_*.pdf      #
│   ├── validation_monthly_*.csv     #   📁 Resultados mensuales
│   └── validation_monthly_*.pdf     #
│
├── email_utils/                      # 📧 NOTIFICACIÓN
│   ├── oauth2.py                    #   Autenticación MSAL
│   └── sender.py                    #   Envío SMTP + OAuth2
│
├── CAT_Conexions/                    # 🔌 SUBMODULO GIT
│   └── src/conexions/               #   pgDataLake, apiSagedCAT
│
├── assets/                           # 🖼️ RECURSOS
│   └── logo.jpg                     #   Logo corporativo CAT
│
├── log/                              # 📝 LOGS
│   └── daily_report_YYYYMMDD.log    #   Logs ejecución diaria
│
├── docs/                             # 📚 DOCUMENTACIÓN
│   ├── AUTOMATIZACION.md
│   ├── DIAGRAMA_VALIDACION_MENSUAL.md
│   ├── EMAIL_SETUP.md
│   ├── per10_multiplier.md
│   └── VALIDACION_MENSUAL.md
│
├── context/                          # 🧠 CONTEXTO PERSISTENTE
│   ├── PROJECT.md                   #   Estado actual
│   ├── RULES.md                     #   7 reglas críticas
│   ├── ARCHITECTURE.md              #   Este archivo
│   └── DECISIONS.md                 #   Decisiones técnicas
│
├── .github/
│   ├── copilot-instructions.md      #   Instrucciones Copilot
│   └── agents/                      #   Agentes personalizados
│
├── daily_report.py                   # 🚀 ORQUESTADOR PRINCIPAL
├── run_pipeline.py                   # 🔧 Pipeline manual
├── run_monthly_validation.py         # 📅 Validación mensual manual
├── send_last_report.py               # 📤 Reenviar último informe
│
├── consums_config.json               # ⚙️ Configuración (NO versionar)
├── token_cache.json                  # 🔐 Token OAuth2 (NO versionar)
├── README.md                         # 📖 Documentación principal
└── CHANGELOG.md                      # 📋 Historial cambios
```

---

## 🔌 Integraciones Externas

### API SagedCAT
- **URL**: Configurado en `consums_config.json`
- **Autenticación**: Token (`nexustoken`)
- **Vista**: ID específica para acceso datos
- **Endpoints usados**:
  - `GET /api/Documents/tagviews/{vista}/historic` - Datos históricos
  - Parámetros: `timestamp`, `signal_uid`

### PostgreSQL Azure
- **Esquema principal**: `ga_datalake`
- **Tablas usadas**:
  - `cfg_tags` (lectura): Metadatos señales, per10 flag
  - `ite_tags_consums` (escritura): Consumos horarios
- **Vista usada**:
  - `ite_v_consums_24h` (lectura): Consumos con rectificaciones (validación mensual)
- **Conexión**: psycopg v3 vía `CAT_Conexions.pgDataLake`

### Microsoft OAuth2
- **Azure Portal**: Registro de aplicación
- **Client ID**: En configuración
- **Permisos**: `SMTP.Send` (delegado)
- **Token cache**: Local en `token_cache.json`
- **Refresh automático**: Vía MSAL

### SMTP Outlook
- **Servidor**: smtp-mail.outlook.com:587
- **Seguridad**: TLS
- **Autenticación**: OAuth2 (no contraseña)

---

## ⏱️ Flujo Temporal

### Ejecución Diaria Típica (02:00 AM)

```
02:00:00 - Inicio daily_report.py (Task Scheduler)
02:00:01 - Actualiza consums_config.json (período = ayer)
02:00:02 - Detecta si es día 1 del mes
02:00:05 - Ejecuta run_pipeline.py
   02:00:10 - Extrae señales (si cambió cfg_tags)
   02:00:15 - Descarga datos API (~238 señales × 1440 minutos)
   02:05:30 - Combina TOT_L/TOT_H → TOT32
   02:06:00 - Aplica per10 multiplier
   02:06:30 - Calcula consumos + detecta anomalías
   02:07:00 - Detecta resets contador
   02:07:30 - Agrega a resolución horaria
   02:08:00 - INSERT bulk a PostgreSQL
02:10:00 - Ejecuta validate_consumption.py
   02:10:30 - Consulta totalizadores API (238 señales)
   02:12:00 - Consulta consumos PostgreSQL
   02:13:00 - Compara y clasifica
   02:13:30 - Genera validation_report_*.csv
02:14:00 - Genera PDF con generate_validation_report.py
02:15:00 - SI día 1: Ejecuta validate_monthly_consumption.py
   02:15:30 - Consulta totalizadores mes completo
   02:16:00 - Consulta vista ite_v_consums_24h
   02:16:30 - Compara mes completo
   02:17:00 - Genera validation_monthly_*.csv
   02:17:30 - Genera validation_monthly_*.pdf
02:18:00 - Envía email (OAuth2 + SMTP)
   02:18:05 - Autenticación OAuth2
   02:18:10 - Adjunta PDFs + CSVs (2 o 4 archivos)
   02:18:30 - Envío exitoso
02:19:00 - FIN (log guardado)
```

**Duración típica**: 15-20 minutos (diario), 20-25 minutos (día 1 con mensual)

---

## 🔀 Puntos de Decisión

### ¿Cuándo se aplica per10?
```
Condición: señal tiene per10=True en cfg_tags
Momento: DESPUÉS de combinar TOT_L/TOT_H, ANTES de calcular consumo
Archivo: adquisicion/run_compute_for_minutes.py
```

### ¿Cuándo se detecta un reset?
```
Condición: consumo < -1.000.000 OR consumo ≈ -65.536
Momento: DESPUÉS de detectar anomalías regulares
Archivo: procesado/compute_consumption.py → detect_counter_resets()
```

### ¿Cuándo se ejecuta validación mensual?
```
Condición: datetime.now().day == 1
Momento: DESPUÉS de validación diaria, ANTES de enviar email
Archivo: daily_report.py → main()
```

### ¿Cuándo se redistribuyen anomalías?
```
Condición: Par consecutivo consumo negativo + consumo positivo
Momento: DESPUÉS de calcular consumo, ANTES de detectar resets
Archivo: procesado/compute_consumption.py → attach_anomalies_to_df()
```

---

## 📈 Escalabilidad y Performance

### Datos Procesados Diariamente
- **Señales**: ~238
- **Resolución original**: 1 minuto
- **Puntos de datos**: 238 × 1440 minutos = 342.720 registros
- **Output horario**: 238 × 24 horas = 5.712 registros
- **Inserciones PostgreSQL**: 5.712 registros/día

### Optimizaciones Implementadas
1. **Bulk INSERT**: psycopg COPY para inserción masiva
2. **DELETE previo**: Evita duplicados, permite reprocesamiento
3. **CSV intermedio**: Permite debugging sin regenerar todo
4. **Per10 en memoria**: Multiplicación vectorizada pandas
5. **Agregación pandas**: resample() eficiente

### Límites Conocidos
- **Timeout API**: 3600s (1 hora) para descarga completa
- **Timeout subprocess**: 3600s para cada fase del pipeline
- **Memoria**: ~500MB para procesar día completo
- **Disco**: ~100MB/día en CSVs intermedios

---

## ✅ Checklist de Arquitectura

Al modificar el sistema, verifica:

- [ ] ¿El cambio respeta el orden del pipeline? (extracción → procesamiento → persistencia → validación)
- [ ] ¿El cambio afecta el flujo de datos? (actualiza este diagrama)
- [ ] ¿El cambio añade nuevas dependencias? (documenta en tabla de responsabilidades)
- [ ] ¿El cambio modifica integraciones externas? (actualiza sección de integraciones)
- [ ] ¿El cambio afecta el timing? (actualiza flujo temporal)
- [ ] ¿El cambio añade nuevos archivos? (actualiza estructura de directorios)

---

## 🎯 Resumen Ejecutivo

**Arquitectura**: Pipeline ETL lineal con validación y notificación  
**Componentes**: 7 módulos principales  
**Frecuencia**: Diaria automatizada + mensual (día 1)  
**Datos**: ~340K registros procesados/día → ~5.7K registros persistidos  
**Duración**: 15-25 minutos  
**Destino**: PostgreSQL Azure + Email notificación  
**Estado**: Producción estable desde v3.1
