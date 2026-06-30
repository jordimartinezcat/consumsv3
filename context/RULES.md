# Reglas de Negocio Críticas - Consums_v3

⚠️ **IMPORTANTE**: Estas reglas NO deben romperse sin entender completamente el impacto en el sistema de producción.

---

## 📋 Las 7 Reglas de Oro

### 1. per10 Multiplier (18 señales)

**Qué hace**: Multiplica por 10 los valores de totalizadores de 18 señales específicas que tienen configurado el flag `per10=True` en la base de datos.

**¿Por qué existe?**: Algunas señales tienen configuración de hardware que divide el valor real por 10 en el totalizador. Debemos multiplicar para obtener el valor verdadero.

**Ubicación del código**:
- **Módulo**: `adquisicion/run_compute_for_minutes.py`
- **Función**: `apply_per10_multiplier(df, per10_signals)`
- **Consulta BD**: `db_connection.py` → `get_tag_per10()` consulta `cfg_tags.per10`

**Momento de aplicación**:
```
1. Descarga TOT_L y TOT_H desde API (16-bit cada uno)
2. Combina TOT_L + TOT_H → TOT32 (32-bit)
3. 👉 AQUÍ: Aplica per10 multiplicador (TOT32 × 10) 👈
4. Calcula consumo (TOT32[t+1] - TOT32[t])
```

**Código exacto**:
```python
# En adquisicion/run_compute_for_minutes.py
def apply_per10_multiplier(df, per10_signals):
    """
    Multiplica por 10 las columnas de totalizadores para señales con per10=True.
    Se aplica DESPUÉS de combinar TOT_L/TOT_H en TOT32.
    """
    for signal in per10_signals:
        tot32_col = f"{signal}_TOT32"
        if tot32_col in df.columns:
            df[tot32_col] = df[tot32_col] * 10
            logger.info(f"  ✓ Applied per10 multiplier to {signal}")
    return df
```

**⚠️ ERROR COMÚN**: NO aplicar per10 después de calcular consumo. Debe ser ANTES.

**Señales afectadas** (18 total):
- BPB02, BPB04, BPB05, BPB06, BPB07, BPB08, BPB09, BPB10, BPB11
- PAB01, PBB02, PBB03
- SCA02, SCA03, SCA04, SCA05, SCB02, SCD04

**Documentación adicional**: `docs/per10_multiplier.md`

---

### 2. Detección de Resets de Contador

**Qué hace**: Detecta cuando un contador de agua se resetea (vuelve a 0) y marca la anomalía para corrección.

**¿Por qué existe?**: Los contadores tienen capacidad máxima (típicamente 65.536L para contadores 16-bit). Cuando se llenan, resetean a 0, causando un "consumo negativo" aparente que debe corregirse.

**Ubicación del código**:
- **Módulo**: `procesado/compute_consumption.py`
- **Función**: `detect_counter_resets(df, signal_prefixes, logger=None)`
- **Helper**: `determine_counter_max(max_totalizer_value)`

**Thresholds de detección**:
```python
# Reset completo (contador > 1M litros)
if consumption < -1_000_000:
    # Es un reset de contador grande

# Reset 16-bit (≈65.536L)
if -70_000 < consumption < -60_000:
    # Es un reset típico de contador 16-bit (65.536L)
    reset_value = 65_536
```

**Código exacto**:
```python
# En procesado/compute_consumption.py
def detect_counter_resets(df, signal_prefixes, logger=None):
    """
    Detecta resets de contador cuando consumo cae significativamente.
    Marca corrección en columna *_anom, preserva valor original en *_cons.
    """
    for prefix in signal_prefixes:
        cons_col = f"{prefix}_cons"
        anom_col = f"{prefix}_anom"
        
        if cons_col not in df.columns:
            continue
            
        # Detectar consumos muy negativos
        resets_mask = df[cons_col] < -1_000_000
        
        if resets_mask.any():
            # Estimar capacidad máxima del contador
            max_val = df[f"{prefix}_TOT32"].max()
            counter_max = determine_counter_max(max_val)
            
            # Marcar corrección en columna anomaly
            df.loc[resets_mask, anom_col] = counter_max + df.loc[resets_mask, cons_col]
            
            logger.info(f"  ✓ Detected {resets_mask.sum()} counter resets for {prefix}")
```

**Proceso de 3 fases**:
1. Calcular consumo básico (TOT[t+1] - TOT[t])
2. Detectar y redistribuir anomalías regulares (pares negativo/positivo)
3. Detectar y marcar resets de contador

**Resultado en validación**:
- Las correcciones aparecen en columna `*_anom`
- Validación detecta diferencias de ~65.536L o múltiplos
- Se categorizan como "✅ OK amb resets"

**⚠️ IMPORTANTE**: El valor original negativo se preserva en `*_cons`, la corrección va a `*_anom`.

---

### 3. Última Hora del Día (23:00)

**Qué hace**: Para calcular el consumo de la hora 23:00 del día D, necesita el totalizador del minuto 00:00 del día D+1.

**¿Por qué existe?**: El consumo horario es la suma de consumos minutales. El último minuto de 23:00 (23:59) necesita el totalizador de 00:00 para calcular: Tot[00:00] - Tot[23:59].

**Ubicación del código**:
- **Módulo**: `procesado/compute_hourly_consumption.py`
- **Configuración**: `consums_config.json` → `period.end` debe ser día+1 00:00

**Período de descarga extendido**:
```json
{
  "period": {
    "start": "2026-06-29 00:00:00",  // Día a procesar
    "end": "2026-06-30 00:00:00"     // Incluye 00:00 del día siguiente
  }
}
```

**Proceso**:
```
1. Descarga datos: 29/06 00:00 → 30/06 00:00 (incluye minuto extra)
2. Calcula consumo: usa minuto 00:00 del día 30 para última hora del 29
3. Filtra resultados: elimina hora 00:00 del día 30 (timezone-aware)
4. Resultado: Solo 24 horas del día 29 (00:00 a 23:00)
```

**Código exacto**:
```python
# En procesado/compute_hourly_consumption.py
def aggregate_to_hourly(df_minutes, signal_prefixes, tz='Europe/Madrid'):
    """
    Agrega consumos minutales a horarios.
    Periodo debe incluir 00:00 del día siguiente.
    """
    # Asegurar timezone aware
    df_minutes['timestamp'] = pd.to_datetime(df_minutes['timestamp']).dt.tz_localize(tz)
    
    # Agregar por hora (incluye hora 00:00 siguiente día)
    df_hourly = df_minutes.resample('H', on='timestamp').sum()
    
    # Filtrar para no incluir hora extra del día siguiente
    # (esto se hace en el módulo de inserción o validación)
```

**⚠️ ERROR COMÚN**: No extender el período de descarga causa que falte la hora 23:00 o tenga consumo incorrecto.

---

### 4. Timestamps de Validación (00:00, no 23:59)

**Qué hace**: La validación consulta el totalizador exactamente a las 00:00:00 del día inicial y 00:00:00 del día final.

**¿Por qué existe?**: Debe ser consistente con cómo se calculó el consumo. El consumo de la última hora (23:00) usa el totalizador de 00:00 del día siguiente.

**Ubicación del código**:
- **Módulo**: `validacions/validate_consumption.py`
- **Función**: `validate_signal(api, signal, ...)`

**Timestamps correctos**:
```python
# CORRECTO ✅
start_timestamp = "2026-06-29 00:00:00"  # Primer minuto del día
end_timestamp = "2026-06-30 00:00:00"    # Primer minuto del día siguiente

# INCORRECTO ❌
end_timestamp = "2026-06-29 23:59:00"    # Último minuto del día
# Esto daría diferencia incorrecta en totalizadores
```

**Query API SagedCAT**:
```python
# En validacions/validate_consumption.py
def validate_signal(api, signal, df_hourly_consumption, ...):
    # Obtener totalizador inicial
    tot_initial = api.get_historic_value(
        signal_uid, 
        timestamp=start_timestamp  # "2026-06-29 00:00:00"
    )
    
    # Obtener totalizador final
    tot_final = api.get_historic_value(
        signal_uid,
        timestamp=end_timestamp  # "2026-06-30 00:00:00"
    )
    
    # Calcular diferencia de totaladores
    diff_totalizer = tot_final - tot_initial
```

**Consistencia**:
```
Cálculo consumo:    29/06 00:00 → 30/06 00:00 (24 horas + minuto extra)
Validación:         Tot[30/06 00:00] - Tot[29/06 00:00]
Resultado esperado: Σ(consumos 24 horas) ≈ diferencia totalizadores
```

**⚠️ IMPORTANTE**: Cambiar a 23:59 rompería la consistencia y causaría discrepancias falsas.

---

### 5. Encoding Windows Server (cp1252)

**Qué hace**: Cuando se ejecutan subprocesos (subprocess), usa encoding `cp1252` con `errors='replace'` para manejar caracteres catalanes.

**¿Por qué existe?**: Windows Server usa codificación cp1252 (Windows-1252) por defecto para español/catalán, no UTF-8. Los caracteres catalanes (ò, à, é, í, ó, ú, ç, ñ) pueden causar errores si se asume UTF-8.

**Ubicación del código**:
- **Módulo**: `daily_report.py`, cualquier script que use `subprocess.run()`
- **Afectado**: Output de consola, logs, captura de stdout/stderr

**Código correcto**:
```python
# En daily_report.py
result = subprocess.run(
    [sys.executable, str(ROOT / "run_pipeline.py")],
    cwd=str(ROOT),
    capture_output=True,
    text=True,
    encoding="cp1252",      # 👈 Windows encoding
    errors="replace",       # 👈 Reemplaza caracteres inválidos en vez de crashear
    timeout=3600
)
```

**Logging también**:
```python
# Configurar logging con UTF-8
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),  # Archivo: UTF-8
        logging.StreamHandler()  # Consola: usa default del sistema
    ]
)
```

**Caracteres problemáticos**:
```python
# ❌ NO usar caracteres Unicode en output de consola
print("✓ Completat")   # Puede fallar en Windows
print("✗ Error")       # Puede fallar en Windows

# ✅ SÍ usar ASCII equivalentes
print("[OK] Completat")    # Seguro
print("[ERROR] Error")      # Seguro
```

**Dónde SÍ usar catalán con Unicode**:
- Archivos de log (encoding="utf-8")
- PDFs generados (ReportLab)
- Emails HTML (charset UTF-8)
- CSV (encoding="utf-8-sig" para Excel)

**⚠️ IMPORTANTE**: No asumir UTF-8 en subprocess o fallarán scripts en producción.

---

### 6. Permisos PostgreSQL (Manejo de Errores)

**Qué hace**: El usuario de la aplicación puede no ser owner de las tablas. Al crear índices o hacer operaciones DDL, captura errores de permisos y continúa.

**¿Por qué existe?**: En Azure PostgreSQL, las tablas pueden ser creadas por un DBA con permisos de owner, mientras la app usa un usuario con permisos limitados (INSERT, SELECT, UPDATE, DELETE).

**Ubicación del código**:
- **Módulo**: `persistencia/save_hourly_consumption.py`
- **Afectado**: Creación de índices, operaciones DDL

**Código correcto**:
```python
# En persistencia/save_hourly_consumption.py
try:
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ite_tags_consums_timestamp 
        ON ga_datalake.ite_tags_consums(timestamp_utc)
    """)
    logger.info("  ✓ Index created successfully")
except psycopg.errors.InsufficientPrivilege:
    logger.warning("  ⚠ Insufficient privileges to create index (continuing)")
    conn.rollback()
except psycopg.errors.ProgrammingError as e:
    logger.warning(f"  ⚠ Could not create index: {e} (continuing)")
    conn.rollback()
```

**Errores a capturar**:
- `psycopg.errors.InsufficientPrivilege` - Usuario no tiene permiso
- `psycopg.errors.ProgrammingError` - Operación no permitida

**Estrategia**:
1. Intentar operación DDL (crear índice, alterar tabla)
2. Si falla por permisos: log warning, rollback, **continuar**
3. Si falla por otro motivo: log error, rollback, **continuar** (no fallar pipeline)

**⚠️ IMPORTANTE**: No permitir que falta de permisos DDL rompa el pipeline de inserción de datos.

**Operaciones críticas vs opcionales**:
```python
# CRÍTICO: INSERT/UPDATE/DELETE de datos (debe funcionar)
try:
    cursor.execute("INSERT INTO ite_tags_consums ...")
except Exception as e:
    logger.error("CRITICAL: Could not insert data")
    raise  # 👈 Aquí SÍ fallar

# OPCIONAL: CREATE INDEX (nice to have)
try:
    cursor.execute("CREATE INDEX ...")
except psycopg.errors.InsufficientPrivilege:
    logger.warning("Could not create index (continuing)")
    # 👈 Aquí NO fallar
```

---

### 7. Idioma Català (Output Usuario)

**Qué hace**: TODO el output orientado a usuario final debe estar en català (catalán). El código interno puede estar en castellano/inglés.

**¿Por qué existe?**: El CAT es una institución catalana. Los informes son para técnicos catalanes. Es un requisito del cliente.

**Ubicación del código**:
- **Afectado**: PDFs, emails, CSV (headers), logs de validación
- **NO afectado**: Código fuente, comentarios técnicos, nombres de variables

**Català en PDFs**:
```python
# En validacions/generate_validation_report.py
title = "INFORME DE VALIDACIÓ DE CONSUMS"  # ✅ Català
subtitle = f"Període: {start_date} → {end_date}"

sections = {
    "errors": "Errors - Sense dades a l'API",
    "resets": "Senyals amb resets detectats",
    "discrepancies": "Discrepàncies per revisar"
}
```

**Català en emails**:
```python
# En email_utils/sender.py
subject = f"[Consums] Informe de Validació - {date}"
body_html = f"""
<h2>Informe de Validació de Consums</h2>
<p>Període: {start} → {end}</p>
<h3>RESUM DE VALIDACIÓ</h3>
<ul>
  <li>Total senyals processades: {total}</li>
  <li>✓ OK (perfectes): {ok} ({ok_pct}%)</li>
  <li>⚠ Discrepàncies: {disc} ({disc_pct}%)</li>
  <li>✗ Errors (sense dades): {err} ({err_pct}%)</li>
</ul>
"""
```

**Català en CSV headers**:
```python
# Headers de CSV de validación
headers = [
    "senyal",           # signal
    "consum_calculat",  # calculated_consumption
    "diferencia_API",   # api_difference
    "error_percentual", # percentage_error
    "estat"            # status
]
```

**Castellano/inglés en código**:
```python
# Código interno - puede ser en inglés/castellano
def compute_consumption(df, signal_prefixes):
    """
    Calculate consumption from totalizer differences.
    
    Args:
        df: DataFrame with totalizer columns
        signal_prefixes: List of signal name prefixes
    """
    for prefix in signal_prefixes:
        tot_col = f"{prefix}_TOT32"
        cons_col = f"{prefix}_cons"
        df[cons_col] = df[tot_col].diff()  # Consumption = difference
```

**Traducción clave**:
- Signal → senyal
- Consumption → consum
- Error → error
- Discrepancy → discrepància
- Reset → reset (se mantiene)
- OK → OK / D'acord
- Period → període
- Report → informe
- Validation → validació

**⚠️ IMPORTANTE**: No mezclar idiomas en el mismo documento orientado a usuario. O todo català o todo castellano. En este proyecto: català siempre para usuario final.

---

## 🎯 Resumen de Impacto

| Regla | Módulo Principal | Impacto si se Rompe | Severidad |
|-------|-----------------|---------------------|-----------|
| 1. per10 | `adquisicion/run_compute_for_minutes.py` | Consumos x10 incorrectos en 18 señales | 🔴 CRÍTICO |
| 2. Resets | `procesado/compute_consumption.py` | Consumos negativos no detectados | 🟠 ALTO |
| 3. Última hora | `procesado/compute_hourly_consumption.py` | Hora 23:00 incorrecta o faltante | 🟠 ALTO |
| 4. Timestamps | `validacions/validate_consumption.py` | Discrepancias falsas en validación | 🟡 MEDIO |
| 5. Encoding | `daily_report.py` | Crashes con caracteres catalanes | 🟡 MEDIO |
| 6. Permisos PG | `persistencia/save_hourly_consumption.py` | Pipeline falla innecesariamente | 🟢 BAJO |
| 7. Català | `validacions/generate_validation_report.py` | Informes en idioma incorrecto | 🟢 BAJO |

---

## ✅ Checklist al Modificar Código

Antes de hacer cambios, verifica:

- [ ] ¿Afecta señales con per10? → Revisar regla 1
- [ ] ¿Modifica cálculo de consumo? → Revisar reglas 1, 2, 3
- [ ] ¿Cambia períodos de descarga/validación? → Revisar reglas 3, 4
- [ ] ¿Usa subprocess? → Revisar regla 5
- [ ] ¿Crea índices o hace DDL? → Revisar regla 6
- [ ] ¿Genera output para usuario? → Revisar regla 7

**Si modificas alguna regla**, actualiza este documento con la justificación del cambio.
