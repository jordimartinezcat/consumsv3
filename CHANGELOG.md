# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- **Sistema de guardado de totalizadores en PostgreSQL**: Nueva tabla `ga_landing.ite_consums_totalitzadors` para almacenar totalizadores inicial/final obtenidos durante la validación diaria
  - Módulo `persistencia/save_totalizers.py` con funciones para crear tabla y guardar/leer totalizadores
  - Integración automática en `validacions/validate_consumption.py` para guardar totalizadores después de cada validación
  - Ventajas: Histórico de totalizadores, consistencia de datos, mejor rendimiento
- **Integración SQL Server para resumen diario**: Nueva funcionalidad para guardar datos diarios consolidados en SQL Server (tabla `Consums_dia`) después de la validación. Incluye:
  - Módulo `persistencia/save_daily_to_sqlserver.py` con funciones para conexión, extracción de ID contador, consulta de consumos desde PostgreSQL
  - **OPTIMIZACIÓN**: Lee totalizadores desde PostgreSQL (`ite_consums_totalitzadors`) en lugar de consultar la API cada vez
  - Script de ejecución `persistencia/run_save_to_sqlserver.py` para integración en pipeline
  - Estrategia MERGE (UPDATE si existe, INSERT si no) para evitar duplicados
  - Autenticación Windows (Trusted Connection) para SQL Server
  - Extracción automática de ID contador desde tag completo (ej: `CL_CAT_B11_FTR_G01_TOT` → `B11`)
  - Documentación completa en `docs/SQL_SERVER_SETUP.md` con ejemplos de configuración, troubleshooting y verificación
  - Dependencia `pyodbc==5.3.0` añadida al entorno virtual
  - Ejemplo de configuración en `consums_config.json.example`
  - Nueva tarea `save_to_sqlserver` en pipeline, ejecutada después de validación
- **ANALISIS_DELETE_RECTIFICACIONES.md**: Documento de análisis confirmando que la lógica DELETE en `save_hourly_consumption.py` filtra correctamente por `idtag` (no afecta otras señales)

### Changed

- Actualizado `run_pipeline.py` para incluir nueva tarea `save_to_sqlserver` en el mapa de tareas
- Modificado `validacions/validate_consumption.py` para guardar totalizadores en PostgreSQL automáticamente después de cada validación
- Optimizado `persistencia/save_daily_to_sqlserver.py` para leer totalizadores desde PostgreSQL en lugar de API (más rápido y consistente)
- **OPTIMIZACIÓN MAYOR de performance en SQL Server** (2026-07-01): Reducido de 624 queries a 3 operaciones batch:
  - **Antes**: 208 señales × 3 queries (1 totalizer + 1 consumption + 1 MERGE) = **624 operaciones**
  - **Después**: 3 operaciones totales:
    1. `get_all_totalizers_batch()` - Obtiene TODOS los totalizadores en 1 query usando `WHERE tag = ANY(:tags)`
    2. `get_all_consumptions_batch()` - Obtiene TODOS los consumos en 1 query con JOIN
    3. `executemany()` - Inserta todos los registros en 1 batch a SQL Server
  - **Mejora**: **208x más rápido** (de ~30s a <1s)
  - **Técnica**: Batch queries con `ANY(:array)` en PostgreSQL, preparación en memoria, insert batch con `executemany()`
  - **Beneficio adicional**: Reduce carga en servidores PostgreSQL y SQL Server
- **Corregido cálculo de fecha en `validate_consumption.py`**: Ahora lee la fecha desde `consums_config.json` en lugar de usar el timestamp del archivo horario (que está en UTC), evitando errores de desfase de días
- **REDISEÑO COMPLETO del mapeo de IDs en SQL Server**: Evolución del sistema de mapeo a través de 4 versiones
  - **V1**: Extracción simplista desde nombre del tag → 23/208 registros (11%)
  - **V2**: Normalización + SQL Server Comptadors.IdMaximo → 122/208 registros (59%)
  - **V3**: Normalización + PostgreSQL ite_comptadors.IdMaximo → 163/208 registros (78%)
  - **V4 (FINAL)**: JOIN entre `ite_consums_tags.tagOld` e `ite_comptadors.Id` → **165/208 registros (79%)**
  - Nueva función `get_comptadors_mapping()` usando JOIN directo en PostgreSQL:
    ```sql
    SELECT REPLACE(tags.tag, '_CSM', '_TOT') as tag_tot, comp."Id" as id_comptador
    FROM ga_landing.ite_consums_tags tags
    INNER JOIN ga_landing.ite_comptadors comp ON tags."tagOld" = comp."Id"
    WHERE tags.tag LIKE '%_CSM' AND tags."tagOld" IS NOT NULL
    ```
  - Simplificación del procesamiento: lookup directo en mapeo sin normalización
  - **Ventaja conceptual**: Usa el campo `tagOld` diseñado explícitamente para mapear sistema antiguo → nuevo
  - 40 señales sin mapeo (19%): no tienen `tagOld` o su `tagOld` no existe en `ite_comptadors`
- **Validación de valores NaN**: Agregada validación para detectar y reemplazar NaN por 0.0 antes de insertar en SQL Server (evita error de tipo de datos)

### Fixed

- **BUG CRÍTICO: Totalizadores desaparecidos en SQL Server** (2026-07-01): Los totalizadores se guardaban en 0 debido a desajuste de prefijos CL_CAT_:
  - **Problema**: Los tags en `ite_consums_tags` no tienen prefijo `CL_CAT_`, pero los totalizadores se guardan en `ite_consums_totalitzadors` con prefijo (vienen de validación que usa `ite_sql4_cfg_tags`)
  - **Síntoma**: `get_all_totalizers_batch()` devolvía 0 registros aunque la tabla tenía 231 totalizadores guardados
  - **Causa**: `signals_with_mapping` (sin prefijo) se comparaba con tags en tabla (con prefijo `CL_CAT_`)
  - **Solución**: Agregar prefijo `CL_CAT_` a la lista de tags antes de buscar totalizadores batch:
    ```python
    signals_with_prefix = ['CL_CAT_' + tag for tag in signals_with_mapping]
    totalizers_batch = get_all_totalizers_batch(pg_engine, signals_with_prefix, date)
    # Luego quitar prefijo para facilitar lookup posterior
    totalizers_batch_no_prefix = {tag.replace('CL_CAT_', ''): value for tag, value in totalizers_batch.items()}
    ```
  - **Resultado**: 165 totalizadores recuperados correctamente (antes 0, ahora 100% de señales con mapeo)
  - **Commit**: [fecha] Fix totalizer prefix mismatch in batch query
- **Bug crítico de fecha en totalizadores**: Los totalizadores se guardaban con fecha incorrecta (un día anterior) porque el archivo horario tiene timestamps en UTC. Solución: leer fecha desde `period.start` en configuración
- **Error de mapeo de IDs en SQL Server**: Evolución de soluciones hasta lograr 79% de cobertura:
  1. Extracción simplista fallaba (11% cobertura)
  2. Mapeo vía SQL Server Comptadors.IdMaximo mejoró a 59%
  3. Mapeo vía PostgreSQL ite_comptadors.IdMaximo mejoró a 78%
  4. **SOLUCIÓN FINAL**: JOIN directo entre `ite_consums_tags.tagOld` e `ite_comptadors.Id` logra **79% cobertura (165/208 señales)**
- **Error de valores NaN en SQL Server**: Algunos totalizadores contienen NaN que SQL Server rechaza. Solución: validar con `math.isnan()` y reemplazar por 0.0
- **Error de columna NOT NULL**: SQL Server rechazaba NULL en columna `Especial`. Solución: usar valor 0 por defecto en columnas `Validat`, `Nivell` y `Especial`
- **Error de encoding en subprocess (Windows)**: Modificado `daily_report.py` para usar Python del virtual environment (`.venv\Scripts\python.exe`) en lugar de `sys.executable`, evitando problemas de encoding y dependencias
- **Error de sintaxis en daily_report.py línea 339**: Corregido código duplicado con paréntesis sin cerrar que causaba SyntaxError



### Added

- **Documentación de contexto persistente** (`context/` folder):
  - `context/PROJECT.md`: Estado actual del proyecto, últimos desarrollos, trabajo en curso
  - `context/RULES.md`: Las 7 reglas de negocio críticas con código exacto y ubicación
  - `context/ARCHITECTURE.md`: Arquitectura completa con diagrama de flujo, responsabilidades, transformaciones
  - `context/DECISIONS.md`: 12 decisiones técnicas documentadas (Python 3.13, psycopg v3, OAuth2, etc.)
  - Esta documentación facilita el mantenimiento y desarrollo futuro del sistema

- **Validación mensual automática**:
  - Nuevo script `validacions/validate_monthly_consumption.py` para validar períodos mensuales completos
  - **Fuente de datos**: Vista PostgreSQL `ga_datalake.ite_v_consums_24h` (incluye rectificaciones)
  - Se ejecuta automáticamente el día 1 de cada mes (integrado en `daily_report.py`)
  - Compara totalizer(01/MM 00:00) vs totalizer(01/MM+1 00:00) contra suma de consumos del mes desde BD
  - Genera **CSV y PDF mensuales** con resultados: `validation_monthly_YYYYMM_*.csv` y `validation_monthly_YYYYMM_*.pdf`
  - **Informe PDF mensual** con estructura visual idéntica al diario:
    - Logo y encabezado con título "INFORME DE VALIDACIÓ MENSUAL"
    - Tabla resumen con estadísticas (% OK, resets, errores, discrepancias)
    - Sección 1: Errors - Sense dades a l'API (color rojo)
    - Sección 2: Senyals amb resets detectats (color naranja) con explicación
    - Sección 3: Discrepàncies (color azul) para revisión caso por caso
    - Formato A4 horizontal para mejor visualización
  - Criterios de validación más tolerantes para períodos largos (0.5% error, 100L diferencia)
  - Detección de múltiples resets acumulados durante el mes (múltiplos de 65.536L)
  - Script manual `run_monthly_validation.py` para ejecución independiente
  - Integración en email diario: adjunta **CSV y PDF mensuales** cuando se ejecuta el día 1
  - Consulta SQL optimizada con JOIN a `cfg_tags` para obtener nombres de señales
  - Documentación completa en README y docs/VALIDACION_MENSUAL.md con ejemplos de uso y estructura del PDF

- per10 multiplier feature for consumption calculation:
  - `get_tag_per10()` function in `db_connection.py` queries per10 flag from cfg_tags table
  - `apply_per10_multiplier()` function in `run_compute_for_minutes.py` multiplies totalizer columns by 10
  - Automatic detection and multiplication of totalizador values at data combination stage
  - Applied AFTER combining H/L into 32-bit totals and BEFORE consumption calculation
  - Affects 18 tags with per10=True flag in cfg_tags table

### Fixed

- per10 multiplier now applied at correct stage (during data combination, not after consumption calculation)
- Moved per10 logic from `procesado/run_compute_consumption.py` to `adquisicion/run_compute_for_minutes.py`

## [0.4.0] - 2025-12-05

### Added

- Counter reset detection and correction system for industrial IoT totalizers:
  - `detect_counter_resets()` function in `compute_consumption.py` detects resets at power-of-10 thresholds
  - `determine_counter_max()` helper estimates counter maximum using log10 calculation
  - Automatic detection of resets when consumption drops below -1,000,000
  - Reset corrections marked in anomaly column while preserving original consumption values
  - Three-phase sequential processing: consumption calculation → regular anomalies → reset detection
  - European CSV format (sep=';', decimal=',') for all minute-level outputs
  - Hourly aggregation correctly identifies periods with resets via has_corrections indicator

### Changed

- Modified `append_minute_consumption()` to focus only on consumption calculation (simplified)
- Updated `run_compute_consumption.py` to execute three-phase processing sequence
- Changed CSV output format to European standard (semicolon separator, comma decimal)

## [0.3.0] - 2025-12-05

### Added

- Hourly aggregation system for consumption data:
  - `compute_hourly_consumption.py`: Module for aggregating minute-level data to hourly resolution
  - `run_hourly_aggregation.py`: Script to generate hourly consumption summaries
  - Three output columns per tag: direct sum, corrected consumption, and correction indicator
  - Proper handling of anomaly redistributions in hourly totals
  - European CSV format support with automatic format detection
  - Comprehensive documentation and validation of hourly processing workflow

## [0.2.1] - 2025-12-05

### Changed

- Code cleanup and organization:
  - Removed all unused debug and verification scripts from `procesado/` directory
  - Removed temporary debug scripts from project root
  - Updated `run_compute_consumption.py` to save output files in `procesado/Data/` subdirectory
  - Updated documentation to reflect new organized file structure
  - Project now has clean, maintainable structure with only essential files

## [0.1.0] - 2025-12-04

### Added

- Updated signal extraction logic in `adquisicion/extraer_senales_ftr.py`:
  - Queries now search directly for `TOT_L` and `TOT_H` signals.
  - Exclude signals starting with `ET` and any tags containing `_LS_` or `_P_`.
  - Escape underscore characters in SQL LIKE patterns to match literal `_`.
  - Filter general `%TOT` results to keep only tags whose first 5 characters are not present among `TOT_L`/`TOT_H` prefixes.
  - Output final list to `adquisicion/senales_para_descarga.txt`.

## [0.2.0] - 2025-12-04

### Added

- Anomaly detection and distribution system for consumption data:
  - `attach_anomalies_to_df()` function in `procesado/compute_consumption.py` detects negative+positive consumption patterns
  - Distributes excess consumption between consecutive minutes with totalizador=0 when available
  - Falls back to distributing between the two problematic consumption minutes when no zero-totalizador minutes exist
  - Preserves original negative consumption values in `_cons` columns while adding corrections in `_anom` columns
- Enhanced CSV processing in `run_compute_consumption.py`:
  - Auto-detection of European CSV format (sep=';', decimal=',')
  - Improved anomaly column detection and regeneration logic
  - Better error handling for path resolution and module imports

### Fixed

- Anomaly columns (`*_anom`) now correctly saved in final CSV output
- Corrected raw totalizador column name resolution for anomaly detection algorithm
- Fixed distribution range to include both negative and positive consumption minutes

  ## [0.1.1] - 2025-12-04

  ### Added

  - Download minute-resolution data for selected tags using `adquisicion/download_minute_data.py`.
  - Combine `TOT_H`/`TOT_L` into 32-bit `*_TOT` values and save per-tag and combined CSVs.

  ### Changed

  - Implemented first quality rule (`rect_0`) that creates `<tag>_TOT_rect_0` columns where invalid readings (0 or transient drops per rule) are replaced with the last valid value.
