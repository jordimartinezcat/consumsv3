# Decisiones Técnicas - Consums_v3

Registro de decisiones técnicas importantes tomadas durante el desarrollo del proyecto, con su contexto y justificación.

---

## 📋 Índice de Decisiones

1. [Python 3.13 como versión base](#1-python-313)
2. [psycopg v3 en lugar de v2](#2-psycopg-v3)
3. [OAuth2 para autenticación email (no SMTP básico)](#3-oauth2-para-email)
4. [Vista PostgreSQL para validación mensual](#4-vista-postgresql)
5. [Formato CSV europeo (semicolon, comma decimal)](#5-csv-europeo)
6. [Submódulo Git para CAT_Conexions](#6-submódulo-git)
7. [Windows Task Scheduler (no cron)](#7-task-scheduler)
8. [DELETE + INSERT (no UPSERT)](#8-delete-insert)
9. [Procesamiento en 3 fases (consumo → anomalías → resets)](#9-procesamiento-3-fases)
10. [per10 en adquisición (no en procesado)](#10-per10-en-adquisición)
11. [PDF horizontal A4 (no vertical)](#11-pdf-horizontal)
12. [Keep a Changelog format](#12-keep-a-changelog)

---

## 1. Python 3.13

**Decisión**: Usar Python 3.13 como versión base del proyecto.

**Contexto**:
- Proyecto iniciado en diciembre 2025
- Python 3.13 released en octubre 2024 (stable)
- Servidor Windows con Python 3.13 disponible

**Alternativas consideradas**:
- Python 3.11 (LTS más conservador)
- Python 3.12 (balance estabilidad/features)

**Razones**:
✅ Performance mejorado (JIT compiler experimental)  
✅ Mejor manejo de errores y stack traces  
✅ Type hints mejorados (útil para desarrollo)  
✅ Compatible con todas las dependencias (pandas, psycopg, MSAL)  

**Consecuencias**:
- ✅ Mayor velocidad procesamiento pandas
- ✅ Mejor debugging con stack traces claros
- ⚠️ Requiere Python 3.13 en cualquier entorno nuevo

**Estado**: ✅ Implementado, funciona correctamente en producción

---

## 2. psycopg v3

**Decisión**: Usar psycopg versión 3 en lugar de psycopg2.

**Contexto**:
- Submódulo CAT_Conexions requiere psycopg v3
- psycopg v3 es la versión moderna (asyncio, mejor performance)
- API diferente a psycopg2

**Alternativas consideradas**:
- psycopg2 (legacy, más documentación disponible)
- SQLAlchemy (ORM, más abstracción)

**Razones**:
✅ Requerido por CAT_Conexions (dependencia necesaria)  
✅ Mejor performance (pool de conexiones nativo)  
✅ API moderna y pythonic  
✅ COPY para bulk insert (más rápido que executemany)  

**Código impactado**:
```python
# psycopg v3 syntax
import psycopg

with psycopg.connect(**db_config) as conn:
    with conn.cursor() as cursor:
        cursor.execute(query)
```

**Consecuencias**:
- ✅ Compatible con submódulo CAT_Conexions
- ✅ Bulk INSERT más rápido (~5x vs psycopg2)
- ⚠️ Requiere cambio de código si se migra desde psycopg2

**Estado**: ✅ Implementado, funciona correctamente

---

## 3. OAuth2 para Email

**Decisión**: Usar OAuth2 (MSAL) para autenticación email en lugar de SMTP básico con contraseña.

**Contexto**:
- Microsoft deprecó autenticación básica SMTP en 2022
- Cuenta corporativa @ccaait.cat usa Microsoft 365
- Requerimiento de seguridad corporativa

**Alternativas consideradas**:
- SMTP básico con contraseña (ya no soportado por Microsoft)
- App passwords (limitado en cuentas corporativas)
- SendGrid u otro servicio tercero (coste adicional)

**Razones**:
✅ Única opción soportada por Microsoft 365  
✅ Más seguro (tokens temporales, no contraseñas en código)  
✅ Token refresh automático (no requiere reautenticación)  
✅ Permisos granulares (solo SMTP.Send)  

**Implementación**:
```python
from msal import PublicClientApplication

app = PublicClientApplication(
    client_id=config['oauth2_client_id'],
    authority="https://login.microsoftonline.com/common"
)
```

**Consecuencias**:
- ✅ Seguridad mejorada (sin contraseñas en config)
- ✅ Cumple con políticas Microsoft 365
- ⚠️ Requiere configuración inicial en Azure Portal
- ⚠️ Primera ejecución debe ser interactiva (autorización navegador)
- ✅ Posteriores ejecuciones 100% automáticas

**Documentación**: `docs/EMAIL_SETUP.md`

**Estado**: ✅ Implementado, automatizado en producción

---

## 4. Vista PostgreSQL

**Decisión**: Usar vista `ite_v_consums_24h` como fuente de verdad para validación mensual.

**Contexto**:
- Validación mensual debe comparar contra datos rectificados manualmente
- DBA puede hacer correcciones directamente en BD
- CSV históricos pueden no incluir últimas rectificaciones

**Alternativas consideradas**:
- Leer CSVs procesados históricos (desactualizados)
- Query directa a tabla raw (no incluye correcciones)
- Crear tabla de auditoría (duplicación datos)

**Razones**:
✅ Vista incluye TODAS las rectificaciones (fuente de verdad única)  
✅ Mantenida por DBA (no requiere sincronización)  
✅ Performante (vista materializada o indexada)  
✅ Consulta simple con JOIN a cfg_tags  

**Query usada**:
```sql
SELECT 
    t.tag_name as signal,
    SUM(c.consum) as sum_consumption
FROM ga_datalake.ite_v_consums_24h c
INNER JOIN ga_datalake.cfg_tags t ON c.id_tag = t.id_tag
WHERE c.timestamp_utc >= %s AND c.timestamp_utc < %s
GROUP BY t.tag_name
```

**Consecuencias**:
- ✅ Validación mensual 100% precisa
- ✅ Incluye correcciones manuales posteriores
- ⚠️ Depende de que vista esté actualizada (responsabilidad DBA)

**Estado**: ✅ Implementado en validación mensual

---

## 5. CSV Europeo

**Decisión**: Usar formato CSV europeo (separador `;`, decimal `,`) para archivos intermedios.

**Contexto**:
- España/Cataluña usa formato numérico europeo
- Excel español espera punto y coma como separador
- Compatibilidad con herramientas locales

**Alternativas consideradas**:
- CSV anglosajón (separador `,`, decimal `.`) - estándar internacional
- TSV (Tab-separated) - menos ambiguo
- Parquet (binario, más eficiente) - requiere herramientas especiales

**Razones**:
✅ Compatible con Excel español (abre directamente)  
✅ Formato esperado por usuarios catalanes  
✅ Evita confusión con decimales (1.234,56 vs 1,234.56)  

**Implementación**:
```python
# Escribir CSV europeo
df.to_csv(output_path, sep=';', decimal=',', index=False)

# Leer con auto-detección
df = pd.read_csv(input_path, sep=None, engine='python')
```

**Consecuencias**:
- ✅ Excel abre archivos directamente sin importar
- ✅ Usuarios entienden formato numérico fácilmente
- ⚠️ Requiere conversión para herramientas internacionales

**Estado**: ✅ Implementado en procesado/

---

## 6. Submódulo Git

**Decisión**: Usar CAT_Conexions como submódulo Git (no instalación pip).

**Contexto**:
- CAT_Conexions es librería interna CAT (no en PyPI)
- Múltiples proyectos lo usan
- Necesita actualizaciones frecuentes

**Alternativas consideradas**:
- Copiar código directamente (duplicación, difícil mantener)
- Instalación pip local (requiere build cada vez)
- Monorepo (todos proyectos juntos, demasiado acoplado)

**Razones**:
✅ Código compartido entre proyectos  
✅ Fácil actualización (`git submodule update`)  
✅ Versionado específico por proyecto (branch `wip/consums-mods`)  
✅ No requiere publicar a PyPI  

**Setup**:
```bash
git submodule add https://github.com/CAT/CAT_Conexions.git
git submodule update --init --recursive
cd CAT_Conexions
git checkout wip/consums-mods
```

**Consecuencias**:
- ✅ Sincronización fácil con mejoras upstream
- ✅ Branch específico para cambios de Consums
- ⚠️ Requiere `git submodule update` al clonar
- ⚠️ Commits en submódulo deben hacerse con cuidado

**Estado**: ✅ Implementado, branch `wip/consums-mods` activo

---

## 7. Task Scheduler

**Decisión**: Usar Windows Task Scheduler para automatización (no cron u otros).

**Contexto**:
- Servidor es Windows Server
- Pipeline debe ejecutarse diariamente a las 02:00 AM
- Debe correr aunque nadie esté conectado

**Alternativas consideradas**:
- cron (requiere WSL o Cygwin, no nativo)
- Airflow (overkill para tarea simple)
- Azure Functions (coste cloud, requiere migración)
- Python schedule (requiere proceso siempre corriendo)

**Razones**:
✅ Nativo Windows (no dependencias extra)  
✅ Robusto (logs, reintentos, notificaciones)  
✅ GUI fácil para soporte (no solo línea comandos)  
✅ Ejecuta aunque usuario no loggeado  

**Configuración**:
```
Nombre: Consums Daily Report
Trigger: Diario @ 02:00 AM
Action: C:\Python313\python.exe
Arguments: daily_report.py
Start in: D:\Projects\Python\Consums_v3
Run whether user is logged on or not: Yes
```

**Consecuencias**:
- ✅ Automatización confiable 100% uptime
- ✅ Fácil modificar horario desde GUI
- ⚠️ Específico Windows (no portable a Linux)

**Documentación**: `docs/AUTOMATIZACION.md`

**Estado**: ✅ Implementado, ejecuta diariamente

---

## 8. DELETE + INSERT

**Decisión**: Usar estrategia DELETE + INSERT en lugar de UPSERT/ON CONFLICT.

**Contexto**:
- Pipeline puede reprocesar días históricos
- Datos pueden cambiar (correcciones, nuevos resets detectados)
- No hay clave primaria única obvia (timestamp + id_tag son compuestos)

**Alternativas consideradas**:
- UPSERT (ON CONFLICT UPDATE) - requiere constraint único
- INSERT ignorando duplicados - no actualiza correcciones
- Merge temporal table - más complejo

**Razones**:
✅ Simple de implementar  
✅ Garantiza datos limpios (no duplicados)  
✅ Permite reprocesamiento total  
✅ Performance aceptable (~5K registros/día)  

**Implementación**:
```python
# 1. Borrar período específico
cursor.execute("""
    DELETE FROM ga_datalake.ite_tags_consums
    WHERE timestamp_utc >= %s AND timestamp_utc < %s
""", (start_timestamp, end_timestamp))

# 2. Bulk insert nuevo
with cursor.copy("COPY ga_datalake.ite_tags_consums FROM STDIN") as copy:
    for row in data:
        copy.write_row(row)
```

**Consecuencias**:
- ✅ Reprocesamiento fácil (solo ejecutar de nuevo)
- ✅ No requiere constraint único complejo
- ⚠️ DELETE puede ser lento si período grande (mitigado con índice en timestamp)

**Estado**: ✅ Implementado en persistencia/

---

## 9. Procesamiento 3 Fases

**Decisión**: Procesar consumo en 3 fases secuenciales (consumo → anomalías → resets).

**Contexto**:
- Anomalías regulares (pares neg/pos) deben procesarse primero
- Resets son anomalía especial (solo negativos grandes)
- Orden importa para no interferir detecciones

**Alternativas consideradas**:
- Fase única (detectar todo junto) - confunde resets con anomalías
- Dos fases (anomalías+resets juntos) - threshold ambiguos
- Detección paralela independiente - resultados inconsistentes

**Razones**:
✅ Separación de responsabilidades clara  
✅ Evita falsos positivos (reset detectado como anomalía)  
✅ Facilita debugging (output intermedio por fase)  
✅ Permite ajustar thresholds independientemente  

**Implementación**:
```python
# Fase 1: Calcular consumo básico
df = append_minute_consumption(df, signal_prefixes)

# Fase 2: Detectar y redistribuir anomalías regulares
df = attach_anomalies_to_df(df, signal_prefixes)

# Fase 3: Detectar resets (solo en negativos no redistribuidos)
df = detect_counter_resets(df, signal_prefixes)
```

**Consecuencias**:
- ✅ Lógica clara y mantenible
- ✅ Detección precisa (no falsos positivos)
- ⚠️ 3 pasadas sobre datos (mitigado por pandas vectorización)

**Estado**: ✅ Implementado en procesado/compute_consumption.py

---

## 10. per10 en Adquisición

**Decisión**: Aplicar multiplicador per10 en fase de adquisición (no en procesado).

**Contexto**:
- Intento inicial aplicaba per10 DESPUÉS de calcular consumo
- Causaba consumos incorrectos (consumo ya calculado, multiplicar x10 no tiene sentido)
- Debe aplicarse a TOTALIZADORES, no a consumos

**Error original**:
```python
# ❌ INCORRECTO (en procesado/)
df[f"{signal}_cons"] = df[f"{signal}_cons"] * 10  # Multiplica consumo (mal)
```

**Solución correcta**:
```python
# ✅ CORRECTO (en adquisicion/)
df[f"{signal}_TOT32"] = df[f"{signal}_TOT32"] * 10  # Multiplica totalizador
# Luego calcular consumo sobre totalizador corregido
```

**Razones**:
✅ Lógicamente correcto (corregir lectura hardware)  
✅ Consumo se calcula automáticamente correcto  
✅ Validación usa totalizador corregido también  

**Orden correcto**:
```
1. Combinar TOT_L + TOT_H → TOT32
2. Aplicar per10 (TOT32 × 10)          👈 AQUÍ
3. Calcular consumo (TOT32[t+1] - TOT32[t])
```

**Consecuencias**:
- ✅ Consumos correctos para 18 señales
- ✅ Validación correcta (usa mismo totalizador)
- ⚠️ Requiere reprocesar históricos si se cambió

**Movido desde**: `procesado/run_compute_consumption.py`  
**Movido a**: `adquisicion/run_compute_for_minutes.py`

**Estado**: ✅ Corregido, documentado en CHANGELOG

---

## 11. PDF Horizontal

**Decisión**: Generar PDFs en formato A4 horizontal (landscape) en lugar de vertical.

**Contexto**:
- Tablas de validación tienen muchas columnas (señal, consumo, API, diferencia, %, estado)
- Vertical causa wrapping o texto muy pequeño
- Informes se ven principalmente en pantalla (no impresión)

**Alternativas consideradas**:
- A4 vertical (estándar, pero tablas no caben)
- A3 horizontal (demasiado grande, difícil imprimir)
- Múltiples páginas por señal (demasiadas páginas)

**Razones**:
✅ Tablas caben cómodamente sin wrapping  
✅ Fuente legible sin reducir tamaño  
✅ Mejor para visualización en pantalla (16:9)  
✅ Fácil scrolling vertical (muchas señales)  

**Implementación**:
```python
from reportlab.lib.pagesizes import A4, landscape

doc = SimpleDocTemplate(
    pdf_path,
    pagesize=landscape(A4),  # 297mm × 210mm (horizontal)
    ...
)
```

**Consecuencias**:
- ✅ Informes legibles sin zoom
- ✅ Todas columnas visibles sin cortar
- ⚠️ Si se imprime, usar configuración landscape

**Estado**: ✅ Implementado en validacions/generate_validation_report.py

---

## 12. Keep a Changelog

**Decisión**: Usar formato Keep a Changelog para CHANGELOG.md.

**Contexto**:
- CHANGELOG debe ser legible por humanos
- Necesita estructura consistente
- Versionado semántico (0.1.0, 0.2.0, etc.)

**Alternativas consideradas**:
- Git log raw (demasiado técnico, commits granulares)
- Formato libre (inconsistente)
- Conventional Commits (más para automation)

**Razones**:
✅ Estándar de la industria  
✅ Estructura clara (Added/Changed/Fixed/Removed)  
✅ Fácil ver qué cambió en cada versión  
✅ Legible por no-técnicos  

**Formato**:
```markdown
## [0.4.0] - 2025-12-05

### Added
- Nueva característica X

### Changed
- Modificación Y

### Fixed
- Bug Z corregido
```

**Consecuencias**:
- ✅ Historial claro y navegable
- ✅ Fácil generar release notes
- ⚠️ Requiere disciplina al documentar cambios

**Estado**: ✅ Implementado, usado consistentemente

---

## 📝 Plantilla para Nuevas Decisiones

Al añadir una decisión técnica, usar esta plantilla:

```markdown
## N. Título de la Decisión

**Decisión**: Qué se decidió hacer.

**Contexto**: Por qué se tomó la decisión, qué problema resuelve.

**Alternativas consideradas**:
- Opción A (por qué no)
- Opción B (por qué no)

**Razones**:
✅ Razón positiva 1  
✅ Razón positiva 2  

**Implementación** (código ejemplo si aplica):
```python
# Código relevante
```

**Consecuencias**:
- ✅ Consecuencia positiva
- ⚠️ Trade-off o limitación

**Estado**: ✅ Implementado / ⚠️ En progreso / ❌ Rechazado
```

---

## 🎯 Resumen de Decisiones

| # | Decisión | Razón Principal | Impacto |
|---|----------|-----------------|---------|
| 1 | Python 3.13 | Performance y features modernos | 🟢 Bajo |
| 2 | psycopg v3 | Requerido por submódulo | 🟠 Medio |
| 3 | OAuth2 email | Única opción Microsoft 365 | 🟠 Medio |
| 4 | Vista PostgreSQL | Fuente de verdad única | 🟢 Bajo |
| 5 | CSV europeo | Compatibilidad Excel ES | 🟢 Bajo |
| 6 | Submódulo Git | Código compartido CAT | 🟠 Medio |
| 7 | Task Scheduler | Nativo Windows | 🟢 Bajo |
| 8 | DELETE + INSERT | Simplicidad reprocesamiento | 🟢 Bajo |
| 9 | 3 fases procesamiento | Precisión detección | 🟠 Medio |
| 10 | per10 en adquisición | Lógica correcta | 🔴 Alto |
| 11 | PDF horizontal | Legibilidad tablas | 🟢 Bajo |
| 12 | Keep a Changelog | Estándar industria | 🟢 Bajo |

---

## ✅ Al Tomar Nuevas Decisiones

Checklist:

- [ ] Documentar en este archivo con la plantilla
- [ ] Actualizar CHANGELOG.md si afecta funcionalidad
- [ ] Actualizar context/RULES.md si afecta reglas de negocio
- [ ] Actualizar context/ARCHITECTURE.md si afecta flujo
- [ ] Avisar a equipo si decisión tiene impacto alto
- [ ] Considerar migración si cambia decisión anterior
