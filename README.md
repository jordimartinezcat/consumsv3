# Consums v3 - Sistema de Monitorització de Consums d'Aigua Industrial CAT

## 📋 Descripció

Sistema automatitzat per al processament diari de dades de consum d'aigua de la infraestructura del Consorci d'Aigües de Tarragona (CAT). Extreu dades de sensors, processa totalitzadors, calcula consums amb detecció d'anomalies, valida resultats i envia informes automàtics per correu electrònic.

## 🎯 Característiques Principals

- ✅ **Extracció automàtica** de dades de l'API SagedCAT
- ✅ **Processament de totalitzadors** (16-bit → 32-bit, _TOT_L/_TOT_H → _TOT32)
- ✅ **Càlcul de consums** amb detecció automàtica de resets de comptador
- ✅ **Validació** contra totalitzadors API amb categorització d'anomalies
- ✅ **Informes PDF** amb logo corporatiu i estadístiques detallades
- ✅ **Enviament automàtic per email** amb autenticació OAuth2 i firma HTML
- ✅ **Sistema de logs** per monitorització i debugging
- ✅ **Emmagatzematge en PostgreSQL** (Azure)

## 🏗️ Arquitectura del Sistema

### Flux de Dades

```
API SagedCAT → Descàrrega Dades → Processament Consums → Validació → Email + BD PostgreSQL
     ↓              ↓                      ↓                ↓              ↓
  Sensors      minute_data/        consumption_minutes    PDF/CSV    goaigua_data
               (TOT_L/TOT_H)       (amb anomalies)        (català)   (Azure)
```

### Components Principals

```
Consums_v3/
├── adquisicion/                    # Descàrrega de dades
│   ├── extraer_senales_ftr.py     # Extracció senyals de PostgreSQL
│   └── download_minute_data.py     # Descàrrega dades minutals API
│
├── procesado/                      # Processament de consums
│   ├── compute_consumption.py      # Càlcul consums amb anomalies
│   └── compute_hourly_consumption.py  # Agregació horària
│
├── persistencia/                   # Inserció a base de dades
│   └── save_hourly_consumption.py  # DELETE + INSERT bulk a PostgreSQL
│
├── validacions/                    # Validació i informes
│   ├── validate_consumption.py     # Validació diària vs API
│   ├── validate_monthly_consumption.py  # Validació mensual vs API
│   ├── generate_validation_report.py    # Generació PDF (català)
│   └── firma.html                  # Firma corporativa per email
│
├── email_utils/                    # Enviament email
│   ├── oauth2.py                   # Autenticació OAuth2 Microsoft
│   └── sender.py                   # Enviament email HTML amb firma
│
├── daily_report.py                 # Script automatització diària + mensual
├── send_last_report.py             # Reenviar últim informe
├── run_monthly_validation.py       # Validació mensual manual
├── run_pipeline.py                 # Pipeline complert manual
└── consums_config.json             # Configuració central
```

## 🚀 Automatització Diària i Mensual

### Script Principal: `daily_report.py`

Executa automàticament cada dia:

1. **Càlcul automàtic del període** (ahir 00:00 → avui 00:00)
2. **Actualització de configuració** amb dates calculades
3. **Execució del pipeline complet**:
   - Descàrrega dades de l'API
   - Processament totalitzadors
   - Càlcul consums amb detecció anomalies
   - Inserció a base de dades
4. **Validació diària automàtica**:
   - Comparació consums vs totalitzadors API
   - Generació CSV amb resultats
   - Generació PDF amb informe detallat
5. **Validació mensual automàtica** (només dia 1 del mes):
   - Validació del mes anterior complert
   - Comparació: Tot(01/MM) vs Tot(01/MM+1)
   - Generació CSV mensual amb resultats
6. **Enviament per email**:
   - Format HTML amb estils
   - Firma corporativa HTML
   - Adjunts: PDF + CSV diaris
   - Adjunt addicional: CSV mensual (si és dia 1)

### Ús Manual

# Si és dia 1, també executa validació mensual
python daily_report.py

# Executar validació mensual manualment
python run_monthly_validation
# Activar entorn virtual
.\.venv\Scripts\Activate.ps1

# Executar procés diari (processa dia anterior)
python daily_report.py
```

### Programació Automàtica

**Windows Task Scheduler**:
- **Nom**: Consums Daily Report
- **Hora**: 02:00 AM cada dia
- **Programa**: `C:\Python313\python.exe`
- **Arguments**: `daily_report.py`
- **Directori**: `D:\Projects\Python\Consums_v3`
- **Opcions**: Executar encara que l'usuari no hagi iniciat sessió

Consulta documentació detallada: [docs/AUTOMATIZACION.md](docs/AUTOMATIZACION.md)

## ⚙️ Configuració

### 1. Base de Dades i API

Configurar a `consums_config.json`:

```json
{
  "db": {
    "host": "your-postgresql-host.database.azure.com",
    "port": 5432,
    "database": "your_database_name",
    "user": "your_database_user",
    "password": "your_secure_password"
  },
  "api": {
    "base_url": "https://your-api-endpoint.com/api",
    "nexustoken": "your-nexus-token-here",
    "vista": "your-vista-id-here"
  }
}
```

### 2. Email amb OAuth2

**⚠️ Configuració inicial interactiva requerida (només una vegada)**

Passos:
1. Registra aplicació a [Azure Portal](https://portal.azure.com)
2. Obté **Client ID**
3. Configura permisos: `SMTP.Send` (delegat)
4. Actualitza `consums_config.json`:

```json
{
  "email": {
    "enabled": true,
    "smtp_server": "smtp-mail.outlook.com",
    "smtp_port": 587,
    "smtp_user": "consums@ccaait.cat",
    "smtp_tls": true,
    "oauth2_client_id": "02f1b85c-441f-4f77-9be3-65a826615d96",
    "oauth2_token_cache": "token_cache.json",
    "from_addr": "consums@ccaait.cat",
    "recipients": [
      "jmartinez@ccaait.cat",
      "lperez@ccaait.cat"
    ]
  }
}
```

5. Primera execució (autorització manual):
```powershell
python daily_report.py
# Segueix instruccions per autoritzar al navegador
```

6. ✅ Execucions posteriors seran automàtiques (sense intervenció)

**Guia completa**: [docs/EMAIL_SETUP.md](docs/EMAIL_SETUP.md)
Validació Diària

Executa **cada dia** per validar el consum del dia anterior:
- **Període**: Dia anterior 00:00 → Dia actual 00:00
- **Comparació**: Tot(dia+1 00:00) - Tot(dia 00:00) vs Σ(consums 24 hores)
- **Generació**: PDF + CSV diaris

### Validació Mensual

Executa **automàticament el dia 1 de cada mes** per validar el mes anterior complert:
- **Període**: Dia 01/MM/AAAA 00:00 → Dia 01/MM+1/AAAA 00:00
- **Comparació**: Tot(01/mes+1) - Tot(01/mes) vs Σ(consums tot el mes)
- **Generació**: CSV mensual amb estadístiques agregades
- **Exemple**: El 1 de juny valida tot el mes de maig

### Tipus de Resultats

**✅ OK (perfectes)**: Consum calculat coincideix exactament amb diferència de totalitzadors API

**✅ OK (amb resets)**: Resets de comptador detectats (65.536L o múltiples) i corregits automàticament
```

## 📊 Sistema de Validació

### Tipus de Resultats

**✅ OK (perfectes)**: Consum calculat coincideix exactament amb diferència de totalitzadors API

**✅ OK (amb reset 16-bit)**: Reset de 65.536L detectat i corregit automàticament en el càlcul horari

**⚠️ Discrepàncies**: Diferències no categorizades que requereixen anàlisi individual

**❌ Errors**: Senyal sense dades disponibles a l'API durant el període

### Informe PDF (Català)

El PDF generat inclou:

**Pàgina 1 - Resum**:
- Logo corporatiu (assets/logo.jpg)
- Estadístiques generals
- Taula resum amb percentatges

**Secció 1 - Errors**:
- Senyals sense dades a l'API
- Missatge d'error per senyal

**Secció 2 - Resets Detectats** (65.536L):
- Resets del comptador LOW (16 bits)
- **Ja corregits** en el càlcul horari
- Confirmació que la correcció és correcta

**Secció 3 - Altres Discrepàncies**:
- Senyals amb diferències no categorizades
- Requereixen anàlisi cas per cas

### Email Automàtic (Català)

**Asunto**: `[Consums] Informe de Validació - YYYY-MM-DD`

**Format**: HTML amb estils CSS + firma corporativa

**Contingut**:
```
Informe de Validació de Consums - 2026-05-21

Període: 2026-05-21 00:00:00 → 2026-05-22 00:00:00

RESUM DE VALIDACIÓ
══════════════════════════════════════════════════════════════════
Total senyals processades: 238
  ✓ OK (perfectes):         225 (94.5%)
  ⚠ Discrepàncies:          0 (0.0%)
  ✗ Errors (sense dades):   13 (5.5%)

L'informe detallat s'adjunta en format PDF.
Les dades completes estan disponibles a l'arxiu CSV adjunt.
```

**Adjunts**:
- `validation_report_YYYYMMDD_HHMMSS.pdf`
- `validation_report_YYYYMMDD_HHMMSS.csv`

## 🔧 Ús del Sistema

### Processar Dia Anterior (Automàtic)

```powershell
python daily_report.py
```

### Processar Període Personalitzat

1. Editar `consums_config.json`:
```json
{
  "period": {
    "start": "2026-05-19 00:00:00",
    "end": "2026-05-20 00:00:00"
  }
}ecutar Validació Mensual Manualment

```powershell
python run_monthly_validation.py
```

### Ex
```

2. Executar pipeline:
```powershell
python run_pipeline.py
```

3. Validar resultats:
```powershell
python validacions/validate_consumption.py
```

### Reenviar Últim Informe

```powershell
python send_last_report.py
```

### Extreure Noves Senyals    # Resultats validació diària
│   ├── validation_report_*.pdf         # Informe visual diari (català)
│   └── validation_monthly_YYYYMM_*.csv # Resultats validació mensual
```powershell
python adquisicion/extraer_senales_ftr.py
```

## 📁 Dades i Logs

### Estructura de Fitxers

```
Consums_v3/
├── adquisicion/minute_data/        # Dades minutals API
│   ├── all_minutes_*.csv           # Totals sense processament
│   └── combined_*.csv              # TOT_L + TOT_H → TOT32
│
├── procesado/Data/                 # Consums processats
│   ├── consumption_minutes_with_anom_*.csv  # Amb detecció anomalies
│   └── consumption_hourly_*.csv    # Agregació horària
│
├── validacions/                    # Informes de validació
│   ├── validation_report_*.csv     # Resultats validació
│   └── validation_report_*.pdf     # Informe visual (català)
│
└── log/                            # Logs d'execució
    └── daily_report_YYYYMMDD.log   # Log procés diari
```

### Monitorització

**Logs diaris**:
```powershell
Get-Content log\daily_report_20260522.log
```

**Últimes validacions**:
```powershell
Get-ChildItem validacions\validation_report_*.pdf | Sort-Object LastWriteTime -Descending | Select-Object -First 5
```

## 🔍 Detecció d'Anomalies

### Tipus d'Anomalies Detectades

1. **Reset Comptador LOW (65.536L)**:
   - Reset del byte baix (16 bits) → 2^16 = 65.536L
   - Detectat quan: `consum_actual < -60.000L`
   - Correcció: `consum_real = consum_negatiu + 65.536L`

2. **Reset Comptador Complet (~10M L)**:
   - Reset total del comptador
   - Detectat quan: `diferència > 9.000.000L`
   - Requereix anàlisi manual

3. **Valors NaN o Faltants**:
   - Dades no disponibles a l'API
   - Marcat com a ERROR en validació

### Configuració Avançada

**Forçar recàlcul horari específic**:
```json
{
  "postprocess": {
    "compute": {
      "force_minute_requery_hours": [
        "2025-06-02 11:00:00"
      ]
    }
  }
}
```

**Override manual d'hora**:
```json
{
  "postprocess": {
    "compute": {
      "manual_overrides": {
        "2025-06-02 11:00:00": {
          "force_minute_sum": 0,
          "tag": "SPIKE",
          "force_replace": true
        }
      }
    }
  }
}
```

## 📝 Canvis Recents (v3.1 - Maig 2026)

### ✨ Noves Funcionalitats

1. **Sistema d'Automatització Completa**:
   - Script `daily_report.py` per execució diària desatendida
   - Càlcul automàtic de període (dia anterior)
   - Logs detallats amb timestamps
   - Gestió d'errors amb logging complet

2. **Email HTML amb OAuth2**:
   - Autenticació OAuth2 amb Microsoft (desatendida després primera autorització)
   - Format HTML amb estils CSS
   - Firma corporativa HTML integrada (`validacions/firma.html`)
   - Text pla alternatiu per compatibilitat
   - Preview HTML disponible: `validacions/email_preview.html`

3. **Informes en Català**:
   - PDF completament traduït al català
   - Email en català amb terminologia tècnica correcta
   - Missatges d'error per senyal en català
   - Firma corporativa del CAT

4. **Validació Millorada**:
   - Timestamp corregit: consulta totalitzador a les 00:00 del dia següent
   - Categorització automàtica d'anomalies
   - Detecció de resets de 65.536L
   - Estadístiques detallades al resum

5. **Compatibilitat Windows Server**:
   - Encoding `cp1252` per subprocess (soluciona errors amb caràcters catalans)
   - Caràcters Unicode (✓/✗/→) reemplaçats per ASCII ([OK]/[ERROR])
   - Gestió d'errors de permisos PostgreSQL (usuari no propietari)

### 🐛 Correccions de Bugs

1. **Fix Prioritat Fitxers** (`compute_hourly_consumption.py`):
   - **Problema**: Prioritzava `all_minutes_*.csv` (sense detecció anomalies) sobre `consumption_minutes_with_anom_*.csv` (amb anomalies)
   - **Solució**: Invertida prioritat per aplicar correccions d'anomalies
   - **Resultat**: Hora 14:00 de BPD01_FTR_G02 ara mostra 245L corregit en lloc de -9.999.755L
   - **Commit**: `6bb6021`

2. **Fix Càlcul Última Hora** (`compute_hourly_consumption.py`):
   - **Problema**: Període acabant a 23:59:59 no tenia totalitzador final per calcular consum hora 23:00
   - **Solució**: Període estès a 00:00 dia següent + filtratge timezone-aware per excloure hora 00:00 de la inserció
   - **Resultat**: 24 hores correctes (00:00-23:00) amb última hora ben calculada
   - **Commit**: `6bb6021`

3. **Fix Timestamp Validació** (`validate_consumption.py`):
   - **Problema**: Validació consultava totalitzador a 23:59:00 però càlcul usava 00:00:00
   - **Solució**: Validació ara consulta a 00:00 del dia següent, consistent amb el càlcul
   - **Resultat**: Discrepàncies reduïdes, només anomalies reals
   - **Commit**: `6bb6021`

4. **Fix Encoding Windows** (`daily_report.py`):
   - **Problema**: `UnicodeDecodeError` amb caràcters catalans (ó, ñ, à) en subprocess
   - **Solució**: Canviat encoding de `utf-8` a `cp1252` amb `errors='replace'`
   - **Resultat**: Pipeline executa correctament en Windows Server
   - **Commit**: `6c8a970`

5. **Fix Caràcters Unicode** (`send_last_report.py`, `download_minute_data.py`):
   - **Problema**: `UnicodeEncodeError: 'charmap' codec can't encode character '\u2717'`
   - **Solució**: Reemplaçat ✓/✗/→ per [OK]/[ERROR] (caràcters ASCII)
   - **Resultat**: Compatible amb consola Windows sense UTF-8
   - **Commit**: `c62941a`

6. **Fix Permisos PostgreSQL** (`save_hourly_consumption.py`):
   - **Problema**: Error "must be owner of table" en crear índexs al servidor
   - **Solució**: Captura específica `ProgrammingError` amb `InsufficientPrivilege`, continua execució
   - **Resultat**: Pipeline funciona encara que usuari no pugui crear índexs
   - **Commit**: `8d5276d`

7. **Fix Dependencies** (`requirements.txt`):
   - **Problema**: Faltava `psycopg` v3 (necessari per CAT_Conexions), només estava v2
   - **Solució**: Afegit `psycopg==3.3.2` i `psycopg-binary==3.3.2`
   - **Resultat**: Imports correctes del submòdul CAT_Conexions
   - **Commit**: `6c8a970`

### 🌐 Internacionalització (i18n)

**Tots els missatges traduïts al català**:

| Component | Abans (Castellà) | Després (Català) |
|-----------|------------------|------------------|
| Errors senyal | "Totalizador no existe" | "Totalitzador no existeix" |
| Resets | "Reset de contador" | "Reset de comptador" |
| Consums | "Error relativo" | "Error relatiu" |
| Resums | "Discrepancias" | "Discrepàncies" |
| Estats | "perfectas" / "con reset" | "perfectes" / "amb reset" |

**Commit**: `3993000`

## 🔒 Seguretat

### Fitxers Sensibles (`.gitignore`)

- ✅ `token_cache.json` - Conté refresh token OAuth2
- ✅ `consums_config.json` - Conté credencials BD i API
- ✅ `*.log` - Logs amb possibles dades sensibles

### Bones Pràctiques

1. **No compartir** `token_cache.json` ni `consums_config.json`
2. **Rotar credencials** periòdicament
3. **Monitoritzar logs** per detectar accessos no autoritzats

## 📚 Documentació de Contexto Persistent

El projecte inclou documentació detallada per facilitar el manteniment i desenvolupament futur:

### Carpeta `context/`

Documentació tècnica per desenvolupadors i manteniment:

- **`context/PROJECT.md`** - Estat actual del projecte
  - Què està en producció
  - Últims desenvolupaments completats
  - Treball en curs i millores futures
  - Operacions comunes
  - Mètriques de producció

- **`context/RULES.md`** - Les 7 regles de negoci crítiques
  - per10 multiplier (18 senyals)
  - Detecció de resets de comptador
  - Última hora del dia (23:00)
  - Timestamps de validació (00:00)
  - Encoding Windows Server (cp1252)
  - Permisos PostgreSQL
  - Idioma català
  - Codi exacte i ubicació de cada regla

- **`context/ARCHITECTURE.md`** - Arquitectura del sistema
  - Diagrama de flux complet (ASCII art)
  - Taula de responsabilitats per mòdul
  - Flux de dades detallat (transformacions)
  - Estructura de fitxers i directoris
  - Integracions externes (API, BD, OAuth2)
  - Flux temporal d'execució diària

- **`context/DECISIONS.md`** - Decisions tècniques
  - 12 decisions tècniques documentades
  - Context, alternatives, raons, conseqüències
  - Python 3.13, psycopg v3, OAuth2, etc.
  - Plantilla per noves decisions

### Carpeta `docs/`

Documentació operativa i guies d'ús:

- **`docs/AUTOMATIZACION.md`** - Configuració Windows Task Scheduler
- **`docs/EMAIL_SETUP.md`** - Configuració OAuth2 Microsoft
- **`docs/VALIDACION_MENSUAL.md`** - Guia validació mensual
- **`docs/DIAGRAMA_VALIDACION_MENSUAL.md`** - Flux detallat validació mensual
- **`docs/per10_multiplier.md`** - Documentació tècnica per10

### Workflow de Desenvolupament

**Abans de modificar codi**:
1. Llegeix `context/RULES.md` → entendre regles de negoci afectades
2. Revisa `context/ARCHITECTURE.md` → entendre flux de dades
3. Consulta `context/PROJECT.md` → estat actual del sistema
4. Revisa `CHANGELOG.md` → què va canviar recentment

**Després de fer canvis**:
1. Actualitza `CHANGELOG.md` (format Keep a Changelog)
2. Si afecta regles: actualitza `context/RULES.md`
3. Si afecta arquitectura: actualitza `context/ARCHITECTURE.md`
4. Si és decisió tècnica important: documenta a `context/DECISIONS.md`
5. Actualitza `context/PROJECT.md` amb nou estat
4. **Backups** de configuració en ubicació segura
5. **Permisos** dels fitxers només per usuari d'execució

## 🆘 Troubleshooting

### Pipeline Falla

**Símptomes**: Error en execució de `run_pipeline.py`

**Revisions**:
1. Verificar connectivitat BD: `telnet 40.85.79.213 5432`
2. Verificar API disponible: `curl https://sagedcat-nex0-vm.xylemvue.goaigua.com:56443/api`
3. Revisar logs: `log/daily_report_*.log`

### Email No Arriba

**Símptomes**: Procés completa OK però no es rep email

**Revisions**:
1. Verificar `email.enabled = true` en config
2. Verificar recipients correctes
3. Revisar carpeta spam/correu no desitjat
4. Verificar token: `ls token_cache.json`
5. Si token expirat: eliminar i reautoritzar

### Token OAuth2 Expirat

**Símptomes**: Error "XOAUTH2 failed"

**Solució**:
```powershell
rm token_cache.json
python daily_report.py  # Reautoritzar interactivament
```

### Discrepàncies en Validació

**Símptomes**: Moltes senyals amb discrepàncies

**Possibles causes**:
1. Resets no detectats (revisar umbral a `compute_consumption.py`)
2. Dades faltants en període consultat
3. Canvi en format de dades API

**Acció**: Revisar secció de discrepàncies en PDF i analitzar patrons

## � Historial de Sessions

### Sessió 22 Maig 2026 - Completar Sistema de Producció

**Objectiu**: Resoldre bug crític BPD01_FTR_G02, processar dataset complet (235 senyals, maig 01-20), automatitzar completament, traduir al català i desplegar a producció.

**Tasques Completades**:
1. ✅ **Debugging i Fixes Crítics**:
   - Fix prioritat fitxers agregació horària (aplicar correccions anomalies)
   - Fix càlcul última hora (extensió període + filtratge timezone)
   - Fix timestamp validació (consulta a 00:00 següent dia)
   - Validat: BPD01_FTR_G02 hora 14:00 = 245.0L (abans -9.999.755.0L)

2. ✅ **Processament Dataset Complet**:
   - 235 senyals × 20 dies = 112.800 registres insertats
   - 93 correccions d'anomalies aplicades
   - Recàlcul dia 19: 5.640 registres, 4 correccions
   - Validació final: 238 senyals, 225 OK, 13 errors

3. ✅ **Sistema d'Automatització**:
   - Creat `daily_report.py` (càlcul automàtic període anterior)
   - Creat `send_last_report.py` (reenviar últim informe)
   - Integració completa: descàrrega → procés → validació → email
   - Logs estructurats: `log/daily_report_YYYYMMDD.log`

4. ✅ **Email amb OAuth2**:
   - Mòdul `email_utils/oauth2.py` (autenticació Microsoft MSAL)
   - Mòdul `email_utils/sender.py` (HTML + firma corporativa)
   - Device flow primera autorització, silent refresh posterior
   - Token cache: `token_cache.json` (30 dies validesa)

5. ✅ **Traducció Completa al Català**:
   - PDF: tots els textos, seccions, missatges d'error
   - Email: assumpte, cos HTML, estadístiques
   - Scripts: missatges de validació, resums, errors per senyal
   - Firma corporativa HTML integrada

6. ✅ **Compatibilitat Windows Server**:
   - Fix encoding subprocess: `utf-8` → `cp1252` + `errors='replace'`
   - Fix caràcters Unicode: `✓✗→` → `[OK][ERROR]` (ASCII)
   - Fix permisos PostgreSQL: captura `InsufficientPrivilege`, continua execució
   - Afegit psycopg v3 a `requirements.txt`

7. ✅ **Documentació i Deployment**:
   - Creat `README.md` complet amb guies d'ús
   - Creat `docs/AUTOMATIZACION.md` (Windows Task Scheduler)
   - Creat `docs/EMAIL_SETUP.md` (OAuth2 Azure Portal)
   - Creat `validacions/email_preview.html` (mostra visual email)
   - Creat `requirements.txt` amb totes les dependències
   - Anonimitzat credencials en documentació
   - `.gitignore` actualitzat (token_cache.json, consums_config.json)

8. ✅ **Control de Versions (GitHub)**:
   - Repository: `https://github.com/jordimartinezcat/consumsv3.git`
   - 8 commits amb missatges descriptius
   - Tots els canvis documentats i versionats

**Resultats Quantitatius**:
- ⏱️ Temps total sessió: ~8 hores
- 📝 Commits: 8 (6bb6021, 4536b28, a8df562, d4f29d5, 6c8a970, 8d5276d, 3993000, c62941a)
- 📊 Dataset processat: 112.800 registres horaris
- 🔧 Bugs resolts: 7 crítics
- 🌐 Traduccions: 100% català
- 📧 Emails prova: 2 enviats correctament
- ✅ Sistema: 100% funcional i automatitzat

**Lliçons Apreses**:
1. **File Priority Matters**: L'ordre de prioritat en lectura de fitxers és crític quan hi ha múltiples fonts de dades amb/sense processament
2. **Period Extension**: Càlcul d'última hora requereix dada següent dia, però amb filtratge per no insertar-la
3. **Timezone Aware**: Tots els filtrats de timestamps han de ser timezone-aware per evitar errors
4. **Windows Compatibility**: Windows Server requereix encoding específic (cp1252) i caràcters ASCII en consola
5. **Database Permissions**: Scripts han de gestionar gracefullly errors de permisos i continuar execució
6. **OAuth2 Token Management**: Token refresh automàtic funciona perfectament després primera autorització interactiva

**Estat Final**: ✅ Sistema en producció, completament funcional, automatitzat i documentat. Llest per desplegar a servidor Windows amb Task Scheduler.

---

## �📞 Suport i Documentació

### Documentació Addicional

- [docs/AUTOMATIZACION.md](docs/AUTOMATIZACION.md) - Guia completa d'automatització
- [docs/EMAIL_SETUP.md](docs/EMAIL_SETUP.md) - Configuració OAuth2 detallada
- [CHANGELOG.md](CHANGELOG.md) - Historial de canvis

### Contacte

**Consorci d'Aigües de Tarragona**
- Email: consums@ccaait.cat
- Tel: 977 636 254

---

**Estat**: ✅ Sistema en producció - Completament funcional i automatitzat

**Última actualització**: 22 maig 2026

**Versió**: v3.1
