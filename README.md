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
│   ├── validate_consumption.py     # Validació vs API
│   ├── generate_validation_report.py  # Generació PDF (català)
│   └── firma.html                  # Firma corporativa per email
│
├── email_utils/                    # Enviament email
│   ├── oauth2.py                   # Autenticació OAuth2 Microsoft
│   └── sender.py                   # Enviament email HTML amb firma
│
├── daily_report.py                 # Script automatització diària
├── send_last_report.py             # Reenviar últim informe
├── run_pipeline.py                 # Pipeline complert manual
└── consums_config.json             # Configuració central
```

## 🚀 Automatització Diària

### Script Principal: `daily_report.py`

Executa automàticament cada dia:

1. **Càlcul automàtic del període** (ahir 00:00 → avui 00:00)
2. **Actualització de configuració** amb dates calculades
3. **Execució del pipeline complet**:
   - Descàrrega dades de l'API
   - Processament totalitzadors
   - Càlcul consums amb detecció anomalies
   - Inserció a base de dades
4. **Validació automàtica**:
   - Comparació consums vs totalitzadors API
   - Generació CSV amb resultats
   - Generació PDF amb informe detallat
5. **Enviament per email**:
   - Format HTML amb estils
   - Firma corporativa HTML
   - Adjunts: PDF + CSV

### Ús Manual

```powershell
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

Ja configurat a `consums_config.json`:

```json
{
  "db": {
    "host": "40.85.79.213",
    "port": 5432,
    "database": "goaigua_data",
    "user": "ga_nifisagecad",
    "password": "UbU8APdhoFxv6"
  },
  "api": {
    "base_url": "https://sagedcat-nex0-vm.xylemvue.goaigua.com:56443/api",
    "nexustoken": "0333adb8-07fa-40d6-8f10-f5e66b6163a9",
    "vista": "560597aa-89e3-43df-9273-7875595319b8"
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

### 3. Dependències

```powershell
pip install pandas sqlalchemy psycopg2 requests msal reportlab pillow
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
}
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

### Extreure Noves Senyals

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
   - Logs detallats amb timestampsç

2. **Email HTML amb OAuth2**:
   - Autenticació OAuth2 amb Microsoft (desatendida després primera autorització)
   - Format HTML amb estils CSS
   - Firma corporativa HTML integrada
   - Text pla alternatiu per compatibilitat

3. **Informes en Català**:
   - PDF completament traduït al català
   - Email en català amb terminologia tècnica correcta
   - Firma corporativa del CAT

4. **Validació Millorada**:
   - Timestamp corregit: consulta totalitzador a les 00:00 del dia següent
   - Categorització automàtica d'anomalies
   - Detecció de resets de 65.536L

### 🐛 Correccions de Bugs

1. **Fix Prioritat Fitxers** (`compute_hourly_consumption.py`):
   - **Problema**: Prioritzava `all_minutes_*.csv` (sense detecció anomalies) sobre `consumption_minutes_with_anom_*.csv` (amb anomalies)
   - **Solució**: Invertida prioritat per aplicar correccions d'anomalies
   - **Resultat**: Hora 14:00 de BPD01_FTR_G02 ara mostra 245L corregit en lloc de -9.999.755L

2. **Fix Càlcul Última Hora** (`compute_hourly_consumption.py`):
   - **Problema**: Període acabant a 23:59:59 no tenia totalitzador final per calcular consum hora 23:00
   - **Solució**: Període estès a 00:00 dia següent + filtratge timezone-aware per excloure hora 00:00 de la inserció
   - **Resultat**: 24 hores correctes (00:00-23:00) amb última hora ben calculada

3. **Fix Timestamp Validació** (`validate_consumption.py`):
   - **Problema**: Validació consultava totalitzador a 23:59:00 però càlcul usava 00:00:00
   - **Solució**: Validació ara consulta a 00:00 del dia següent, consistent amb el càlcul
   - **Resultat**: Discrepàncies reduïdes, només anomalies reals

## 🔒 Seguretat

### Fitxers Sensibles (`.gitignore`)

- ✅ `token_cache.json` - Conté refresh token OAuth2
- ✅ `consums_config.json` - Conté credencials BD i API
- ✅ `*.log` - Logs amb possibles dades sensibles

### Bones Pràctiques

1. **No compartir** `token_cache.json` ni `consums_config.json`
2. **Rotar credencials** periòdicament
3. **Monitoritzar logs** per detectar accessos no autoritzats
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

## 📞 Suport i Documentació

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
