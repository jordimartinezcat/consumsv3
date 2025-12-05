# Copilot Instructions for Consums Project

## Project Overview
This is an industrial IoT data processing system for water consumption monitoring from CAT infrastructure. The project extracts sensor data from PostgreSQL/APIs, processes totalizer readings, and computes consumption metrics with anomaly detection.

## Architecture & Data Flow

### Core Pipeline (v3)
1. **Signal Extraction** (`adquisicion/extraer_senales_ftr.py`): Query PostgreSQL to find `*_TOT_L`/`*_TOT_H` signals, filter exclusions (`ET*`, `*_LS_*`, `*_P_*`)
2. **Data Download** (`adquisicion/download_minute_data.py`): Fetch minute-resolution data via `apiSagedCAT`, combine 16-bit pairs into 32-bit totals
3. **Consumption Computing** (`procesado/compute_consumption.py`): Calculate consumption as `next_minute_total - current_minute_total`, handle counter resets and anomalies

### Key Components
- **CAT_Conexions**: Git submodule (`wip/consums-mods` branch) providing `pgDataLake`, `apiSagedCAT` connection classes
- **Configuration**: `consums_config.json` drives task execution, API credentials, processing parameters
- **Data Processing**: Two-stage (16→32 bit reconstruction, then consumption calculation with reset handling)

## Critical Patterns

### Module Import Pattern
```python
# Standard pattern for accessing CAT_Conexions submodule
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "CAT_Conexions", "src"))
from conexions import apiSagedCAT, pgDataLake
```

### Configuration-Driven Execution
Tasks are controlled via `consums_config.json`:
- `fetch_api_data`: Download from SagedCAT API with filters
- `save_to_csv`: Output to timestamped files in structured directories
- `compute_consumption`: Process totals with anomaly detection

### Reset Detection & Handling
Counter resets (negative consumption) trigger minute-level requerying to compute accurate hourly sums. Use `force_minute_requery_hours` in config to manually trigger reanalysis.

### Data Processing Conventions
- **Timestamps**: Always handle timezone conversion (Europe/Madrid local → UTC for storage)
- **Column Naming**: `*_TOT_L`/`*_TOT_H` → `*_TOT32` → `*_CSM` (consumption)
- **Error Handling**: Extensive logging with prefix/tag context for debugging

## Development Workflow

### Environment Setup (v2 reference)
```powershell
git submodule update --init --recursive
.\setup_env.ps1 -PythonExe python  # Creates .venv312
```

### Common Operations
```bash
# Extract available signals
python .\adquisicion\extraer_senales_ftr.py

# Download and process minute data
python .\adquisicion\download_minute_data.py

# Compute consumption with anomaly detection
python .\procesado\run_compute_consumption.py

# Generate hourly aggregation with anomaly corrections
python .\procesado\run_hourly_aggregation.py
```

### Core Processing Scripts
- `procesado/compute_consumption.py`: Core anomaly detection and consumption calculation module
- `procesado/run_compute_consumption.py`: Main execution script, outputs to `procesado/Data/`
- `procesado/compute_hourly_consumption.py`: Hourly aggregation module for minute-level consumption data
- `procesado/run_hourly_aggregation.py`: Script to generate hourly consumption summaries with anomaly corrections
- Reference v2 `scripts/` for debugging patterns if needed

## Integration Points

### Database Connections
- **PostgreSQL**: `pgDataLake` class for signal metadata queries
- **SagedCAT API**: `apiSagedCAT` for time-series data with vista/token auth

### File I/O Patterns
- **Input**: `senales_para_descarga.txt` (one signal per line)
- **Output**: Raw data in `adquisicion/minute_data/`, processed results in `procesado/Data/`
- **Config**: JSON-driven with manual overrides for specific timestamps

## Anti-Patterns to Avoid
- Don't modify CAT_Conexions locally (it's a tracked submodule)
- Avoid hardcoded signal names - use config filters (`PBD07`, etc.)
- Never skip timezone handling in datetime operations
- Don't ignore negative consumption without reset analysis

## Testing & Validation
- Consumption calculations must handle counter resets gracefully
- Minute-level aggregation should match hourly totals when no resets occur  
- Audit columns (`*_CSM_ADJUSTED`) track all processing interventions
- Use `force_minute_requery_hours` config for reproducible debugging

When working on this codebase, prioritize understanding the signal extraction logic and consumption calculation patterns, as these form the core business logic for industrial telemetry processing.