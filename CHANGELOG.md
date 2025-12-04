# Changelog

All notable changes to this project will be documented in this file.

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


