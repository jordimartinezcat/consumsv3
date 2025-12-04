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

