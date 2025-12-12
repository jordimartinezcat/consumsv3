import glob
import json
import logging
import os
import sys
from datetime import datetime

import pandas as pd

# Ajustar el path para importar el submódulo y utilidades
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

sys.path.insert(0, os.path.join(ROOT, "persistencia"))

from procesado.compute_consumption import (
    append_minute_consumption,
    distribute_negative_compensations,
)
from persistencia.db_connection import get_db_connection, get_tag_per10

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# Leer configuración (ajusta el nombre si usas otro archivo)
CONFIG_PATH = os.path.join(ROOT, "consums_config.json")
with open(CONFIG_PATH, "r") as f:
    cfg = json.load(f)

MINUTE_DATA_DIR = os.path.join(ROOT, "adquisicion", "minute_data")
CACHED_SEP_FORMATS = [(";", ","), (",", "."), (",", ","), (";", ".")]


def load_existing_minutes():
    """Return cached minute dataset if previously generated, else None."""

    candidates = []
    default_csv = os.path.join(MINUTE_DATA_DIR, "all_minutes.csv")
    if os.path.exists(default_csv):
        candidates.append(default_csv)

    pattern = os.path.join(MINUTE_DATA_DIR, "all_minutes_*.csv")
    for path in sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True):
        if path not in candidates:
            candidates.append(path)

    def normalize_dataframe(raw_df):
        df = raw_df.copy()

        for noisy_col in ("Unnamed: 0", "index"):
            if noisy_col in df.columns:
                df = df.drop(columns=noisy_col)

        ts_col = None
        for cand in ("timeStamp", "timestamp", "data"):
            if cand in df.columns:
                ts_col = cand
                break

        if ts_col is not None:
            ts_index = pd.to_datetime(df[ts_col], errors="coerce")
            df = df.drop(columns=ts_col)
        else:
            ts_index = pd.to_datetime(df.index, errors="coerce")

        if ts_index.isna().all():
            return None

        df.index = ts_index
        if df.empty or len(df.columns) < 2:
            return None

        # Reject datasets where the delimiter wasn't applied (columns still contain commas)
        if any(
            isinstance(col, str)
            and "," in col
            and col not in {"timeStamp", "timestamp", "data"}
            for col in df.columns
        ):
            return None

        value_columns = [
            c
            for c in df.columns
            if any(
                c.endswith(suffix)
                for suffix in (
                    "_TOT",
                    "_TOT_H",
                    "_TOT_L",
                    "_rect_0",
                    "_cons",
                )
            )
        ]
        if not value_columns:
            return None

        return df

    for candidate in candidates:
        for sep, decimal in CACHED_SEP_FORMATS:
            try:
                raw_df = pd.read_csv(candidate, sep=sep, decimal=decimal)
            except Exception:
                continue

            df = normalize_dataframe(raw_df)
            if df is None:
                continue

            logging.info(
                "Loaded cached minute dataset from %s using sep='%s' decimal='%s'",
                candidate,
                sep,
                decimal,
            )
            return df

    return None


existing_minutes = load_existing_minutes()
if existing_minutes is not None:
    logging.info("Using cached minute dataset; skipping API download")
    df = existing_minutes.copy()
else:
    logging.info("No cached dataset found, importing download module...")
    from download_minute_data import download_minute_data

    # Read API config only when needed
    api_cfg = cfg.get("api", {})
    filter_prefix = None
    for task in cfg.get("tasks", []):
        if task.get("name") == "fetch_api_data":
            filter_prefix = task.get("filter")
            break

    start = cfg.get("period", {}).get("start", "2025-01-01 00:00:00")
    end = cfg.get("period", {}).get("end", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    logging.info(
        f"Fetching minute totalizer data {start} -> {end} (filter: {filter_prefix})"
    )
    combined_df, missing = download_minute_data()
    if combined_df is None or combined_df.empty:
        logging.error("No minute data downloaded or combined dataframe is empty")
        sys.exit(1)
    df = combined_df.copy()

# Asegurar índice datetime
if not isinstance(df.index, pd.DatetimeIndex):
    for cand in ("data", "timeStamp", "timestamp"):
        if cand in df.columns:
            df.index = pd.to_datetime(df[cand], errors="coerce")
            break

print("\nMinute totalizer data (before forward-fill):")
print(df.head())

# Forward-fill NaN values in TOT_H and TOT_L columns
tot_h_cols = [c for c in df.columns if c.endswith("_TOT_H")]
tot_l_cols = [c for c in df.columns if c.endswith("_TOT_L")]

for col in tot_h_cols + tot_l_cols:
    original_nans = df[col].isna().sum()
    if original_nans > 0:
        df[col] = df[col].ffill()
        filled_nans = original_nans - df[col].isna().sum()
        logging.info(f"Forward-filled {filled_nans} NaN values in {col}")

print("\nMinute totalizer data (after forward-fill):")
print(df.head())


# Aquí puedes continuar con el procesamiento, por ejemplo, combinar 16/32 bits o calcular consumos
# Combinar pares TOT_H / TOT_L en una sola columna TOT
def combine_tot_high_low(
    df, near_zero_threshold=1000, max_reasonable_consumption=100000
):
    """
    Combina TOT_H y TOT_L en totalizador de 32 bits con detección de saltos anómalos.

    Un reset real debe cumplir:
    - Consumo muy negativo
    - Valor del totalizador después del salto cercano a 0

    Si no cumple, es un salto anómalo y se marca para corrección (consumo = 0).

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame con columnas TOT_H y TOT_L
    near_zero_threshold : int
        Umbral para considerar que el totalizador está "cerca de 0" después de un reset
    max_reasonable_consumption : int
        Consumo máximo razonable por minuto para detectar posibles saltos
    """
    cols = list(df.columns)
    combined = df.copy()
    processed = set()

    for col in cols:
        if col in processed:
            continue
        if col.endswith("_TOT_H"):
            base = col[:-6]  # remove _TOT_H
            low_col = base + "_TOT_L"
            tot_col = base + "_TOT"
            anomaly_col = base + "_TOT_is_anomalous_jump"

            if low_col in combined.columns:
                # Combine high and low 16-bit parts into 32-bit unsigned int
                import numpy as np

                high_s = pd.to_numeric(combined[col], errors="coerce").fillna(0)
                low_s = pd.to_numeric(combined[low_col], errors="coerce").fillna(0)
                # Convert to numpy int64 arrays for bit ops
                high_arr = high_s.to_numpy(dtype="int64")
                low_arr = low_s.to_numpy(dtype="int64")
                # Mask low to 16 bits and shift high
                result_arr = (high_arr << 16) | (low_arr & 0xFFFF)
                combined[tot_col] = pd.Series(result_arr, index=combined.index)

                # Detectar saltos anómalos
                tot_diff = combined[tot_col].diff()
                is_anomalous_jump = pd.Series(False, index=combined.index)

                # Buscar consumos negativos que podrían ser resets o saltos anómalos:
                # 1. Consumos muy negativos (< -max_reasonable_consumption)
                # 2. Cualquier caída hacia valores cercanos a 0 desde valores >1000
                very_negative = tot_diff < -max_reasonable_consumption

                # Detectar caídas a valores cercanos a 0 desde valores altos
                # diff() calcula current - previous, entonces diff negativo significa current < previous
                # Si current está cerca de 0 y previous era alto, es sospechoso
                tot_current = combined[tot_col]
                tot_previous = combined[tot_col].shift(1)

                falls_to_near_zero = (
                    (tot_diff < -1000)  # caída significativa (diff negativo > 1000)
                    & (tot_current <= near_zero_threshold)  # valor actual cerca de 0
                    & (tot_previous > 1000)  # valor anterior era alto
                )

                possible_resets = very_negative | falls_to_near_zero
                reset_indices = combined[possible_resets].index

                anomaly_count = 0
                reset_count = 0

                for idx in reset_indices:
                    pos = combined.index.get_loc(idx)
                    if pos == 0:
                        continue

                    tot_before = combined[tot_col].iloc[pos - 1]
                    tot_after = combined[tot_col].iloc[pos]

                    # Validar si es reset real (totalizador después cerca de 0)
                    if abs(tot_after) <= near_zero_threshold:
                        # Verificar si el totalizador incrementa después del reset de forma consistente
                        # (ventana de 90 minutos para verificar comportamiento estable)
                        window_size = min(90, len(combined) - pos - 1)
                        incrementa_de_forma_estable = False

                        if window_size > 10:
                            window_values = (
                                combined[tot_col]
                                .iloc[pos : pos + window_size + 1]
                                .values
                            )

                            # Buscar primer valor >100 después del reset
                            first_nonzero_idx = None
                            for i, val in enumerate(window_values[1:], start=1):
                                if val > 100:
                                    first_nonzero_idx = i
                                    break

                            if first_nonzero_idx is not None:
                                # Verificar que el tiempo hasta incrementar sea razonable (<60 minutos)
                                # y que después incremente de forma consistente
                                if first_nonzero_idx <= 60:
                                    # Verificar estabilidad: al menos 5 valores consecutivos >0 después
                                    valores_despues = window_values[
                                        first_nonzero_idx : first_nonzero_idx + 5
                                    ]
                                    if len(valores_despues) >= 5 and all(
                                        v > 0 for v in valores_despues
                                    ):
                                        incrementa_de_forma_estable = True

                        if incrementa_de_forma_estable:
                            # Es un reset REAL - el totalizador vuelve a incrementar
                            reset_count += 1
                        else:
                            # El totalizador NO incrementa de forma estable → FALLO DE HARDWARE
                            is_anomalous_jump.iloc[pos] = True
                            anomaly_count += 1
                            logging.warning(
                                f"Salto anómalo detectado en {base} en {idx}: "
                                f"TOT cae de {tot_before:,.0f} a {tot_after:,.0f} pero NO incrementa de forma estable (fallo hardware)"
                            )
                    else:
                        # Es un SALTO ANÓMALO - marcar para corrección
                        is_anomalous_jump.iloc[pos] = True
                        anomaly_count += 1
                        logging.warning(
                            f"Salto anómalo detectado en {base} en {idx}: "
                            f"TOT después del salto = {tot_after:,.0f} (no es reset real)"
                        )

                combined[anomaly_col] = is_anomalous_jump

                if anomaly_count > 0:
                    logging.info(
                        f"{base}: Detectados {reset_count} resets reales y {anomaly_count} saltos anómalos"
                    )

                processed.add(col)
                processed.add(low_col)
                # Optionally drop the original H/L columns
                try:
                    combined.drop(columns=[col, low_col], inplace=True)
                except Exception:
                    pass
    return combined


df = combine_tot_high_low(df)


# Apply per10 multiplier BEFORE any other processing
def apply_per10_multiplier(df):
    """Apply x10 multiplier to totalizer columns for tags with per10=True."""
    try:
        engine = get_db_connection()
        
        tot_cols = [c for c in df.columns if c.endswith("_TOT")]
        
        if not tot_cols:
            logging.info("No totalizer columns found for per10 multiplier")
            return df
        
        result = df.copy()
        multiplied_tags = []
        
        for col in tot_cols:
            # Check if this tag has per10=True
            per10 = get_tag_per10(engine, col)
            
            if per10:
                result[col] = result[col] * 10
                multiplied_tags.append(col)
                logging.info(f"Applied per10 multiplier (x10) to {col}")
        
        if multiplied_tags:
            logging.info(f"per10 multiplier applied to {len(multiplied_tags)} tags")
        else:
            logging.info("No tags with per10=True found in dataset")
        
        return result
    except Exception as e:
        logging.warning(f"Could not apply per10 multiplier: {e}")
        return df


df = apply_per10_multiplier(df)


# Regla de calidad: rect_0 -> si el TOT calculado es 0, reemplazar por último valor válido (>0)
def apply_rect_0(df):
    # Remove any previous rect columns to avoid duplicates
    existing_rect_cols = [
        c for c in df.columns if c.endswith("_rect_0") or c == "rect_0"
    ]
    rected = df.copy()
    if existing_rect_cols:
        rected = rected.drop(columns=existing_rect_cols)

    tot_cols = [c for c in rected.columns if c.endswith("_TOT")]

    for col in tot_cols:
        rect_col = f"{col}_rect_0"
        s = pd.to_numeric(rected[col], errors="coerce")

        # previous and next values
        prev = s.shift(1)
        nxt = s.shift(-1)

        # invalid if exactly zero OR the new rule:
        # if current < previous AND (next < current OR next == 0), then current is invalid
        is_zero = s == 0
        rule = (s < prev) & ((nxt < s) | (nxt == 0))
        invalid = is_zero | rule

        # Mask invalid points to NaN then forward-fill using last valid
        s_masked = s.where(~invalid)
        s_filled = s_masked.ffill()
        s_filled = s_filled.fillna(0).astype("int64")
        rected[rect_col] = s_filled

    return rected


df = apply_rect_0(df)

# Calculate minute consumptions and append them
try:
    df = append_minute_consumption(df)
    logging.info("Appended per-minute consumption columns")
except Exception as e:
    logging.warning("Could not compute/append consumption columns: %s", e)

# Apply anomaly distribution rule and attach anomaly columns
try:
    # compute anomalies per total column explicitly here to ensure values are filled
    tot_candidates = [c for c in df.columns if c.endswith("_rect_0")]
    if not tot_candidates:
        tot_candidates = [c for c in df.columns if c.endswith("_TOT")]

    import numpy as _np

    for total_col in tot_candidates:
        cons_col = f"{total_col}_cons"
        anom_col = f"{total_col}_anom"
        if cons_col not in df.columns:
            df[anom_col] = _np.nan
            continue

        # prefer raw TOT to detect zero runs
        raw_col = (
            total_col.replace("_rect_0", "_TOT")
            if total_col.endswith("_rect_0")
            else total_col
        )
        if raw_col in df.columns:
            totals_raw = (
                pd.to_numeric(df[raw_col], errors="coerce").fillna(_np.nan).to_numpy()
            )
        else:
            totals_raw = (
                pd.to_numeric(df[total_col], errors="coerce").fillna(_np.nan).to_numpy()
            )

        cons = pd.to_numeric(df[cons_col], errors="coerce").fillna(0).to_numpy()
        n = len(df)
        anom = _np.zeros(n, dtype=float)
        i = 0
        matches = 0
        applied = 0
        while i < n - 1:
            cur = cons[i]
            nxt = cons[i + 1]
            if cur < 0 and nxt > 0:
                matches += 1
                net = cur + nxt
                if net > 0:
                    j = i
                    while j >= 0 and (totals_raw[j] == 0 or _np.isnan(totals_raw[j])):
                        j -= 1
                    start = j + 1
                    end = i
                    count = end - start + 1
                    logging.info(
                        "  i=%d cur=%s nxt=%s net=%s j=%d start=%d end=%d count=%d",
                        i,
                        cur,
                        nxt,
                        net,
                        j,
                        start,
                        end,
                        count,
                    )
                    if count > 0:
                        per = net / count
                        anom[start : end + 1] += per
                        applied += 1
                i += 2
            else:
                i += 1
        logging.info(
            "%s: found %d neg+pos patterns, applied %d distributions",
            total_col,
            matches,
            applied,
        )

        # replace zeros with NaN
        anom_series = pd.Series(anom, index=df.index, dtype="float64").replace(
            0.0, _np.nan
        )
        df[anom_col] = anom_series
    logging.info("Applied anomaly distribution to totalized columns (inline)")
except Exception as e:
    logging.warning("Could not apply anomaly distribution: %s", e)
else:
    # log counts of anomaly values for debugging
    try:
        for c in anom_df.columns:
            cnt = df[c].notna().sum()
            logging.info("Anomaly column %s non-null count: %d", c, cnt)
    except Exception:
        pass

# Guardar CSV con separador ';' y decimales ',' si está habilitado en config
save_task = next(
    (t for t in cfg.get("tasks", []) if t.get("name") == "save_to_csv"), None
)
if save_task and save_task.get("enabled"):
    out_dir = save_task.get("output_dir") or os.path.join(
        ROOT, "adquisicion", "minute_data"
    )
    os.makedirs(out_dir, exist_ok=True)
    filename = (
        save_task.get("filename")
        or f"all_minutes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    out_path = os.path.join(out_dir, filename)
    # Pandas accepts decimal=',' and sep=';'
    df.to_csv(out_path, sep=";", decimal=",", index=True)
    logging.info("Saved combined dataset to %s", out_path)
