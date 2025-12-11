import numpy as np
import pandas as pd


def detect_counter_resets(df: pd.DataFrame, total_columns=None, near_zero_threshold=1000) -> pd.DataFrame:
    """Detect counter resets in totalizer columns and mark corrections in anomaly columns.
    
    Solo trata como reset si el totalizador después del salto está cerca de 0.
    Si no está cerca de 0, es un salto anómalo y NO se corrige.

    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with totalizer columns
    total_columns : list, optional
        List of totalizer columns to check
    near_zero_threshold : int, default=1000
        Threshold to consider totalizer "near zero" after a reset

    Returns:
    --------
    pd.DataFrame
        Copy of df with reset corrections marked in anomaly columns
    """
    import logging
    
    if total_columns is None:
        # Prefer rectified totals if available, otherwise use raw *_TOT
        rect_cols = [c for c in df.columns if c.endswith("_rect_0")]
        if rect_cols:
            total_columns = rect_cols
        else:
            total_columns = [c for c in df.columns if c.endswith("_TOT")]

    result = df.copy()

    for col in total_columns:
        print(f"Checking for counter resets in column: {col}")
        totals = df[col].astype(float)

        # Calculate consumption differences to find potential resets
        consumption = totals.shift(-1) - totals

        # Look for very negative values that could indicate counter resets
        # Threshold: differences less than -1,000,000 (1 million) are likely resets
        reset_mask = consumption < -1000000
        reset_indices = reset_mask[reset_mask].index

        if len(reset_indices) > 0:
            print(f"  Found {len(reset_indices)} potential counter resets/jumps")

            anom_col = f"{col}_anom"
            anomalous_jump_col = col + "_is_anomalous_jump"
            
            if anom_col in result.columns:
                real_resets = 0
                anomalous_jumps = 0
                
                for reset_idx in reset_indices:
                    # Get position in array
                    reset_pos = totals.index.get_loc(reset_idx)
                    if reset_pos < len(totals) - 1:
                        prev_value = totals.iloc[reset_pos]
                        curr_value = totals.iloc[reset_pos + 1]

                        # Validar si es reset REAL (totalizador después cerca de 0)
                        is_real_reset = abs(curr_value) <= near_zero_threshold
                        
                        if is_real_reset:
                            # Es un RESET REAL
                            print(
                                f"  ✓ Reset REAL detectado en {reset_idx} (pos {reset_pos}): {prev_value} → {curr_value}"
                            )

                            # Estimate counter maximum (usually power of 10: 10^7, 10^8, 10^9)
                            counter_max = determine_counter_max(prev_value)

                            # Calculate actual consumption during reset
                            actual_consumption = (counter_max - prev_value) + curr_value
                            print(f"    Estimated counter maximum: {counter_max}")
                            print(
                                f"    Actual consumption during reset: {actual_consumption}"
                            )

                            # Mark the correction in the anomaly column at the reset minute
                            result.loc[reset_idx, anom_col] = actual_consumption
                            print(f"    Marked correction in {anom_col} at {reset_idx}")
                            real_resets += 1
                        else:
                            # Es un SALTO ANÓMALO - NO corregir
                            # Verificar si ya está marcado desde combine_tot_high_low
                            if anomalous_jump_col in result.columns:
                                is_marked = result.loc[result.index[reset_pos + 1], anomalous_jump_col]
                                if is_marked:
                                    print(
                                        f"  ⚠️  Salto anómalo YA MARCADO en {reset_idx}: {prev_value} → {curr_value} (no es reset)"
                                    )
                                else:
                                    logging.warning(
                                        f"Salto anómalo NO marcado previamente en {reset_idx}: {prev_value} → {curr_value}"
                                    )
                            else:
                                print(
                                    f"  ⚠️  Salto anómalo detectado en {reset_idx}: {prev_value} → {curr_value} (no es reset, NO se corrige)"
                                )
                            anomalous_jumps += 1
                
                print(f"  Resumen: {real_resets} resets reales, {anomalous_jumps} saltos anómalos (no corregidos)")

    return result


def determine_counter_max(value):
    """Determine the maximum value of a counter based on the current value.

    Industrial counters typically reset at powers of 10: 10^7, 10^8, 10^9, etc.
    """
    import math

    if value <= 0:
        return 10000000  # Default to 10 million

    # Find the next power of 10 above the current value
    log_value = math.log10(value)
    next_power = math.ceil(log_value)
    counter_max = 10**next_power

    return int(counter_max)


def compute_minute_consumption(df: pd.DataFrame, total_columns=None) -> pd.DataFrame:
    """Compute minute consumption for totalized columns.

    For each column in `total_columns` (or detected columns ending with '_TOT'),
    compute consumption as next_minute_total - current_minute_total.

    Returns a new DataFrame with consumption columns named `<col>_cons`.
    The returned DataFrame is aligned with the original index (minute timestamps);
    the last row will contain NaN for consumption because there's no next minute.
    """
    if total_columns is None:
        # Prefer rectified totals if available, otherwise use raw *_TOT
        rect_cols = [c for c in df.columns if c.endswith("_rect_0")]
        if rect_cols:
            total_columns = rect_cols
        else:
            total_columns = [c for c in df.columns if c.endswith("_TOT")]

    out = pd.DataFrame(index=df.index)

    for col in total_columns:
        # compute difference with next row: shift(-1)
        out[f"{col}_cons"] = df[col].shift(-1) - df[col]

    return out


def append_minute_consumption(df: pd.DataFrame, total_columns=None) -> pd.DataFrame:
    """Return a copy of df with consumption columns appended.

    Keeps original columns and appends `<col>_cons` for each total column.
    Returns the dataframe ready for reset detection (without applying it yet).
    
    Si existe columna *_is_anomalous_jump, corrige el consumo a 0 en esos puntos.
    """
    import logging
    
    # Compute normal consumption
    cons = compute_minute_consumption(df, total_columns=total_columns)

    # Join with original dataframe
    result = df.copy()
    for c in cons.columns:
        result[c] = cons[c]
    
    # Aplicar correcciones de saltos anómalos si existen
    if total_columns is None:
        rect_cols = [c for c in df.columns if c.endswith("_rect_0")]
        if rect_cols:
            total_columns = rect_cols
        else:
            total_columns = [c for c in df.columns if c.endswith("_TOT")]
    
    for col in total_columns:
        cons_col = f"{col}_cons"
        anomaly_col = col + "_is_anomalous_jump"
        
        if anomaly_col in result.columns and cons_col in result.columns:
            # Corregir consumo a 0 donde hay saltos anómalos
            anomaly_mask = result[anomaly_col] == True
            num_anomalies = anomaly_mask.sum()
            
            if num_anomalies > 0:
                consumption_original = result[cons_col].copy()
                result.loc[anomaly_mask, cons_col] = 0.0
                
                total_eliminated = consumption_original.loc[anomaly_mask].sum()
                logging.info(
                    f"{col}: Corregidos {num_anomalies} saltos anómalos en consumo, "
                    f"eliminado: {total_eliminated:,.0f}"
                )

    return result


def distribute_negative_compensations(
    df: pd.DataFrame, total_columns=None
) -> pd.DataFrame:
    """Detect negative consumption followed by compensating positive and distribute
    the net positive across previous minutes where the total column was zero.

    Returns a DataFrame with new anomaly columns named `<total_column>_anom`.
    The function does NOT modify the input df.
    """
    import numpy as np

    if total_columns is None:
        rect_cols = [c for c in df.columns if c.endswith("_rect_0")]
        if rect_cols:
            total_columns = rect_cols
        else:
            total_columns = [c for c in df.columns if c.endswith("_TOT")]

    anom_df = pd.DataFrame(index=df.index)

    for total_col in total_columns:
        cons_col = f"{total_col}_cons"
        anom_col = f"{total_col}_anom"

        if cons_col not in df.columns:
            anom_df[anom_col] = np.nan
            continue

        cons = df[cons_col].astype(float).reset_index(drop=True)
        # prefer to inspect the raw *_TOT column to find zero runs
        if total_col.endswith("_rect_0"):
            raw_col = total_col.replace("_rect_0", "_TOT")
        else:
            raw_col = total_col

        if raw_col in df.columns:
            totals_raw = df[raw_col].astype(float).reset_index(drop=True)
        else:
            totals_raw = df[total_col].astype(float).reset_index(drop=True)
        n = len(df)
        anom = np.zeros(n, dtype=float)

        i = 0
        while i < n - 1:
            cur = cons.iat[i]
            nxt = cons.iat[i + 1]
            # negative followed by positive
            if cur < 0 and nxt > 0:
                net = cur + nxt  # cur is negative
                if net > 0:
                    # walk backwards from i while totals == 0
                    j = i
                    while j >= 0 and (
                        totals_raw.iat[j] == 0 or np.isnan(totals_raw.iat[j])
                    ):
                        j -= 1
                    start = j + 1
                    end = i
                    count = end - start + 1
                    if count > 0:
                        per = net / count
                        anom[start : end + 1] += per
                i += 2
            else:
                i += 1

        anom_series = pd.Series(anom, index=df.index, dtype="float64")
        # set NaN where anom is zero to keep CSV cleaner
        anom_series = anom_series.replace(0.0, np.nan)
        anom_df[anom_col] = anom_series

    return anom_df


def detect_phantom_totalizer_jumps(df: pd.DataFrame, total_columns=None, threshold=1000000) -> pd.DataFrame:
    """Detect and mark phantom jumps in totalizer as anomalies (consumption = 0).
    
    Phantom jumps are large changes in the totalizer that don't represent real consumption:
    - Large change in totalizer (> threshold, default 1 million)
    - Consumption calculated is anomalously high
    - Often the totalizer returns to a similar value shortly after
    
    These are marked with consumption = 0 in the anomaly column.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with totalizer and consumption columns
    total_columns : list, optional
        Columns to check for phantom jumps
    threshold : int, default=1000000
        Minimum consumption value to consider as potential phantom jump (1 million)
        
    Returns:
    --------
    pd.DataFrame
        DataFrame with phantom jumps marked in _anom columns
    """
    if total_columns is None:
        rect_cols = [c for c in df.columns if c.endswith("_rect_0")]
        if rect_cols:
            total_columns = rect_cols
        else:
            total_columns = [c for c in df.columns if c.endswith("_TOT")]
    
    result = df.copy()
    
    for total_col in total_columns:
        cons_col = f"{total_col}_cons"
        anom_col = f"{total_col}_anom"
        
        if cons_col not in df.columns:
            continue
            
        print(f"Checking for phantom jumps in {total_col}...")
        
        # Find consumptions above threshold
        high_cons_mask = df[cons_col] > threshold
        high_cons_indices = df[high_cons_mask].index
        
        phantom_count = 0
        
        for idx in high_cons_indices:
            pos = df.index.get_loc(idx)
            
            # Need at least 10 minutes before and after for reliable detection
            if pos < 10 or pos >= len(df) - 10:
                continue
            
            # Get totalizer values in window
            tot_before = df[total_col].iloc[pos - 10:pos].values
            tot_current = df[total_col].iloc[pos]
            tot_after = df[total_col].iloc[pos + 1:pos + 11].values
            
            # Check if totalizer was stable before (all same value or very close)
            if len(tot_before) > 0 and len(tot_after) > 0:
                # Check stability: all values within 10 units
                before_stable = (tot_before.max() - tot_before.min()) < 10
                after_stable = (tot_after.max() - tot_after.min()) < 10

                if before_stable and after_stable:
                    avg_before = tot_before.mean()
                    avg_after = tot_after.mean()
                    delta = abs(avg_before - avg_after)
                    future_cons = df[cons_col].iloc[pos + 1 : pos + 11].abs().max()

                    reverts_to_previous = delta < abs(0.01 * max(avg_before, 1))
                    large_step_flat_future = delta > threshold and (future_cons < 10)

                    if reverts_to_previous or large_step_flat_future:
                        # This is a phantom jump - mark consumption as 0
                        if anom_col not in result.columns:
                            result[anom_col] = np.nan

                        result.loc[idx, anom_col] = 0.0
                        phantom_count += 1
                        reason = (
                            "revert" if reverts_to_previous else "flat-after jump"
                        )
                        print(
                            f"  Phantom jump detected ({reason}) at {idx}: {df[cons_col].loc[idx]:.0f} → 0"
                        )
        
        if phantom_count > 0:
            print(f"  Total phantom jumps corrected: {phantom_count}")
    
    return result


def attach_anomalies_to_df(df: pd.DataFrame, total_columns=None) -> pd.DataFrame:
    """Compute anomaly columns and attach them to df (in-place-like, returns df).

    This function expects `df` to already contain `<col>_cons` columns for totals.
    It preserves the original negative values in _cons and creates separate _anom
    columns with the distributed corrections.
    """
    import numpy as np

    if total_columns is None:
        rect_cols = [c for c in df.columns if c.endswith("_rect_0")]
        if rect_cols:
            total_columns = rect_cols
        else:
            total_columns = [c for c in df.columns if c.endswith("_TOT")]

    for total_col in total_columns:
        cons_col = f"{total_col}_cons"
        anom_col = f"{total_col}_anom"

        if cons_col not in df.columns:
            df[anom_col] = np.nan
            continue

        # prefer raw TOT to detect zero runs
        raw_col = (
            total_col.replace("_rect_0", "")
            if total_col.endswith("_rect_0")
            else total_col
        )
        if raw_col in df.columns:
            totals_raw = (
                pd.to_numeric(df[raw_col], errors="coerce").fillna(np.nan).to_numpy()
            )
        else:
            totals_raw = (
                pd.to_numeric(df[total_col], errors="coerce").fillna(np.nan).to_numpy()
            )

        cons = pd.to_numeric(df[cons_col], errors="coerce").fillna(0).to_numpy()
        n = len(df)
        anom = np.zeros(n, dtype=float)
        i = 0
        while i < n - 1:
            cur = cons[i]
            nxt = cons[i + 1]
            if cur < 0 and nxt > 0:
                net = cur + nxt
                if net > 0:
                    # Buscar hacia atrás los minutos consecutivos donde totals_raw == 0
                    j = i - 1  # Empezar desde el minuto anterior al negativo
                    while j >= 0 and totals_raw[j] == 0:
                        j -= 1
                    start = j + 1
                    end = i + 1  # Incluir hasta el minuto DESPUÉS del consumo negativo
                    count = end - start + 1

                    if (
                        count > 2
                    ):  # Hay minutos con total=0 además de los dos problemáticos
                        # Distribuir entre TODOS los minutos del rango (incluye minutos con total=0 + los dos problemáticos)
                        per = net / count
                        for k in range(start, end + 1):
                            anom[k] += per
                    else:
                        # No hay minutos con total=0, distribuir solo entre los dos consumos problemáticos
                        per = net / 2
                        anom[i] += per  # Minuto del consumo negativo
                        anom[i + 1] += per  # Minuto del consumo positivo
                i += 2
            else:
                i += 1

        anom_series = pd.Series(anom, index=df.index, dtype="float64").replace(
            0.0, np.nan
        )
        df[anom_col] = anom_series

    return df
