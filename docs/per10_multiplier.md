# per10 Multiplier Feature

## Overview

The `per10` feature allows automatic multiplication of totalizer values by 10 for specific signals. This is necessary for certain sensors that report values at 1/10th scale.

## How It Works

### 1. Database Configuration

Tags that need 10x multiplication have `per10=TRUE` in the `ga_landing.cfg_tags` table.

Currently, 18 tags have this flag enabled:
- ATT03_FTR_T01_D_TOT
- ATT03_FTR_T01_I_TOT
- CP010_FTR_C01_TOT
- CP020_FTR_C02_TOT
- CPB00_FTR_I01_TOT
- CRD03_FTR_G01_TOT
- LTB20_FTR_I01_TOT
- PAB01_FTR_I02_TOT
- PBB02_FTR_D01_D_TOT
- PBB02_FTR_D01_I_TOT
- PBB02_FTR_E01_D_TOT
- PBB02_FTR_E01_I_TOT
- PBB02_FTR_I01_D_TOT
- PBB02_FTR_T11_TOT
- SCB03_FTR_I01_TOT
- SCB03_FTR_Q01_IN_TOT
- SCB03_FTR_Q01_OUT_TOT
- SCD04_FTR_G01_TOT

### 2. Implementation Flow

```
1. Load individual signal CSVs → run_compute_for_minutes.py
2. Combine H/L into 32-bit totalizers → combine_tot_high_low()
3. Query per10 flag for each TOT column → get_tag_per10()
4. Multiply totalizer values by 10 if per10=True → apply_per10_multiplier()
5. Apply data quality rules → apply_rect_0()
6. Calculate consumption (automatic, uses multiplied values) → append_minute_consumption()
7. Detect anomalies/resets (automatic, uses multiplied values)
8. Persist to database (automatic, uses corrected consumption)
```

### 3. Key Design Decision

**Multiplier is applied to totalizer values, not consumption values.**

This approach is cleaner because:
- ✅ Single multiplication point (at data loading)
- ✅ All downstream logic works automatically
- ✅ Reset detection uses correct scale
- ✅ Anomaly detection uses correct scale
- ✅ Consumption calculation inherits correct scale

Alternative (multiply consumption after calculation) would require:
- ❌ Multiple multiplication points
- ❌ Manual adjustment of anomaly corrections
- ❌ Manual adjustment of reset compensations
- ❌ More complex code maintenance

### 4. Code Locations

#### `persistencia/db_connection.py`
```python
def get_tag_per10(engine, tag_name):
    """Return per10 flag for a tag from cfg_tags. Returns False if not found."""
    # Queries: SELECT per10 FROM ga_landing.cfg_tags WHERE tag = :tag_name
```

#### `adquisicion/run_compute_for_minutes.py`
```python
def apply_per10_multiplier(df):
    """Apply x10 multiplier to totalizer columns for tags with per10=True."""
    # 1. Get engine
    # 2. Find all _TOT columns (after combine_tot_high_low)
    # 3. Query per10 for each tag
    # 4. Multiply by 10 if per10=True
    # 5. Return modified DataFrame
```

Called right after combining H/L into 32-bit totals, before any other processing:
```python
df = combine_tot_high_low(df)      # Combine 16-bit H/L → 32-bit TOT
df = apply_per10_multiplier(df)    # ← Applied here (NEW LOCATION)
df = apply_rect_0(df)              # Data quality rules
df = append_minute_consumption(df) # Calculate consumption
```

**Note:** Previously this was in `procesado/run_compute_consumption.py`, but it has been moved to `adquisicion/run_compute_for_minutes.py` to ensure the multiplier is applied before consumption calculation, not after.

## Testing

Run `test_per10_multiplier.py` to verify functionality:
```bash
python test_per10_multiplier.py
```

Expected output:
- Tags with per10=True are multiplied by 10
- Tags with per10=False remain unchanged
- Database queries work correctly

## Database Schema

The `per10` field is a BOOLEAN in `ga_landing.cfg_tags`:
```sql
SELECT tag, per10 
FROM ga_landing.cfg_tags 
WHERE tag LIKE '%_TOT'
AND per10 = TRUE;
```

## Future Considerations

If additional multipliers are needed (per100, per5, etc.), the same pattern can be extended:
1. Add field to cfg_tags table
2. Add query function to db_connection.py
3. Add multiplier function to run_compute_consumption.py
4. Apply before consumption calculation

## Performance

- Database query per tag (cached by SQLAlchemy connection pool)
- Multiplication is vectorized (pandas DataFrame operation)
- Negligible performance impact for typical datasets
- For 250 tags: ~250 DB queries (one-time per run)
- Multiplication: O(n) where n = number of rows

## Error Handling

- If tag not found in cfg_tags → assumes per10=False (logs warning)
- If per10 is NULL → treats as False
- Database connection errors → logs warning, assumes False
- Never fails the pipeline, always has safe fallback
