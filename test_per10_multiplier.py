"""Test per10 multiplier with a small CSV of per10 tags."""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, "persistencia"))
sys.path.insert(0, os.path.join(ROOT, "procesado"))

from db_connection import get_db_connection, get_tag_per10

# Test get_tag_per10 function
engine = get_db_connection()

test_tags = [
    "ATT03_FTR_T01_D_TOT",  # Should be True
    "BPD04_FTR_G01_TOT",    # Should be False
    "PAB01_FTR_I02_TOT",    # Should be True
]

print("Testing get_tag_per10 function:")
print("=" * 50)
for tag in test_tags:
    per10 = get_tag_per10(engine, tag)
    print(f"  {tag}: per10={per10}")

# Create a small test CSV with one per10=True and one per10=False
print("\nCreating test CSV with mixed per10 tags...")
print("=" * 50)

# Create datetime index
start_date = datetime(2025, 1, 1, 0, 0, 0)
dates = [start_date + timedelta(minutes=i) for i in range(10)]

data = {
    "ATT03_FTR_T01_D_TOT": [1000 + i*10 for i in range(10)],  # per10=True, should be multiplied
    "BPD04_FTR_G01_TOT": [5000 + i*20 for i in range(10)],    # per10=False, should stay same
}

df = pd.DataFrame(data, index=dates)
df.index.name = "timeStamp"

test_csv = os.path.join(ROOT, "test_per10_input.csv")
df.to_csv(test_csv, sep=";", decimal=",")
print(f"Created test CSV: {test_csv}")
print("\nOriginal data:")
print(df.head())

# Now apply the per10 multiplier using the function from run_compute_consumption.py
from run_compute_consumption import apply_per10_multiplier

print("\nApplying per10 multiplier...")
result = apply_per10_multiplier(df)

print("\nResult after per10 multiplier:")
print(result.head())

print("\nComparison:")
print("=" * 50)
print("ATT03_FTR_T01_D_TOT (per10=True):")
print(f"  Original: {df['ATT03_FTR_T01_D_TOT'].iloc[0]}")
print(f"  After:    {result['ATT03_FTR_T01_D_TOT'].iloc[0]}")
print(f"  Expected: {df['ATT03_FTR_T01_D_TOT'].iloc[0] * 10}")
print(f"  Multiplied: {result['ATT03_FTR_T01_D_TOT'].iloc[0] == df['ATT03_FTR_T01_D_TOT'].iloc[0] * 10}")

print("\nBPD04_FTR_G01_TOT (per10=False):")
print(f"  Original: {df['BPD04_FTR_G01_TOT'].iloc[0]}")
print(f"  After:    {result['BPD04_FTR_G01_TOT'].iloc[0]}")
print(f"  Should be same: {result['BPD04_FTR_G01_TOT'].iloc[0] == df['BPD04_FTR_G01_TOT'].iloc[0]}")
