"""
Script to create the v_ite_consums_data view in PostgreSQL
"""

import json
import os
import sys
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Load config
with open(os.path.join(ROOT, "consums_config.json"), "r") as f:
    cfg = json.load(f)

db_config = cfg["db"]

# Connect to database
encoded_password = quote_plus(db_config["password"])
connection_string = f"postgresql://{db_config['user']}:{encoded_password}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(connection_string)

print("=" * 70)
print("Creating view v_ite_consums_data in localhost database")
print("=" * 70)

# Read SQL file
sql_file = os.path.join(os.path.dirname(__file__), "create_view_consums_data.sql")
with open(sql_file, "r", encoding="utf-8") as f:
    sql_content = f.read()

# Execute SQL
with engine.connect() as conn:
    # Remove comments for execution
    statements = [
        s.strip()
        for s in sql_content.split(";")
        if s.strip() and not s.strip().startswith("--")
    ]

    for statement in statements:
        if statement:
            conn.execute(text(statement))
            conn.commit()  # Commit each statement separately

    print("✓ View v_ite_consums_data created successfully")

    # Test the view
    result = conn.execute(
        text(
            """
        SELECT COUNT(*) as total_records,
               SUM(CASE WHEN te_correccio THEN 1 ELSE 0 END) as records_with_correction
        FROM ga_datalake.v_ite_consums_data
    """
        )
    )
    row = result.fetchone()
    print(f"\nView statistics:")
    print(f"  Total records: {row[0]}")
    print(f"  Records with corrections: {row[1]}")
    print(f"  Records without corrections: {row[0] - row[1]}")

print("=" * 70)
print("Done!")
