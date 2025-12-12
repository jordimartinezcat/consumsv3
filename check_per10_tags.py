"""Check which tags have per10=True in cfg_tags table."""
import os
import sys

ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(ROOT, "persistencia"))

from db_connection import get_db_connection
from sqlalchemy import text

engine = get_db_connection()

query = text("""
    SELECT tag, per10
    FROM ga_landing.cfg_tags
    WHERE tag LIKE '%_TOT'
    AND per10 = TRUE
    ORDER BY tag
""")

print("Tags with per10=True:")
print("=" * 50)

with engine.connect() as conn:
    result = conn.execute(query)
    rows = result.fetchall()
    
    if not rows:
        print("No tags found with per10=True")
    else:
        for row in rows:
            print(f"  {row[0]}: per10={row[1]}")
        print(f"\nTotal: {len(rows)} tags")

print("\nAll tags per10 status:")
print("=" * 50)

query_all = text("""
    SELECT tag, per10
    FROM ga_landing.cfg_tags
    WHERE tag LIKE '%_TOT'
    ORDER BY tag
""")

with engine.connect() as conn:
    result = conn.execute(query_all)
    rows = result.fetchall()
    
    for row in rows:
        per10_status = "TRUE" if row[1] else "FALSE"
        print(f"  {row[0]}: per10={per10_status}")
    
    print(f"\nTotal: {len(rows)} TOT tags")
