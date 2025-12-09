import os
import sys
import json
from sqlalchemy import text

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "CAT_Conexions", "src"))

from persistencia.db_connection import get_db_connection


cfg = json.load(open("consums_config.json"))
engine = get_db_connection(cfg)
conn = engine.connect()


def q(stmt, params=None):
    return conn.execute(text(stmt), params or {}).fetchall()


print("Searching v_ite_consums_24h for values > 10000 on 2025-06-02...")
rows = q(
    """
SELECT data, idtag, valor, descrip_correccio
FROM ga_datalake.v_ite_consums_24h
WHERE data >= '2025-06-02'::timestamptz AND data < '2025-06-03'::timestamptz
  AND valor > 10000
ORDER BY valor DESC
LIMIT 50
"""
)
for r in rows:
    print(r)

if not rows:
    print("No large daily values found in v_ite_consums_24h for that date.")
else:
    idtags = sorted(set(r[1] for r in rows))
    for idt in idtags:
        print("\n--- Details for idtag=", idt, "---")
        tag = q('SELECT tag FROM ga_landing.cfg_tags WHERE "idTag" = :id', {"id": idt})
        print("Tag name:", tag[0][0] if tag else "N/A")
        print("\nHourly rows in ite_consums_data for that idtag and date:")
        hr = q(
            """
SELECT data, valor, data_insercio
FROM ga_datalake.ite_consums_data
WHERE idtag = :id AND data >= '2025-06-02'::timestamptz AND data < '2025-06-03'::timestamptz
ORDER BY data
""",
            {"id": idt},
        )
        for h in hr:
            print(h)

conn.close()
print("\nDone.")
