"""
Quick script to check where data was inserted
"""

import json
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "CAT_Conexions", "src"))

from conexions import pgDataLake

# Load config
with open(os.path.join(ROOT, "consums_config.json"), "r") as f:
    cfg = json.load(f)

# Check localhost
print("=" * 60)
print("CHECKING LOCALHOST DATABASE")
print("=" * 60)
db_local = cfg["db"]
print(f"Host: {db_local['host']}")
print(f"Port: {db_local['port']}")
print(f"Database: {db_local['database']}")

db = pgDataLake(
    address=db_local["host"],
    port=db_local["port"],
    db=db_local["database"],
    user=db_local["user"],
    pwd=db_local["password"],
)
db.connect()

result = db.get_data(
    """
    SELECT COUNT(*) as count, MAX(data_insercio) as last_insertion
    FROM ga_datalake.ite_consums_data
    WHERE idtag = 21504
"""
)
print(f"\nRecords found: {result.iloc[0]['count']}")
print(f"Last insertion: {result.iloc[0]['last_insertion']}")

# Check if there's a cloud config
if "db_cloud" in cfg:
    print("\n" + "=" * 60)
    print("CHECKING CLOUD DATABASE")
    print("=" * 60)
    db_cloud = cfg["db_cloud"]
    print(f"Host: {db_cloud['host']}")
    print(f"Port: {db_cloud['port']}")
    print(f"Database: {db_cloud['database']}")

    db2 = pgDataLake(
        address=db_cloud["host"],
        port=db_cloud["port"],
        db=db_cloud["database"],
        user=db_cloud["user"],
        pwd=db_cloud["password"],
    )
    db2.connect()

    result2 = db2.get_data(
        """
        SELECT COUNT(*) as count, MAX(data_insercio) as last_insertion
        FROM ga_datalake.ite_consums_data
        WHERE idtag = 21504
    """
    )
    print(f"\nRecords found: {result2.iloc[0]['count']}")
    print(f"Last insertion: {result2.iloc[0]['last_insertion']}")
