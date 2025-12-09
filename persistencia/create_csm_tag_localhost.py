"""
Script to create CSM tag in localhost database based on TOT tag
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
print("Creating CSM tag in localhost database")
print("=" * 70)

with engine.connect() as conn:
    # Check if CSM tag already exists
    result = conn.execute(
        text(
            """
        SELECT "idTag", tag FROM ga_landing.cfg_tags 
        WHERE tag = 'PBD07_FTR_T01_CSM'
    """
        )
    )
    existing = result.fetchone()

    if existing:
        print(f"✓ CSM tag already exists with idTag={existing[0]}")
    else:
        # Get next available idTag
        result = conn.execute(text('SELECT MAX("idTag") FROM ga_landing.cfg_tags'))
        max_id = result.fetchone()[0]
        next_id = max_id + 1
        print(f"Next available idTag: {next_id}")

        # Get column names first
        result = conn.execute(
            text(
                """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'ga_landing' 
            AND table_name = 'cfg_tags'
            ORDER BY ordinal_position
        """
            )
        )
        columns = [row[0] for row in result.fetchall()]
        print(f"Available columns: {columns}")

        # Get TOT tag attributes
        result = conn.execute(
            text(
                """
            SELECT * FROM ga_landing.cfg_tags 
            WHERE tag = 'PBD07_FTR_T01_TOT'
        """
            )
        )
        tot_tag = result.fetchone()

        if not tot_tag:
            print("ERROR: TOT tag 'PBD07_FTR_T01_TOT' not found")
            sys.exit(1)

        print(f"Found TOT tag: {tot_tag.tag} (idTag={tot_tag.idTag})")
        print(f"TOT tag values: {dict(zip(columns, tot_tag))}")

        # Create CSM tag with same attributes as TOT
        insert_query = text(
            """
            INSERT INTO ga_landing.cfg_tags (
                "idTag", tag, "idTagTip", eng_unit, eng_zero, eng_full,
                "descTag", "tagOld", "dataAlta", "dataBaixa", "idInstTip", "idPare",
                "LimMinQ", "LimMaxQ", "UltAcces", per10, aporta, xarxa, fictici
            ) VALUES (
                :idTag, :tag, :idTagTip, :eng_unit, :eng_zero, :eng_full,
                :descTag, :tagOld, :dataAlta, :dataBaixa, :idInstTip, :idPare,
                :LimMinQ, :LimMaxQ, :UltAcces, :per10, :aporta, :xarxa, :fictici
            )
        """
        )

        conn.execute(
            insert_query,
            {
                "idTag": next_id,
                "tag": "PBD07_FTR_T01_CSM",
                "idTagTip": tot_tag.idTagTip,
                "eng_unit": tot_tag.eng_unit,
                "eng_zero": tot_tag.eng_zero,
                "eng_full": tot_tag.eng_full,
                "descTag": tot_tag.descTag,
                "tagOld": "CSM",
                "dataAlta": tot_tag.dataAlta,
                "dataBaixa": tot_tag.dataBaixa,
                "idInstTip": tot_tag.idInstTip,
                "idPare": tot_tag.idPare,
                "LimMinQ": tot_tag.LimMinQ,
                "LimMaxQ": tot_tag.LimMaxQ,
                "UltAcces": tot_tag.UltAcces,
                "per10": tot_tag.per10,
                "aporta": tot_tag.aporta,
                "xarxa": tot_tag.xarxa,
                "fictici": tot_tag.fictici,
            },
        )
        conn.commit()

        print(f"✓ CSM tag created successfully with idTag={next_id}")

        # Verify
        result = conn.execute(
            text(
                """
            SELECT "idTag", tag FROM ga_landing.cfg_tags 
            WHERE tag = 'PBD07_FTR_T01_CSM'
        """
            )
        )
        verify = result.fetchone()
        print(f"Verification: tag={verify[1]}, idTag={verify[0]}")

print("=" * 70)
print("Done!")
