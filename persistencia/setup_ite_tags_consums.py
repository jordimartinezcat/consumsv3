"""Utility to create and populate ga_landing.ite_tags_consums from cfg_tags."""

from __future__ import annotations

import logging
import os
import sys

from sqlalchemy import text

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from persistencia.db_connection import get_db_connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


CREATE_TABLE_SQL = text(
    """
    CREATE TABLE IF NOT EXISTS ga_landing.ite_tags_consums
    (LIKE ga_landing.cfg_tags INCLUDING ALL)
    """
)

TRUNCATE_SQL = text("TRUNCATE ga_landing.ite_tags_consums")

INSERT_SQL = text(
    """
    INSERT INTO ga_landing.ite_tags_consums (
        "idTag", "tag", "idTagTip", eng_unit, eng_zero, eng_full,
        "descTag", "tagOld", "dataAlta", "dataBaixa", "idInstTip",
        "idPare", "LimMinQ", "LimMaxQ", "UltAcces", per10, aporta,
        xarxa, fictici
    )
    SELECT
        "idTag",
        regexp_replace("tag", '_TOT$', '_CSM') AS "tag",
        "idTagTip", eng_unit, eng_zero, eng_full,
        "descTag", "tagOld", "dataAlta", "dataBaixa", "idInstTip",
        "idPare", "LimMinQ", "LimMaxQ", "UltAcces", per10, aporta,
        xarxa, fictici
    FROM ga_landing.cfg_tags
    WHERE "tag" ~ '_TOT$'
    """
)


def ensure_table(engine) -> None:
    with engine.connect() as conn:
        conn.execute(CREATE_TABLE_SQL)
        conn.commit()
    logging.info("Ensured ga_landing.ite_tags_consums exists")


def repopulate_table(engine) -> int:
    with engine.begin() as conn:
        conn.execute(TRUNCATE_SQL)
        result = conn.execute(INSERT_SQL)
    inserted = result.rowcount if result.rowcount is not None else 0
    logging.info("Inserted %s CSM tags", inserted)
    return inserted


def main() -> int:
    engine = get_db_connection()
    ensure_table(engine)
    inserted = repopulate_table(engine)
    logging.info("Setup completed (%d rows)", inserted)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
