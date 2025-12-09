"""
Database connection module for PostgreSQL persistence.
Uses direct psycopg connection for explicit control.
"""

import json
import logging
import os
import sys
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def get_db_connection(cfg=None):
    """
    Create and return a SQLAlchemy engine using configuration.

    Args:
        cfg (dict, optional): Configuration dictionary with 'db' section.
                             If None, loads from consums_config.json

    Returns:
        sqlalchemy.engine.Engine: Connected database engine

    Raises:
        Exception: If connection fails
    """
    if cfg is None:
        CONFIG_PATH = os.path.join(ROOT, "consums_config.json")
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)

    db_config = cfg.get("db", {})

    host = db_config.get("host")
    port = db_config.get("port")
    database = db_config.get("database")
    user = db_config.get("user")
    password = db_config.get("password")

    logging.info(f"Connecting to PostgreSQL: {host}:{port}/{database}")
    logging.info(f"User: {user}")

    try:
        # URL encode password to handle special characters
        encoded_password = quote_plus(password)

        # Create connection string
        connection_string = (
            f"postgresql://{user}:{encoded_password}@{host}:{port}/{database}"
        )
        engine = create_engine(connection_string)

        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()

        logging.info("Database connection established successfully")
        return engine
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        raise


def get_tag_id(engine, tag_name):
    """
    Query ga_landing.cfg_tags to get the idtag for a given tag name.

    Args:
        engine (sqlalchemy.engine.Engine): Database engine
        tag_name (str): Tag name (e.g., 'PBD07_FTR_T01_CSM')

    Returns:
        int: idtag if found

    Raises:
        ValueError: If tag not found in cfg_tags
    """
    query = text(
        """
        SELECT "idTag" 
        FROM ga_landing.cfg_tags 
        WHERE tag = :tag_name
    """
    )

    try:
        with engine.connect() as conn:
            result = conn.execute(query, {"tag_name": tag_name})
            row = result.fetchone()

            if row is None:
                raise ValueError(f"Tag '{tag_name}' not found in ga_landing.cfg_tags")

            idtag = int(row[0])
            logging.info(f"Found idtag={idtag} for tag '{tag_name}'")
            return idtag

    except ValueError:
        raise
    except Exception as e:
        logging.error(f"Error querying tag ID for '{tag_name}': {e}")
        raise
