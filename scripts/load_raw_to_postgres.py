"""
Load Raw Data to PostgreSQL
Reads JSON files from the data lake and loads them into raw.telegram_messages table.
Run this BEFORE dbt: python scripts/load_raw_to_postgres.py
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
MESSAGES_DIR = BASE_DIR / "data" / "raw" / "telegram_messages"
LOGS_DIR = BASE_DIR / "logs"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            LOGS_DIR / f"load_raw_{datetime.now():%Y%m%d_%H%M%S}.log",
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("load_raw")

# ─────────────────────────────────────────────
# DDL
# ─────────────────────────────────────────────
CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS raw;"

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    id                  SERIAL PRIMARY KEY,
    message_id          BIGINT,
    channel_name        TEXT,
    message_date        TIMESTAMPTZ,
    message_text        TEXT,
    views               INTEGER,
    forwards            INTEGER,
    reply_to_msg_id     BIGINT,
    from_id             TEXT,
    has_photo           BOOLEAN,
    media_type          TEXT,
    image_local_path    TEXT,
    raw_channel         TEXT,
    scraped_at          TIMESTAMPTZ,
    loaded_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (message_id, channel_name)
);
"""

# ─────────────────────────────────────────────
# DB connection
# ─────────────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "medical_warehouse"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

# ─────────────────────────────────────────────
# Load logic
# ─────────────────────────────────────────────
def parse_record(record: dict, channel_name: str) -> tuple:
    """Extract and coerce fields from a raw JSON record."""
    media = record.get("media") or {}
    return (
        record.get("message_id"),
        channel_name,
        record.get("date"),
        record.get("text") or "",
        record.get("views"),
        record.get("forwards"),
        record.get("reply_to_msg_id"),
        record.get("from_id"),
        bool(media.get("has_photo", False)),
        media.get("type"),
        media.get("local_path"),
        record.get("_raw_channel"),
        record.get("_scraped_at"),
    )


def load_json_files(conn) -> dict:
    """Walk the data lake and load all JSON files into PostgreSQL."""
    stats = {"files": 0, "inserted": 0, "skipped": 0, "errors": 0}

    if not MESSAGES_DIR.exists():
        logger.error("Data lake directory not found: %s", MESSAGES_DIR)
        logger.error("Run src/scraper.py first to populate the data lake.")
        sys.exit(1)

    json_files = sorted(MESSAGES_DIR.rglob("*.json"))
    if not json_files:
        logger.warning("No JSON files found in %s", MESSAGES_DIR)
        return stats

    logger.info("Found %d JSON files to process", len(json_files))

    with conn.cursor() as cur:
        for json_file in json_files:
            channel_name = json_file.stem  # filename without .json = channel name
            try:
                records = json.loads(json_file.read_text(encoding="utf-8"))
                if not records:
                    continue

                rows = [parse_record(r, channel_name) for r in records]

                execute_values(
                    cur,
                    """
                    INSERT INTO raw.telegram_messages (
                        message_id, channel_name, message_date, message_text,
                        views, forwards, reply_to_msg_id, from_id,
                        has_photo, media_type, image_local_path,
                        raw_channel, scraped_at
                    ) VALUES %s
                    ON CONFLICT (message_id, channel_name) DO NOTHING
                    """,
                    rows,
                )
                inserted = cur.rowcount
                skipped = len(rows) - inserted
                stats["files"] += 1
                stats["inserted"] += inserted
                stats["skipped"] += skipped
                logger.info(
                    "  %-55s → inserted=%-4d  skipped=%d",
                    str(json_file.relative_to(BASE_DIR)),
                    inserted,
                    skipped,
                )
            except Exception as exc:
                stats["errors"] += 1
                logger.error("Failed to load %s: %s", json_file, exc)
                conn.rollback()
                continue

        conn.commit()

    return stats


def main():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Connecting to PostgreSQL…")

    try:
        conn = get_connection()
    except Exception as exc:
        logger.critical("Cannot connect to PostgreSQL: %s", exc)
        logger.critical("Make sure Docker is running: docker compose up postgres -d")
        sys.exit(1)

    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_SCHEMA)
            cur.execute(CREATE_TABLE)
            conn.commit()
            logger.info("Schema and table ready: raw.telegram_messages")

        stats = load_json_files(conn)

        logger.info("═" * 55)
        logger.info("LOAD COMPLETE")
        logger.info("  Files processed : %d", stats["files"])
        logger.info("  Rows inserted   : %d", stats["inserted"])
        logger.info("  Rows skipped    : %d (already exist)", stats["skipped"])
        logger.info("  Errors          : %d", stats["errors"])
        logger.info("═" * 55)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
