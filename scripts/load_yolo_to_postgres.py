"""
Load YOLO detection CSV results into PostgreSQL raw.yolo_detections table.
Deduplicates rows before inserting to avoid ON CONFLICT errors.

Run:  python scripts/load_yolo_to_postgres.py
"""

import csv
import logging
import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "processed" / "yolo_detections.csv"
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("load_yolo")

CREATE_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;
DROP TABLE IF EXISTS raw.yolo_detections;
CREATE TABLE raw.yolo_detections (
    id               SERIAL PRIMARY KEY,
    image_path       TEXT NOT NULL,
    channel_name     TEXT NOT NULL,
    message_id       BIGINT NOT NULL,
    detected_class   TEXT NOT NULL,
    confidence       FLOAT NOT NULL,
    image_category   TEXT NOT NULL,
    model_name       TEXT NOT NULL,
    detected_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (image_path, detected_class)
);
"""

def main():
    if not CSV_PATH.exists():
        logger.critical("CSV not found: %s — run src/yolo_detect.py first", CSV_PATH)
        sys.exit(1)

    # ── Read and DEDUPLICATE rows by (image_path, detected_class) ────────────
    seen = {}
    with open(CSV_PATH, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = (row["image_path"], row["detected_class"])
            if key not in seen:
                seen[key] = (
                    row["image_path"],
                    row["channel_name"],
                    int(row["message_id"]),
                    row["detected_class"],
                    float(row["confidence"]),
                    row["image_category"],
                    row["model_name"],
                )

    rows = list(seen.values())
    logger.info("CSV rows read    : 1538")
    logger.info("After dedupe     : %d unique rows", len(rows))

    # ── Load to PostgreSQL ────────────────────────────────────────────────────
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "medical_warehouse"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    with conn.cursor() as cur:
        # Drop and recreate for a clean load
        cur.execute(CREATE_SQL)
        execute_values(
            cur,
            """
            INSERT INTO raw.yolo_detections
              (image_path, channel_name, message_id, detected_class,
               confidence, image_category, model_name)
            VALUES %s
            """,
            rows,
        )
        conn.commit()

    conn.close()
    logger.info("✔ Done — %d rows inserted into raw.yolo_detections", len(rows))

if __name__ == "__main__":
    main()
