"""
YOLO Object Detection Pipeline — Task 3
Scans images downloaded in Task 1, runs YOLOv8 detection,
classifies images, and saves results to CSV + PostgreSQL.

Run:  python src/yolo_detect.py
"""

import csv
import logging
import os
import sys
import time
from pathlib import Path
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
load_dotenv()

BASE_DIR   = Path(__file__).resolve().parent.parent
IMAGES_DIR = BASE_DIR / "data" / "raw" / "images"
OUTPUT_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR   = BASE_DIR / "logs"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

CSV_OUTPUT = OUTPUT_DIR / "yolo_detections.csv"
MODEL_NAME = "yolov8n.pt"          # nano model — fast on standard laptops
CONF_THRESHOLD = 0.25              # minimum confidence to record a detection

# ── COCO class groups used for image classification ──────────────────────────
# These are standard COCO class names that YOLOv8n is trained on
PERSON_CLASSES   = {"person"}
PRODUCT_CLASSES  = {
    "bottle", "cup", "bowl", "vase", "book", "scissors",
    "toothbrush", "hair drier", "handbag", "backpack",
    "suitcase", "umbrella", "tie", "laptop", "cell phone",
    "remote", "keyboard", "mouse", "clock", "potted plant",
}

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
def setup_logging() -> logging.Logger:
    log_file = LOGS_DIR / f"yolo_detect_{datetime.now():%Y%m%d_%H%M%S}.log"
    fmt = "%(asctime)s | %(levelname)-8s | %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    logger = logging.getLogger("yolo_detect")
    logger.info("Logging → %s", log_file)
    return logger

logger = setup_logging()

# ─────────────────────────────────────────────
# Image classification
# ─────────────────────────────────────────────
def classify_image(detected_classes: set[str]) -> str:
    """
    Categorise an image based on its detected object classes.

    Rules:
      promotional    — person AND at least one product class
      product_display — product class but NO person
      lifestyle      — person but NO product class
      other          — neither person nor product detected
    """
    has_person  = bool(detected_classes & PERSON_CLASSES)
    has_product = bool(detected_classes & PRODUCT_CLASSES)

    if has_person and has_product:
        return "promotional"
    elif has_product and not has_person:
        return "product_display"
    elif has_person and not has_product:
        return "lifestyle"
    else:
        return "other"


# ─────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "medical_warehouse"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


CREATE_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.yolo_detections (
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


def load_to_postgres(rows: list[dict]) -> None:
    """Insert detection rows into raw.yolo_detections."""
    if not rows:
        logger.warning("No rows to insert into PostgreSQL.")
        return

    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            execute_values(
                cur,
                """
                INSERT INTO raw.yolo_detections
                  (image_path, channel_name, message_id, detected_class,
                   confidence, image_category, model_name)
                VALUES %s
                ON CONFLICT (image_path, detected_class) DO UPDATE
                  SET confidence     = EXCLUDED.confidence,
                      image_category = EXCLUDED.image_category,
                      detected_at    = NOW()
                """,
                [
                    (
                        r["image_path"], r["channel_name"], r["message_id"],
                        r["detected_class"], r["confidence"],
                        r["image_category"], r["model_name"],
                    )
                    for r in rows
                ],
            )
            conn.commit()
        logger.info("Inserted / updated %d rows in raw.yolo_detections", len(rows))
        conn.close()
    except Exception as exc:
        logger.error("PostgreSQL load failed: %s", exc)
        logger.error("Results were saved to CSV — you can load them later.")


# ─────────────────────────────────────────────
# Core detection pipeline
# ─────────────────────────────────────────────
def run_detection() -> None:
    # ── Import ultralytics lazily so the error is readable ──────────────────
    try:
        from ultralytics import YOLO
    except ImportError:
        logger.critical(
            "ultralytics is not installed. Run:  pip install ultralytics"
        )
        sys.exit(1)

    # ── Find all images ──────────────────────────────────────────────────────
    if not IMAGES_DIR.exists():
        logger.critical(
            "Images directory not found: %s\n"
            "Run src/scraper.py first to download images.",
            IMAGES_DIR,
        )
        sys.exit(1)

    image_files = sorted(
        p for p in IMAGES_DIR.rglob("*")
        if p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}
    )

    if not image_files:
        logger.warning("No image files found in %s", IMAGES_DIR)
        return

    logger.info("Found %d images to process", len(image_files))

    # ── Load model ───────────────────────────────────────────────────────────
    logger.info("Loading YOLOv8 model: %s", MODEL_NAME)
    model = YOLO(MODEL_NAME)
    logger.info("Model loaded. Starting detection…")

    # ── CSV writer setup ─────────────────────────────────────────────────────
    csv_fields = [
        "image_path", "channel_name", "message_id",
        "detected_class", "confidence", "image_category", "model_name",
    ]

    all_rows: list[dict] = []
    stats = {
        "images_processed": 0,
        "images_skipped": 0,
        "total_detections": 0,
        "categories": {"promotional": 0, "product_display": 0, "lifestyle": 0, "other": 0},
    }

    with open(CSV_OUTPUT, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_fields)
        writer.writeheader()

        for idx, img_path in enumerate(image_files, 1):
            # Derive channel_name and message_id from path structure:
            # data/raw/images/{channel_name}/{message_id}.jpg
            try:
                channel_name = img_path.parent.name
                message_id   = int(img_path.stem)
            except (ValueError, IndexError):
                logger.warning("Cannot parse channel/message from path: %s", img_path)
                stats["images_skipped"] += 1
                continue

            try:
                results = model(
                    str(img_path),
                    conf=CONF_THRESHOLD,
                    verbose=False,
                )

                # Collect all detected classes for this image
                detected_classes: set[str] = set()
                detections_for_image: list[tuple[str, float]] = []

                for result in results:
                    if result.boxes is None:
                        continue
                    for box in result.boxes:
                        cls_id     = int(box.cls[0])
                        cls_name   = model.names[cls_id]
                        confidence = float(box.conf[0])
                        detected_classes.add(cls_name)
                        detections_for_image.append((cls_name, confidence))

                # Classify image
                image_category = classify_image(detected_classes)
                stats["categories"][image_category] += 1

                # If no objects detected, record a single "no_detection" row
                if not detections_for_image:
                    detections_for_image = [("no_detection", 0.0)]

                # Write one row per detected class
                for cls_name, confidence in detections_for_image:
                    row = {
                        "image_path"    : str(img_path.relative_to(BASE_DIR)),
                        "channel_name"  : channel_name,
                        "message_id"    : message_id,
                        "detected_class": cls_name,
                        "confidence"    : round(confidence, 4),
                        "image_category": image_category,
                        "model_name"    : MODEL_NAME,
                    }
                    writer.writerow(row)
                    all_rows.append(row)

                stats["images_processed"] += 1
                stats["total_detections"] += len(detections_for_image)

                if idx % 50 == 0 or idx == len(image_files):
                    logger.info(
                        "  Progress: %d / %d images  |  detections so far: %d",
                        idx, len(image_files), stats["total_detections"],
                    )

            except Exception as exc:
                logger.warning("Error processing %s: %s", img_path, exc)
                stats["images_skipped"] += 1
                continue

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info("═" * 55)
    logger.info("DETECTION COMPLETE")
    logger.info("  Images processed : %d", stats["images_processed"])
    logger.info("  Images skipped   : %d", stats["images_skipped"])
    logger.info("  Total detections : %d", stats["total_detections"])
    logger.info("  CSV output       : %s", CSV_OUTPUT)
    logger.info("  Category breakdown:")
    for cat, count in stats["categories"].items():
        pct = (count / max(stats["images_processed"], 1)) * 100
        logger.info("    %-20s %d  (%.1f%%)", cat, count, pct)
    logger.info("═" * 55)

    # ── Load to PostgreSQL ────────────────────────────────────────────────────
    logger.info("Loading results to PostgreSQL…")
    load_to_postgres(all_rows)


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    start = time.time()
    run_detection()
    elapsed = time.time() - start
    logger.info("Total time: %.1f seconds (%.1f min)", elapsed, elapsed / 60)
