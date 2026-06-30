"""
pipeline/pipeline.py
Medical Telegram Warehouse — Dagster Pipeline
Fixed version: dbt_refresh job uses a dedicated no-input op.
"""
import re
import os
import subprocess
import sys
import json
from datetime import datetime
from pathlib import Path

from dagster import (
    op,
    job,
    OpExecutionContext,
    Out,
    In,
    Output,
    Failure,
    MetadataValue,
    ScheduleDefinition,
    Definitions,
    DefaultScheduleStatus,
)
from dotenv import load_dotenv

load_dotenv()

BASE_DIR    = Path(__file__).resolve().parent.parent
SRC_DIR     = BASE_DIR / "src"
SCRIPTS_DIR = BASE_DIR / "scripts"
DBT_DIR     = BASE_DIR / "medical_warehouse"
DATA_DIR    = BASE_DIR / "data"
LOGS_DIR    = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────
def _run_script(context: OpExecutionContext, cmd: list, cwd: Path = None) -> str:
    cwd = cwd or BASE_DIR
    context.log.info("Running: %s", " ".join(cmd))

    # Force UTF-8 so Windows console encoding (cp1252) doesn't choke on
    # Unicode characters like arrows/emojis used in our log messages.
    env = {**os.environ}
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    result = subprocess.run(
        cmd, capture_output=True, text=True,
        cwd=str(cwd), env=env,
        encoding="utf-8", errors="replace",
    )
    for line in (result.stdout or "").splitlines():
        context.log.info(line)
    for line in (result.stderr or "").splitlines():
        if any(k in line.lower() for k in ["error", "fail", "critical"]):
            context.log.error(line)
        else:
            context.log.info(line)
    if result.returncode != 0:
        raise Failure(
            description=f"Command failed (exit {result.returncode})",
            metadata={"stderr": MetadataValue.text((result.stderr or "")[-2000:])},
        )
    return result.stdout or ""


# ═══════════════════════════════════════════════
# OP 1 — Scrape
# ═══════════════════════════════════════════════
@op(out=Out(dict), description="Scrapes messages and images from Telegram channels.")
def scrape_telegram_data(context: OpExecutionContext) -> dict:
    context.log.info("STEP 1: Scraping Telegram channels")
    scraper_path = SRC_DIR / "scraper.py"
    if not scraper_path.exists():
        raise Failure(description=f"Scraper not found: {scraper_path}")
    _run_script(context, [sys.executable, str(scraper_path)])

    summary_files = sorted(LOGS_DIR.glob("scrape_summary_*.json"), reverse=True)
    stats = {}
    if summary_files:
        try:
            stats = json.loads(summary_files[0].read_text(encoding="utf-8"))
        except Exception:
            pass

    total_msgs = sum(v.get("total_messages", 0) for v in stats.values() if isinstance(v, dict))
    total_imgs = sum(v.get("total_images", 0) for v in stats.values() if isinstance(v, dict))
    context.log.info("Scraped: %d messages, %d images", total_msgs, total_imgs)
    return {"total_messages": total_msgs, "total_images": total_imgs,
            "scraped_at": datetime.utcnow().isoformat()}


# ═══════════════════════════════════════════════
# OP 2 — Load Raw → PostgreSQL
# ═══════════════════════════════════════════════
@op(ins={"scrape_result": In(dict)}, out=Out(dict),
    description="Loads JSON data lake into PostgreSQL raw.telegram_messages.")
def load_raw_to_postgres(context: OpExecutionContext, scrape_result: dict) -> dict:
    context.log.info("STEP 2: Loading raw data to PostgreSQL")
    loader = SCRIPTS_DIR / "load_raw_to_postgres.py"
    if not loader.exists():
        raise Failure(description=f"Loader not found: {loader}")
    stdout = _run_script(context, [sys.executable, str(loader)])
    inserted = 0
    for line in stdout.splitlines():
        if "Rows inserted" in line:
            try:
                inserted = int(line.split(":")[-1].strip())
            except ValueError:
                pass
    context.log.info("Inserted: %d rows", inserted)
    return {**scrape_result, "rows_inserted": inserted}


# ═══════════════════════════════════════════════
# OP 3 — dbt (with upstream input)
# ═══════════════════════════════════════════════
@op(ins={"load_result": In(dict)}, out=Out(dict),
    description="Runs dbt models and tests to build the star schema.")
def run_dbt_transformations(context: OpExecutionContext, load_result: dict) -> dict:
    context.log.info("STEP 3: Running dbt transformations")
    _run_script(context, ["dbt", "deps"], cwd=DBT_DIR)
    _run_script(context, ["dbt", "run", "--profiles-dir", str(DBT_DIR)], cwd=DBT_DIR)
    test_out = _run_script(context, ["dbt", "test", "--profiles-dir", str(DBT_DIR)], cwd=DBT_DIR)

    match = re.search(
        r"Done\.\s+PASS=(\d+)\s+WARN=(\d+)\s+ERROR=(\d+)\s+SKIP=(\d+)\s+NO-OP=(\d+)\s+TOTAL=(\d+)",
        test_out,
    )
    if not match:
        raise Failure(description="Could not parse dbt test summary line — dbt output format may have changed.")

    passed, warned, errored, skipped, no_op, total = map(int, match.groups())
    failed = errored  # WARN is not a failure unless you want to treat it as one
    context.log.info("dbt tests: %d passed, %d failed, %d warned, %d total", passed, failed, warned, total)

    if failed > 0:
        raise Failure(description=f"dbt tests failed: {failed} failures")
    return {**load_result, "dbt_tests_passed": passed}

# ═══════════════════════════════════════════════
# OP 3b — dbt standalone (no upstream input)
# ═══════════════════════════════════════════════
@op(out=Out(dict), description="Runs dbt models and tests — no upstream dependency.")
def run_dbt_standalone(context: OpExecutionContext) -> dict:
    context.log.info("Running dbt standalone refresh")
    _run_script(context, ["dbt", "deps"], cwd=DBT_DIR)
    _run_script(context, ["dbt", "run", "--profiles-dir", str(DBT_DIR)], cwd=DBT_DIR)
    test_out = _run_script(context, ["dbt", "test", "--profiles-dir", str(DBT_DIR)], cwd=DBT_DIR)

    match = re.search(
        r"Done\.\s+PASS=(\d+)\s+WARN=(\d+)\s+ERROR=(\d+)\s+SKIP=(\d+)\s+NO-OP=(\d+)\s+TOTAL=(\d+)",
        test_out,
    )
    if not match:
        raise Failure(description="Could not parse dbt test summary line — dbt output format may have changed.")
    passed, warned, errored, skipped, no_op, total = map(int, match.groups())
    failed = errored
    context.log.info("dbt tests: %d passed, %d failed, %d warned, %d total", passed, failed, warned, total)

    if failed > 0:
        raise Failure(description=f"dbt tests failed: {failed} failures")
    return {"dbt_tests_passed": passed, "ran_at": datetime.utcnow().isoformat()}


# ═══════════════════════════════════════════════
# OP 4 — YOLO
# ═══════════════════════════════════════════════
@op(ins={"dbt_result": In(dict)}, out=Out(dict),
    description="Runs YOLOv8 object detection on all downloaded images.")
def run_yolo_enrichment(context: OpExecutionContext, dbt_result: dict) -> dict:
    context.log.info("STEP 4: Running YOLO object detection")
    yolo_script = SRC_DIR / "yolo_detect.py"
    if not yolo_script.exists():
        raise Failure(description=f"YOLO script not found: {yolo_script}")
    images_dir = DATA_DIR / "raw" / "images"
    image_count = len(list(images_dir.rglob("*.jpg"))) if images_dir.exists() else 0
    context.log.info("Found %d images", image_count)
    _run_script(context, [sys.executable, str(yolo_script)])
    return {**dbt_result, "images_processed": image_count}


# ═══════════════════════════════════════════════
# OP 5 — Load YOLO → PostgreSQL
# ═══════════════════════════════════════════════
@op(ins={"yolo_result": In(dict)}, out=Out(dict),
    description="Loads YOLO detection CSV into PostgreSQL raw.yolo_detections.")
def load_yolo_to_postgres(context: OpExecutionContext, yolo_result: dict) -> dict:
    context.log.info("STEP 5: Loading YOLO results to PostgreSQL")
    csv_path = DATA_DIR / "processed" / "yolo_detections.csv"
    if not csv_path.exists():
        raise Failure(description=f"YOLO CSV not found: {csv_path}")
    loader = SCRIPTS_DIR / "load_yolo_to_postgres.py"
    stdout = _run_script(context, [sys.executable, str(loader)])
    inserted = 0
    for line in stdout.splitlines():
        if "rows inserted" in line.lower():
            try:
                inserted = int("".join(filter(str.isdigit, line.split("—")[-1])))
            except ValueError:
                pass
    context.log.info("YOLO rows inserted: %d", inserted)
    return {**yolo_result, "yolo_rows_inserted": inserted}


# ═══════════════════════════════════════════════
# OP 6 — Rebuild fct_image_detections
# ═══════════════════════════════════════════════
@op(ins={"yolo_load_result": In(dict)}, out=Out(dict),
    description="Rebuilds fct_image_detections dbt model after YOLO load.")
def run_dbt_with_detections(context: OpExecutionContext, yolo_load_result: dict) -> dict:
    context.log.info("STEP 6: Rebuilding fct_image_detections")
    _run_script(context,
        ["dbt", "run", "--select", "fct_image_detections",
         "--profiles-dir", str(DBT_DIR)], cwd=DBT_DIR)
    context.log.info("fct_image_detections rebuilt successfully")
    return {**yolo_load_result,
            "pipeline_completed_at": datetime.utcnow().isoformat(),
            "status": "success"}


# ═══════════════════════════════════════════════
# JOB 1 — Full Pipeline
# ═══════════════════════════════════════════════
@job(name="medical_telegram_pipeline",
     description="Full ETL: scrape → load → dbt → YOLO → load → dbt",
     tags={"project": "medical-warehouse"})
def medical_telegram_pipeline():
    scrape_result = scrape_telegram_data()
    load_result   = load_raw_to_postgres(scrape_result=scrape_result)
    dbt_result    = run_dbt_transformations(load_result=load_result)
    yolo_result   = run_yolo_enrichment(dbt_result=dbt_result)
    yolo_load     = load_yolo_to_postgres(yolo_result=yolo_result)
    run_dbt_with_detections(yolo_load_result=yolo_load)


# ═══════════════════════════════════════════════
# JOB 2 — Daily Scrape + Load
# ═══════════════════════════════════════════════
@job(name="daily_scrape_and_load",
     description="Lightweight daily job: scrape new messages and load to PostgreSQL.")
def daily_scrape_and_load():
    scrape_result = scrape_telegram_data()
    load_raw_to_postgres(scrape_result=scrape_result)


# ═══════════════════════════════════════════════
# JOB 3 — dbt Refresh (standalone — no scraper)
# ═══════════════════════════════════════════════
@job(name="dbt_refresh",
     description="Quick dbt model refresh — no scraping required.")
def dbt_refresh():
    run_dbt_standalone()


# ═══════════════════════════════════════════════
# SCHEDULES
# ═══════════════════════════════════════════════
daily_scrape_schedule = ScheduleDefinition(
    name="daily_scrape_6am",
    job=daily_scrape_and_load,
    cron_schedule="0 6 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Scrapes new Telegram messages every day at 6:00 AM.",
)

weekly_full_pipeline_schedule = ScheduleDefinition(
    name="weekly_full_pipeline",
    job=medical_telegram_pipeline,
    cron_schedule="0 0 * * 0",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Full pipeline every Sunday at midnight.",
)


# ═══════════════════════════════════════════════
# DEFINITIONS
# ═══════════════════════════════════════════════
defs = Definitions(
    jobs=[medical_telegram_pipeline, daily_scrape_and_load, dbt_refresh],
    schedules=[daily_scrape_schedule, weekly_full_pipeline_schedule],
)
