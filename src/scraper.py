"""
Telegram Medical Channel Scraper
Extracts messages and images from Ethiopian medical business Telegram channels
and stores them in a raw data lake with partitioned directory structure.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    UsernameNotOccupiedError,
)
from telethon.tl.types import MessageMediaPhoto, PeerChannel

# Force UTF-8 stdout/stderr so emoji/arrows in log messages don't crash
# on Windows consoles using the legacy 'charmap' (cp1252) encoding —
# this matters especially when running under Dagster, which captures
# subprocess output with strict encoding by default.
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass  # Python < 3.7 fallback not needed; project requires 3.11+

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"
IMAGES_DIR = DATA_DIR / "images"
MESSAGES_DIR = DATA_DIR / "telegram_messages"
LOGS_DIR = BASE_DIR / "logs"

# Target channels
TARGET_CHANNELS = [
    "CheMed123",
    "lobelia4cosmetics",
    "tikvahpharma",
    # Additional Ethiopian medical channels
    "ethiopianpharmacists",
    "DoctorsEthiopia",
]

# How many messages to fetch per channel (set None for all)
MESSAGE_LIMIT: Optional[int] = 500


# ─────────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────────
def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_filename = LOGS_DIR / f"scraper_{datetime.now():%Y%m%d_%H%M%S}.log"

    fmt = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_filename, encoding="utf-8"),
    ]

    logging.basicConfig(level=logging.INFO, format=fmt, datefmt=date_fmt, handlers=handlers)
    logger = logging.getLogger("telegram_scraper")
    logger.info("Logging initialised → %s", log_filename)
    return logger


logger = setup_logging()


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def _message_date_str(msg_date: datetime) -> str:
    """Return YYYY-MM-DD for the message date (UTC)."""
    if msg_date.tzinfo is None:
        msg_date = msg_date.replace(tzinfo=timezone.utc)
    return msg_date.strftime("%Y-%m-%d")


def _safe_channel_name(name: str) -> str:
    """Sanitise channel name for use as a directory / file component."""
    return name.strip().lstrip("@").lower()


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


# ─────────────────────────────────────────────
# Core scraper
# ─────────────────────────────────────────────
class TelegramScraper:
    def __init__(self, api_id: int, api_hash: str, session_name: str = "medical_scraper"):
        self.client = TelegramClient(session_name, api_id, api_hash)
        self.scrape_summary: dict[str, dict] = {}

    # ── Image download ──────────────────────────────────────────────────────
    async def _download_image(
        self,
        message,
        channel_name: str,
    ) -> Optional[str]:
        """Download photo attached to *message* and return its local path."""
        if not isinstance(message.media, MessageMediaPhoto):
            return None

        img_dir = _ensure_dir(IMAGES_DIR / channel_name)
        img_path = img_dir / f"{message.id}.jpg"

        if img_path.exists():
            logger.debug("Image already exists, skipping: %s", img_path)
            return str(img_path.relative_to(BASE_DIR))

        try:
            await self.client.download_media(message.media, file=str(img_path))
            logger.debug("Downloaded image → %s", img_path)
            return str(img_path.relative_to(BASE_DIR))
        except Exception as exc:
            logger.warning("Failed to download image for msg %s: %s", message.id, exc)
            return None

    # ── Single channel scrape ───────────────────────────────────────────────
    async def scrape_channel(self, channel_username: str) -> None:
        safe_name = _safe_channel_name(channel_username)
        logger.info("▶  Starting scrape: @%s", channel_username)

        try:
            entity = await self.client.get_entity(channel_username)
        except (UsernameNotOccupiedError, ChannelPrivateError, ValueError) as exc:
            logger.error("Cannot access channel @%s: %s", channel_username, exc)
            self.scrape_summary[channel_username] = {"status": "error", "reason": str(exc)}
            return

        # Group messages by date so we can write per-day JSON files
        messages_by_date: dict[str, list[dict]] = {}
        total_messages = 0
        total_images = 0
        errors = 0

        try:
            async for message in self.client.iter_messages(entity, limit=MESSAGE_LIMIT):
                try:
                    date_str = _message_date_str(message.date)

                    # Download image (if any)
                    image_local_path: Optional[str] = None
                    has_photo = isinstance(message.media, MessageMediaPhoto)
                    if has_photo:
                        image_local_path = await self._download_image(message, safe_name)
                        if image_local_path:
                            total_images += 1

                    # Build record — preserve original API structure where possible
                    record = {
                        "message_id": message.id,
                        "date": message.date.isoformat(),
                        "text": message.text or "",
                        "views": message.views,
                        "forwards": message.forwards,
                        "reply_to_msg_id": message.reply_to_msg_id,
                        "from_id": str(message.from_id) if message.from_id else None,
                        "media": {
                            "has_photo": has_photo,
                            "local_path": image_local_path,
                            "type": type(message.media).__name__ if message.media else None,
                        },
                        "entities": [
                            {
                                "type": type(e).__name__,
                                "offset": e.offset,
                                "length": e.length,
                            }
                            for e in (message.entities or [])
                        ],
                        "_raw_channel": channel_username,
                        "_scraped_at": datetime.now(tz=timezone.utc).isoformat(),
                    }

                    messages_by_date.setdefault(date_str, []).append(record)
                    total_messages += 1

                except Exception as exc:
                    errors += 1
                    logger.warning("Error processing message %s in @%s: %s", message.id, channel_username, exc)

        except FloodWaitError as exc:
            logger.error("Rate limited on @%s — must wait %d s. Saving partial data.", channel_username, exc.seconds)
            await asyncio.sleep(exc.seconds)

        # ── Persist to data lake ─────────────────────────────────────────────
        for date_str, records in messages_by_date.items():
            out_dir = _ensure_dir(MESSAGES_DIR / date_str)
            out_file = out_dir / f"{safe_name}.json"

            # Merge with any existing file for the same day/channel
            existing: list[dict] = []
            if out_file.exists():
                try:
                    existing = json.loads(out_file.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    logger.warning("Corrupt existing file, overwriting: %s", out_file)

            existing_ids = {r["message_id"] for r in existing}
            new_records = [r for r in records if r["message_id"] not in existing_ids]
            merged = existing + new_records

            out_file.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("  Saved %d records for %s / %s → %s", len(merged), date_str, safe_name, out_file)

        self.scrape_summary[channel_username] = {
            "status": "success",
            "total_messages": total_messages,
            "total_images": total_images,
            "errors": errors,
            "dates_scraped": sorted(messages_by_date.keys()),
        }
        logger.info(
            "✔  Finished @%s — %d messages, %d images, %d errors",
            channel_username,
            total_messages,
            total_images,
            errors,
        )

    # ── Run all channels ────────────────────────────────────────────────────
    async def run(self, channels: list[str]) -> None:
        await self.client.start()
        logger.info("Telegram client connected. Scraping %d channels…", len(channels))

        for channel in channels:
            await self.scrape_channel(channel)
            # Polite delay between channels to avoid rate limits
            await asyncio.sleep(2)

        await self.client.disconnect()
        self._write_summary()

    # ── Summary report ──────────────────────────────────────────────────────
    def _write_summary(self) -> None:
        summary_path = LOGS_DIR / f"scrape_summary_{datetime.now():%Y%m%d_%H%M%S}.json"
        summary_path.write_text(
            json.dumps(self.scrape_summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("Summary written → %s", summary_path)

        # Human-readable recap
        ok = sum(1 for v in self.scrape_summary.values() if v.get("status") == "success")
        fail = len(self.scrape_summary) - ok
        logger.info("═" * 50)
        logger.info("SCRAPE COMPLETE  ✔ %d succeeded  ✖ %d failed", ok, fail)
        for ch, info in self.scrape_summary.items():
            if info["status"] == "success":
                logger.info(
                    "  @%-25s  msgs=%-5d  imgs=%-4d  errs=%d",
                    ch,
                    info["total_messages"],
                    info["total_images"],
                    info["errors"],
                )
            else:
                logger.warning("  @%-25s  FAILED — %s", ch, info.get("reason"))
        logger.info("═" * 50)


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
def main() -> None:
    api_id_raw = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")

    if not api_id_raw or not api_hash:
        logger.critical(
            "TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env. "
            "Register your app at https://my.telegram.org"
        )
        sys.exit(1)

    try:
        api_id = int(api_id_raw)
    except ValueError:
        logger.critical("TELEGRAM_API_ID must be an integer, got: %r", api_id_raw)
        sys.exit(1)

    # Allow channel list override via CLI args
    channels = sys.argv[1:] if len(sys.argv) > 1 else TARGET_CHANNELS

    scraper = TelegramScraper(api_id=api_id, api_hash=api_hash)
    asyncio.run(scraper.run(channels))


if __name__ == "__main__":
    main()
