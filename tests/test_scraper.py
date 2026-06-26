"""
Unit tests for src/scraper.py

Run with:  pytest tests/test_scraper.py -v
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.scraper import (
    TelegramScraper,
    _ensure_dir,
    _message_date_str,
    _safe_channel_name,
)


# ─────────────────────────────────────────────
# Pure helper function tests
# ─────────────────────────────────────────────
class TestHelpers:
    def test_message_date_str_utc(self):
        dt = datetime(2024, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
        assert _message_date_str(dt) == "2024-03-15"

    def test_message_date_str_naive(self):
        dt = datetime(2024, 3, 15, 10, 30, 0)  # naive → treated as UTC
        assert _message_date_str(dt) == "2024-03-15"

    def test_safe_channel_name_strips_at(self):
        assert _safe_channel_name("@CheMed123") == "chemed123"

    def test_safe_channel_name_lowercase(self):
        assert _safe_channel_name("Lobelia4Cosmetics") == "lobelia4cosmetics"

    def test_safe_channel_name_strips_whitespace(self):
        assert _safe_channel_name("  tikvahpharma  ") == "tikvahpharma"

    def test_ensure_dir_creates_path(self, tmp_path):
        target = tmp_path / "a" / "b" / "c"
        result = _ensure_dir(target)
        assert result.exists()
        assert result.is_dir()

    def test_ensure_dir_returns_path(self, tmp_path):
        target = tmp_path / "new_dir"
        assert _ensure_dir(target) == target


# ─────────────────────────────────────────────
# TelegramScraper unit tests (mocked client)
# ─────────────────────────────────────────────
def _make_fake_message(
    msg_id: int = 1,
    text: str = "Test message",
    has_photo: bool = False,
    views: int = 100,
    forwards: int = 5,
    date: datetime | None = None,
):
    """Build a minimal fake Telethon Message-like object."""
    from telethon.tl.types import MessageMediaPhoto

    msg = MagicMock()
    msg.id = msg_id
    msg.text = text
    msg.views = views
    msg.forwards = forwards
    msg.reply_to_msg_id = None
    msg.from_id = None
    msg.entities = []
    msg.date = date or datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    msg.media = MagicMock(spec=MessageMediaPhoto) if has_photo else None
    return msg


@pytest.fixture
def scraper(tmp_path, monkeypatch):
    """Return a TelegramScraper whose client is fully mocked."""
    monkeypatch.chdir(tmp_path)

    # Patch directory constants inside the module
    monkeypatch.setattr("src.scraper.BASE_DIR", tmp_path)
    monkeypatch.setattr("src.scraper.DATA_DIR", tmp_path / "data" / "raw")
    monkeypatch.setattr("src.scraper.IMAGES_DIR", tmp_path / "data" / "raw" / "images")
    monkeypatch.setattr("src.scraper.MESSAGES_DIR", tmp_path / "data" / "raw" / "telegram_messages")
    monkeypatch.setattr("src.scraper.LOGS_DIR", tmp_path / "logs")

    with patch("src.scraper.TelegramClient") as MockClient:
        instance = MockClient.return_value
        instance.start = AsyncMock()
        instance.disconnect = AsyncMock()
        instance.get_entity = AsyncMock(return_value=MagicMock())
        instance.download_media = AsyncMock()

        s = TelegramScraper(api_id=12345, api_hash="fakehash")
        s.client = instance
        yield s, tmp_path


@pytest.mark.asyncio
async def test_scrape_channel_writes_json(scraper):
    s, tmp_path = scraper
    messages = [
        _make_fake_message(msg_id=1, text="Hello"),
        _make_fake_message(msg_id=2, text="World"),
    ]

    async def _fake_iter(*args, **kwargs):
        for m in messages:
            yield m

    s.client.iter_messages = _fake_iter

    await s.scrape_channel("TestChannel")

    # Check that a JSON file was written
    all_json = list((tmp_path / "data" / "raw" / "telegram_messages").rglob("*.json"))
    assert len(all_json) == 1

    records = json.loads(all_json[0].read_text())
    assert len(records) == 2
    assert records[0]["text"] == "Hello"
    assert records[1]["text"] == "World"


@pytest.mark.asyncio
async def test_scrape_channel_records_summary(scraper):
    s, _ = scraper
    messages = [_make_fake_message(msg_id=i) for i in range(3)]

    async def _fake_iter(*args, **kwargs):
        for m in messages:
            yield m

    s.client.iter_messages = _fake_iter

    await s.scrape_channel("TestChannel")

    assert "TestChannel" in s.scrape_summary
    assert s.scrape_summary["TestChannel"]["status"] == "success"
    assert s.scrape_summary["TestChannel"]["total_messages"] == 3


@pytest.mark.asyncio
async def test_scrape_channel_handles_private_channel(scraper):
    from telethon.errors import ChannelPrivateError

    s, _ = scraper
    s.client.get_entity = AsyncMock(side_effect=ChannelPrivateError(request=None))

    await s.scrape_channel("PrivateChannel")

    assert s.scrape_summary["PrivateChannel"]["status"] == "error"


@pytest.mark.asyncio
async def test_scrape_channel_downloads_image(scraper):
    s, tmp_path = scraper
    photo_msg = _make_fake_message(msg_id=99, has_photo=True)

    async def _fake_iter(*args, **kwargs):
        yield photo_msg

    s.client.iter_messages = _fake_iter
    s.client.download_media = AsyncMock(return_value=None)

    await s.scrape_channel("PhotoChannel")

    # download_media should have been called once
    s.client.download_media.assert_called_once()


@pytest.mark.asyncio
async def test_scrape_channel_merges_existing_json(scraper):
    """New messages are merged with an existing JSON file, no duplicates."""
    s, tmp_path = scraper

    # Pre-populate a JSON file for 2024-06-01
    existing_dir = tmp_path / "data" / "raw" / "telegram_messages" / "2024-06-01"
    existing_dir.mkdir(parents=True)
    existing_file = existing_dir / "testchannel.json"
    existing_file.write_text(json.dumps([{"message_id": 1, "text": "Old"}]), encoding="utf-8")

    # Scraper returns same id=1 and a new id=2
    messages = [
        _make_fake_message(msg_id=1, text="Old"),
        _make_fake_message(msg_id=2, text="New"),
    ]

    async def _fake_iter(*args, **kwargs):
        for m in messages:
            yield m

    s.client.iter_messages = _fake_iter
    await s.scrape_channel("TestChannel")

    records = json.loads(existing_file.read_text())
    ids = [r["message_id"] for r in records]
    assert sorted(ids) == [1, 2], "Should merge without duplicates"


@pytest.mark.asyncio
async def test_scrape_channel_partitions_by_date(scraper):
    """Messages on different dates go to different partition directories."""
    s, tmp_path = scraper
    messages = [
        _make_fake_message(msg_id=1, date=datetime(2024, 6, 1, tzinfo=timezone.utc)),
        _make_fake_message(msg_id=2, date=datetime(2024, 6, 2, tzinfo=timezone.utc)),
    ]

    async def _fake_iter(*args, **kwargs):
        for m in messages:
            yield m

    s.client.iter_messages = _fake_iter
    await s.scrape_channel("TestChannel")

    partitions = list((tmp_path / "data" / "raw" / "telegram_messages").iterdir())
    assert len(partitions) == 2
