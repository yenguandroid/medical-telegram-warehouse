# Medical Telegram Warehouse

A data pipeline that scrapes Ethiopian medical business Telegram channels, stores raw data in a partitioned data lake, and exposes it via a FastAPI layer backed by a dbt-modelled PostgreSQL warehouse.

---

## Project Structure

```
medical-telegram-warehouse/
├── src/scraper.py              ← Telegram scraper (Task 1)
├── api/                        ← FastAPI application
├── medical_warehouse/          ← dbt project
├── tests/                      ← pytest unit tests
├── data/raw/
│   ├── telegram_messages/      ← JSON files partitioned by YYYY-MM-DD/channel
│   └── images/                 ← Photos partitioned by channel/message_id.jpg
├── logs/                       ← Scraping logs + summary JSON
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

---

## Task 1 — Setup & Running the Scraper

### 1. Get Telegram API credentials

1. Visit <https://my.telegram.org> and log in with your phone number.
2. Go to **API development tools**.
3. Create a new application — note your **api_id** and **api_hash**.

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env and fill in TELEGRAM_API_ID and TELEGRAM_API_HASH
```

### 3. Install dependencies

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Run the scraper

```bash
# Scrape all default channels
python src/scraper.py

# Scrape specific channels
python src/scraper.py CheMed123 lobelia4cosmetics
```

On first run Telethon will prompt for your phone number and a verification code to create a session file.

### 5. Run with Docker

```bash
cp .env.example .env   # fill in credentials
docker compose up scraper
```

---

## Data Lake Structure

```
data/raw/
├── telegram_messages/
│   ├── 2024-06-01/
│   │   ├── chemed123.json
│   │   └── lobelia4cosmetics.json
│   └── 2024-06-02/
│       └── tikvahpharma.json
└── images/
    ├── chemed123/
    │   ├── 101.jpg
    │   └── 102.jpg
    └── lobelia4cosmetics/
        └── 205.jpg
```

### JSON record schema

```json
{
  "message_id": 101,
  "date": "2024-06-01T08:23:11+00:00",
  "text": "New stock arrived…",
  "views": 1400,
  "forwards": 12,
  "reply_to_msg_id": null,
  "from_id": null,
  "media": {
    "has_photo": true,
    "local_path": "data/raw/images/chemed123/101.jpg",
    "type": "MessageMediaPhoto"
  },
  "entities": [],
  "_raw_channel": "CheMed123",
  "_scraped_at": "2024-06-15T10:00:00+00:00"
}
```

---

## Running Tests

```bash
pytest tests/ -v --cov=src
```

---

## Target Channels

| Channel | Username |
|---|---|
| CheMed | `@CheMed123` |
| Lobelia Cosmetics | `@lobelia4cosmetics` |
| Tikvah Pharma | `@tikvahpharma` |
| Additional channels | <https://et.tgstat.com/medicine> |

---

## Logs

Every scraper run produces two files in `logs/`:

- `scraper_YYYYMMDD_HHMMSS.log` — full structured log
- `scrape_summary_YYYYMMDD_HHMMSS.json` — per-channel stats (messages, images, errors)
