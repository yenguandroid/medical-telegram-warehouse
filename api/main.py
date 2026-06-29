"""
api/main.py
Medical Telegram Warehouse — FastAPI Application
Exposes analytical endpoints over the dbt-modelled PostgreSQL data warehouse.

Run:
    uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
    (from project root: C:\\Users\\loolt\\medical-telegram-warehouse)
"""

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging

from api.database import get_db, check_db_connection
from api.schemas import (
    HealthResponse,
    TopProductsResponse,
    TopProductItem,
    ChannelActivityResponse,
    DailyActivity,
    MessageSearchResponse,
    MessageSearchItem,
    VisualContentResponse,
    ChannelVisualStats,
    DetectionSummaryResponse,
    DetectionCategoryStats,
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Medical Telegram Warehouse API",
    description="""
## Ethiopian Medical Telegram Channel Analytics

This API exposes analytical insights from scraped Telegram messages across
Ethiopian pharmaceutical, cosmetics, and medical channels.

### Data Pipeline
- **Task 1** — Telegram scraper (1,077 messages, 728 images)
- **Task 2** — dbt star schema (staging → dims → facts)
- **Task 3** — YOLOv8 object detection (966 detections)
- **Task 4** — This FastAPI layer

### Available Endpoints
| Endpoint | Description |
|---|---|
| `GET /api/reports/top-products` | Most mentioned terms/products |
| `GET /api/channels/{channel_name}/activity` | Channel posting activity |
| `GET /api/search/messages` | Full-text message search |
| `GET /api/reports/visual-content` | Image usage statistics |
| `GET /api/reports/detection-summary` | YOLO detection breakdown |
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── Health check ─────────────────────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse, tags=["System"],
         summary="Health check",
         description="Returns API health status and database connectivity.")
def health_check():
    db_ok = check_db_connection()
    return HealthResponse(
        status="ok" if db_ok else "degraded",
        database="connected" if db_ok else "unreachable",
    )


# ── Endpoint 1 — Top Products ─────────────────────────────────────────────────
@app.get("/api/reports/top-products", response_model=TopProductsResponse,
         tags=["Reports"], summary="Top mentioned terms / products",
         description="Returns most frequently mentioned terms across all channel messages.")
def top_products(
    limit: int = Query(default=10, ge=1, le=50, description="Number of top terms to return"),
    channel: str = Query(default=None, description="Filter by channel name (optional)"),
    db: Session = Depends(get_db),
):
    channel_filter = "AND channel_name = :channel" if channel else ""
    sql = text(f"""
        WITH words AS (
            SELECT channel_name, view_count, LOWER(word) AS term
            FROM public_staging.stg_telegram_messages,
                 LATERAL unnest(string_to_array(
                     regexp_replace(
                   regexp_replace(message_text, '[\\n\\r\\t]', ' ', 'g'),  '[^a-zA-Z0-9\\s]', ' ', 'g'), ' '
                 )) AS word
            WHERE has_text = TRUE AND LENGTH(word) >= 4
            {channel_filter}
        ),
        stop_words AS (
            SELECT unnest(ARRAY[
                'that','this','with','from','have','will','been','they',
                'were','your','also','more','when','what','then','than',
                'some','into','over','http','https','each','which','their',
                'about','these','would','there','where','hello','please',
                'thank','thanks','dear','good','very','just','like','time',
                'here','know','need','make','take','come','back','much',
                'such','many','well','only','even','most','after','before',
                'first','those','other','being','both'
            ]) AS word
        )
        SELECT w.term,
               COUNT(*) AS mention_count,
               COUNT(DISTINCT w.channel_name) AS channel_count,
               ROUND(AVG(w.view_count), 1) AS avg_views
        FROM words w
        WHERE w.term NOT IN (SELECT word FROM stop_words) AND w.term != ''
        GROUP BY w.term
        ORDER BY mention_count DESC
        LIMIT :limit
    """)
    params = {"limit": limit}
    if channel:
        params["channel"] = channel.lower()
    try:
        rows = db.execute(sql, params).fetchall()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return TopProductsResponse(
        total_results=len(rows), limit=limit,
        data=[TopProductItem(term=r.term, mention_count=r.mention_count,
              channel_count=r.channel_count, avg_views=float(r.avg_views or 0))
              for r in rows],
    )


# ── Endpoint 2 — Channel Activity ────────────────────────────────────────────
@app.get("/api/channels/{channel_name}/activity", response_model=ChannelActivityResponse,
         tags=["Channels"], summary="Channel posting activity",
         description="Returns posting activity and engagement trends for a specific channel. Available: chemed123, lobelia4cosmetics, tikvahpharma")
def channel_activity(
    channel_name: str,
    days: int = Query(default=30, ge=1, le=365, description="Number of recent days"),
    db: Session = Depends(get_db),
):
    channel_row = db.execute(text("""
        SELECT channel_name, channel_type, total_posts, avg_views,
               avg_forwards, first_post_date, last_post_date, total_images
        FROM public_marts.dim_channels WHERE channel_name = :channel
    """), {"channel": channel_name.lower()}).fetchone()

    if not channel_row:
        raise HTTPException(status_code=404,
            detail=f"Channel '{channel_name}' not found. Available: chemed123, lobelia4cosmetics, tikvahpharma")

    activity_rows = db.execute(text("""
        SELECT fm.message_date::DATE AS activity_date,
               COUNT(*) AS message_count,
               COALESCE(SUM(fm.view_count), 0) AS total_views,
               SUM(CASE WHEN fm.has_image THEN 1 ELSE 0 END) AS image_count
        FROM public_marts.fct_messages fm
        JOIN public_marts.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE dc.channel_name = :channel
          AND fm.message_date >= CURRENT_DATE - (:days || ' days')::INTERVAL
        GROUP BY fm.message_date::DATE
        ORDER BY activity_date DESC
    """), {"channel": channel_name.lower(), "days": days}).fetchall()

    totals = db.execute(text("""
        SELECT COALESCE(SUM(fm.view_count), 0) AS total_views,
               COUNT(*) AS total_posts,
               SUM(CASE WHEN fm.has_image THEN 1 ELSE 0 END) AS image_posts
        FROM public_marts.fct_messages fm
        JOIN public_marts.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE dc.channel_name = :channel
    """), {"channel": channel_name.lower()}).fetchone()

    total_posts = totals.total_posts or 1
    image_rate = round((totals.image_posts / total_posts) * 100, 1)

    return ChannelActivityResponse(
        channel_name=channel_row.channel_name,
        channel_type=channel_row.channel_type,
        total_posts=channel_row.total_posts,
        total_views=int(totals.total_views),
        avg_views_per_post=round(float(channel_row.avg_views or 0), 1),
        avg_forwards_per_post=round(float(channel_row.avg_forwards or 0), 1),
        first_post_date=channel_row.first_post_date,
        last_post_date=channel_row.last_post_date,
        image_rate_pct=image_rate,
        daily_activity=[
            DailyActivity(activity_date=r.activity_date,
                          message_count=r.message_count,
                          total_views=int(r.total_views),
                          image_count=int(r.image_count))
            for r in activity_rows
        ],
    )


# ── Endpoint 3 — Message Search ───────────────────────────────────────────────
@app.get("/api/search/messages", response_model=MessageSearchResponse,
         tags=["Search"], summary="Search messages by keyword",
         description="Full-text search across all scraped messages. Example queries: paracetamol, amoxicillin, cream, delivery, price")
def search_messages(
    query: str = Query(..., min_length=2, max_length=100, description="Search keyword"),
    limit: int = Query(default=20, ge=1, le=100, description="Maximum results"),
    channel: str = Query(default=None, description="Filter by channel (optional)"),
    db: Session = Depends(get_db),
):
    channel_filter = "AND dc.channel_name = :channel" if channel else ""
    params = {"query": f"%{query}%", "limit": limit}
    if channel:
        params["channel"] = channel.lower()

    rows = db.execute(text(f"""
        SELECT fm.message_id, dc.channel_name, fm.message_date,
               fm.message_text, fm.view_count, fm.forward_count,
               fm.has_image, dc.channel_type
        FROM public_marts.fct_messages fm
        JOIN public_marts.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE fm.message_text ILIKE :query AND fm.has_text = TRUE
        {channel_filter}
        ORDER BY fm.view_count DESC
        LIMIT :limit
    """), params).fetchall()

    total = db.execute(text(f"""
        SELECT COUNT(*) FROM public_marts.fct_messages fm
        JOIN public_marts.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE fm.message_text ILIKE :query AND fm.has_text = TRUE {channel_filter}
    """), {k: v for k, v in params.items() if k != "limit"}).scalar()

    return MessageSearchResponse(
        query=query, total_results=total or 0, limit=limit,
        data=[MessageSearchItem(
            message_id=r.message_id, channel_name=r.channel_name,
            message_date=r.message_date, message_text=(r.message_text or "")[:500],
            view_count=r.view_count, forward_count=r.forward_count,
            has_image=r.has_image, channel_type=r.channel_type)
            for r in rows],
    )


# ── Endpoint 4 — Visual Content Stats ────────────────────────────────────────
@app.get("/api/reports/visual-content", response_model=VisualContentResponse,
         tags=["Reports"], summary="Visual content statistics",
         description="Image usage stats per channel including YOLO category breakdown and engagement lift.")
def visual_content_stats(db: Session = Depends(get_db)):
    rows = db.execute(text("""
        SELECT dc.channel_name, dc.channel_type, dc.total_posts AS total_messages,
               dc.total_images,
               ROUND((dc.total_images::NUMERIC / NULLIF(dc.total_posts,0))*100,1) AS image_rate_pct,
               COALESCE(SUM(CASE WHEN fid.image_category='promotional' THEN 1 ELSE 0 END),0) AS promotional_count,
               COALESCE(SUM(CASE WHEN fid.image_category='product_display' THEN 1 ELSE 0 END),0) AS product_display_count,
               COALESCE(SUM(CASE WHEN fid.image_category='lifestyle' THEN 1 ELSE 0 END),0) AS lifestyle_count,
               COALESCE(SUM(CASE WHEN fid.image_category='other' THEN 1 ELSE 0 END),0) AS other_count,
               ROUND(AVG(CASE WHEN fm.has_image THEN fm.view_count END),1) AS avg_views_with_image,
               ROUND(AVG(CASE WHEN NOT fm.has_image THEN fm.view_count END),1) AS avg_views_without_image
        FROM public_marts.dim_channels dc
        LEFT JOIN public_marts.fct_messages fm ON dc.channel_key = fm.channel_key
        LEFT JOIN public_marts.fct_image_detections fid
            ON fm.channel_key = fid.channel_key AND fm.message_id = fid.message_id
            AND fid.has_detection = TRUE
        GROUP BY dc.channel_name, dc.channel_type, dc.total_posts, dc.total_images
        ORDER BY dc.total_posts DESC
    """)).fetchall()

    total_img = db.execute(text("SELECT SUM(total_images) FROM public_marts.dim_channels")).scalar() or 0
    total_msg = db.execute(text("SELECT SUM(total_posts) FROM public_marts.dim_channels")).scalar() or 1
    overall_rate = round((total_img / total_msg) * 100, 1)

    channels = []
    for r in rows:
        avg_with = float(r.avg_views_with_image or 0)
        avg_without = float(r.avg_views_without_image or 0)
        lift = round(((avg_with - avg_without) / avg_without) * 100, 1) if avg_without > 0 else 0.0
        channels.append(ChannelVisualStats(
            channel_name=r.channel_name, channel_type=r.channel_type,
            total_messages=r.total_messages, total_images=r.total_images,
            image_rate_pct=float(r.image_rate_pct or 0),
            promotional_count=int(r.promotional_count),
            product_display_count=int(r.product_display_count),
            lifestyle_count=int(r.lifestyle_count), other_count=int(r.other_count),
            avg_views_with_image=avg_with, avg_views_without_image=avg_without,
            image_views_lift_pct=lift,
        ))

    return VisualContentResponse(
        total_images_analyzed=int(total_img),
        overall_image_rate_pct=overall_rate,
        channels=channels,
    )


# ── Endpoint 5 — Detection Summary ───────────────────────────────────────────
@app.get("/api/reports/detection-summary", response_model=DetectionSummaryResponse,
         tags=["Reports"], summary="YOLO detection category breakdown",
         description="Summary of YOLOv8 detection results by image category with confidence scores.")
def detection_summary(db: Session = Depends(get_db)):
    rows = db.execute(text("""
        SELECT image_category, COUNT(DISTINCT image_path) AS count,
               ROUND(AVG(confidence_score)::NUMERIC, 3) AS avg_confidence,
               ROUND(AVG(view_count)::NUMERIC, 1) AS avg_views
        FROM public_marts.fct_image_detections
        GROUP BY image_category ORDER BY count DESC
    """)).fetchall()

    total = sum(r.count for r in rows)
    model_name = db.execute(text(
        "SELECT DISTINCT model_name FROM public_marts.fct_image_detections LIMIT 1"
    )).scalar() or "yolov8n.pt"

    return DetectionSummaryResponse(
        total_images_processed=total, model_used=model_name,
        categories=[DetectionCategoryStats(
            image_category=r.image_category, count=r.count,
            percentage=round((r.count/total)*100, 1) if total > 0 else 0,
            avg_views=float(r.avg_views or 0),
            avg_confidence=float(r.avg_confidence or 0))
            for r in rows],
    )


# ── Root ──────────────────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
def root():
    return {
        "message": "Medical Telegram Warehouse API",
        "docs": "/docs",
        "health": "/health",
        "endpoints": [
            "/api/reports/top-products",
            "/api/channels/{channel_name}/activity",
            "/api/search/messages",
            "/api/reports/visual-content",
            "/api/reports/detection-summary",
        ]
    }
