"""
api/schemas.py
Pydantic models for request validation and response serialization.
"""

from datetime import date, datetime
from typing import Optional
from pydantic import BaseModel, Field


# ─────────────────────────────────────────────
# Shared / base schemas
# ─────────────────────────────────────────────
class PaginationParams(BaseModel):
    limit: int = Field(default=10, ge=1, le=100, description="Number of results to return (1-100)")
    offset: int = Field(default=0, ge=0, description="Number of results to skip")


# ─────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────
class HealthResponse(BaseModel):
    status: str
    database: str
    version: str = "1.0.0"
    description: str = "Medical Telegram Warehouse API"


# ─────────────────────────────────────────────
# Endpoint 1 — Top Products
# ─────────────────────────────────────────────
class TopProductItem(BaseModel):
    term: str = Field(description="Frequently mentioned term or product name")
    mention_count: int = Field(description="Number of messages containing this term")
    channel_count: int = Field(description="Number of unique channels mentioning this term")
    avg_views: float = Field(description="Average view count of messages mentioning this term")

    class Config:
        from_attributes = True


class TopProductsResponse(BaseModel):
    total_results: int
    limit: int
    data: list[TopProductItem]


# ─────────────────────────────────────────────
# Endpoint 2 — Channel Activity
# ─────────────────────────────────────────────
class DailyActivity(BaseModel):
    activity_date: date = Field(description="Date of posting activity")
    message_count: int = Field(description="Number of messages posted on this date")
    total_views: int = Field(description="Total views across all messages on this date")
    image_count: int = Field(description="Number of image posts on this date")

    class Config:
        from_attributes = True


class ChannelActivityResponse(BaseModel):
    channel_name: str
    channel_type: str
    total_posts: int
    total_views: int
    avg_views_per_post: float
    avg_forwards_per_post: float
    first_post_date: date
    last_post_date: date
    image_rate_pct: float = Field(description="Percentage of posts that contain images")
    daily_activity: list[DailyActivity]


# ─────────────────────────────────────────────
# Endpoint 3 — Message Search
# ─────────────────────────────────────────────
class MessageSearchItem(BaseModel):
    message_id: int
    channel_name: str
    message_date: datetime
    message_text: str
    view_count: int
    forward_count: int
    has_image: bool
    channel_type: str

    class Config:
        from_attributes = True


class MessageSearchResponse(BaseModel):
    query: str
    total_results: int
    limit: int
    data: list[MessageSearchItem]


# ─────────────────────────────────────────────
# Endpoint 4 — Visual Content Stats
# ─────────────────────────────────────────────
class ChannelVisualStats(BaseModel):
    channel_name: str
    channel_type: str
    total_messages: int
    total_images: int
    image_rate_pct: float
    promotional_count: int
    product_display_count: int
    lifestyle_count: int
    other_count: int
    avg_views_with_image: float
    avg_views_without_image: float
    image_views_lift_pct: float = Field(
        description="% increase in views for posts with images vs without"
    )

    class Config:
        from_attributes = True


class VisualContentResponse(BaseModel):
    total_images_analyzed: int
    overall_image_rate_pct: float
    channels: list[ChannelVisualStats]


# ─────────────────────────────────────────────
# Endpoint 5 — Detection Summary (bonus)
# ─────────────────────────────────────────────
class DetectionCategoryStats(BaseModel):
    image_category: str
    count: int
    percentage: float
    avg_views: float
    avg_confidence: float

    class Config:
        from_attributes = True


class DetectionSummaryResponse(BaseModel):
    total_images_processed: int
    model_used: str
    categories: list[DetectionCategoryStats]


# ─────────────────────────────────────────────
# Error response
# ─────────────────────────────────────────────
class ErrorResponse(BaseModel):
    error: str
    detail: str
    status_code: int
