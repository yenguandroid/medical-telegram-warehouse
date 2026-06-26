-- =============================================================================
-- assert_positive_views.sql
-- Custom data test: ensures all view counts are non-negative.
-- Business rule: a message cannot have a negative view count.
-- This query must return 0 rows to pass.
-- =============================================================================

SELECT
    message_id,
    channel_name,
    view_count
FROM {{ ref('stg_telegram_messages') }}
WHERE view_count < 0
