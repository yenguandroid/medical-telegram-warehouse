-- =============================================================================
-- dim_channels.sql
-- Channel dimension with surrogate key, classification, and aggregate stats.
-- Grain: one row per unique Telegram channel.
-- =============================================================================

WITH channel_stats AS (
    SELECT
        channel_name,
        channel_type,
        MIN(message_date)                           AS first_post_date,
        MAX(message_date)                           AS last_post_date,
        COUNT(*)                                    AS total_posts,
        ROUND(AVG(view_count), 2)                   AS avg_views,
        ROUND(AVG(forward_count), 2)                AS avg_forwards,
        SUM(CASE WHEN has_image THEN 1 ELSE 0 END)  AS total_images,
        SUM(CASE WHEN has_text  THEN 1 ELSE 0 END)  AS total_text_posts
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_name, channel_type
)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['channel_name']) }}    AS channel_key,

    -- Natural key
    channel_name,

    -- Descriptive attributes
    channel_type,

    -- Display-friendly name
    INITCAP(REPLACE(channel_name, '_', ' '))                    AS channel_display_name,

    -- Temporal bounds
    first_post_date::DATE                                       AS first_post_date,
    last_post_date::DATE                                        AS last_post_date,
    (last_post_date::DATE - first_post_date::DATE)              AS active_days,

    -- Aggregate metrics
    total_posts,
    avg_views,
    avg_forwards,
    total_images,
    total_text_posts,

    -- Metadata
    NOW()                                                       AS dbt_updated_at

FROM channel_stats
