-- =============================================================================
-- fct_messages.sql
-- Central fact table — one row per Telegram message.
-- Connects to dim_channels and dim_dates via foreign keys.
-- =============================================================================

WITH messages AS (
    SELECT * FROM {{ ref('stg_telegram_messages') }}
),

channels AS (
    SELECT channel_key, channel_name
    FROM {{ ref('dim_channels') }}
),

dates AS (
    SELECT date_key, full_date
    FROM {{ ref('dim_dates') }}
)

SELECT
    -- Surrogate primary key for the fact row
    {{ dbt_utils.generate_surrogate_key(['m.message_id', 'm.channel_name']) }}
                                                AS message_key,

    -- Natural key from Telegram
    m.message_id,

    -- Foreign keys
    c.channel_key,
    d.date_key,

    -- Message content
    m.message_text,
    m.message_length,

    -- Engagement metrics
    m.view_count,
    m.forward_count,

    -- Media flags
    m.has_image,
    m.has_text,
    m.media_type,

    -- Reply context
    m.reply_to_msg_id,

    -- Full timestamp (for time-of-day analysis)
    m.message_date,

    -- Metadata
    m.scraped_at,
    m.loaded_at

FROM messages m
LEFT JOIN channels c
    ON m.channel_name = c.channel_name
LEFT JOIN dates d
    ON m.message_date_day = d.full_date
