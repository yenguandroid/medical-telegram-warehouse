-- =============================================================================
-- stg_telegram_messages.sql
-- Cleans and standardizes raw Telegram messages from raw.telegram_messages.
-- Casts types, renames columns, removes invalid records, adds derived fields.
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'telegram_messages') }}
),

cleaned AS (
    SELECT
        -- ── Primary identifiers ─────────────────────────────────────────────
        message_id::BIGINT                          AS message_id,
        LOWER(TRIM(channel_name))                   AS channel_name,

        -- ── Timestamps ──────────────────────────────────────────────────────
        message_date::TIMESTAMPTZ                   AS message_date,
        message_date::DATE                          AS message_date_day,

        -- ── Text content ────────────────────────────────────────────────────
        TRIM(message_text)                          AS message_text,
        LENGTH(TRIM(COALESCE(message_text, '')))    AS message_length,

        -- ── Engagement metrics ───────────────────────────────────────────────
        COALESCE(views, 0)::INTEGER                 AS view_count,
        COALESCE(forwards, 0)::INTEGER              AS forward_count,

        -- ── Media flags ─────────────────────────────────────────────────────
        COALESCE(has_photo, FALSE)::BOOLEAN         AS has_image,
        COALESCE(media_type, 'none')                AS media_type,
        image_local_path,

        -- ── Reply context ───────────────────────────────────────────────────
        reply_to_msg_id::BIGINT                     AS reply_to_msg_id,

        -- ── Channel classification ───────────────────────────────────────────
        CASE
            WHEN LOWER(channel_name) IN ('tikvahpharma', 'chemed123', 'ethiopianpharmacists')
                THEN 'Pharmaceutical'
            WHEN LOWER(channel_name) IN ('lobelia4cosmetics')
                THEN 'Cosmetics'
            ELSE 'Medical'
        END                                         AS channel_type,

        -- ── Derived content flags ────────────────────────────────────────────
        CASE
            WHEN LENGTH(TRIM(COALESCE(message_text, ''))) > 0
            THEN TRUE ELSE FALSE
        END                                         AS has_text,

        -- ── Metadata ────────────────────────────────────────────────────────
        scraped_at::TIMESTAMPTZ                     AS scraped_at,
        loaded_at::TIMESTAMPTZ                      AS loaded_at

    FROM source
    WHERE
        -- Remove records with no message ID
        message_id IS NOT NULL
        -- Remove records with no date
        AND message_date IS NOT NULL
        -- Remove future-dated messages (data quality guard)
        AND message_date <= NOW()
        -- Remove records with no channel name
        AND channel_name IS NOT NULL
        AND TRIM(channel_name) != ''
        -- Remove records with negative views (invalid)
        AND COALESCE(views, 0) >= 0
)

SELECT * FROM cleaned
