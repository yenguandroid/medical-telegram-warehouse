-- =============================================================================
-- assert_no_future_messages.sql
-- Custom data test: ensures no messages have a future timestamp.
-- Business rule: a Telegram message cannot have been posted in the future.
-- This query must return 0 rows to pass.
-- =============================================================================

SELECT
    message_id,
    channel_name,
    message_date,
    NOW() AS current_time,
    (message_date - NOW()) AS time_ahead
FROM {{ ref('stg_telegram_messages') }}
WHERE message_date > NOW()
