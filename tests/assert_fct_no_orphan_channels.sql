-- =============================================================================
-- assert_fct_no_orphan_channels.sql
-- Custom data test: ensures every fact row has a matching channel dimension.
-- Business rule: every message must belong to a known channel.
-- This query must return 0 rows to pass.
-- =============================================================================

SELECT
    f.message_key,
    f.message_id,
    f.channel_key
FROM {{ ref('fct_messages') }} f
LEFT JOIN {{ ref('dim_channels') }} c
    ON f.channel_key = c.channel_key
WHERE c.channel_key IS NULL
