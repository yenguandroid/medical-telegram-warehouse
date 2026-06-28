-- =============================================================================
-- fct_image_detections.sql
-- Fact table for YOLO object detection results.
-- Joins detection data with fct_messages, dim_channels, and dim_dates.
-- Grain: one row per (image, detected_class) pair.
-- =============================================================================

WITH detections AS (
    SELECT
        image_path,
        LOWER(TRIM(channel_name))                       AS channel_name,
        message_id::BIGINT                              AS message_id,
        LOWER(TRIM(detected_class))                     AS detected_class,
        confidence::FLOAT                               AS confidence_score,
        LOWER(TRIM(image_category))                     AS image_category,
        model_name,
        detected_at
    FROM {{ source('raw', 'yolo_detections') }}
    WHERE
        detected_class IS NOT NULL
        AND image_path IS NOT NULL
        AND confidence >= 0
),

-- Join via dim_channels to get channel_key from channel_name
channels AS (
    SELECT channel_key, channel_name, channel_type
    FROM {{ ref('dim_channels') }}
),

-- Get date_key and engagement metrics from fct_messages
messages AS (
    SELECT
        message_id,
        channel_key,
        date_key,
        view_count,
        forward_count,
        has_image
    FROM {{ ref('fct_messages') }}
),

-- Combine detections with channel_key first
detections_with_channel AS (
    SELECT
        d.*,
        c.channel_key,
        c.channel_type
    FROM detections d
    LEFT JOIN channels c
        ON d.channel_name = c.channel_name
)

SELECT
    -- Surrogate primary key
    {{ dbt_utils.generate_surrogate_key(
        ['dc.image_path', 'dc.detected_class']
    ) }}                                                AS detection_key,

    -- Message context (FK joins)
    dc.message_id,
    dc.channel_key,
    m.date_key,

    -- Detection details
    dc.image_path,
    dc.channel_name,
    dc.detected_class,
    ROUND(dc.confidence_score::NUMERIC, 4)              AS confidence_score,

    -- Image classification
    dc.image_category,

    -- Is this a meaningful detection or a "no_detection" placeholder?
    CASE WHEN dc.detected_class = 'no_detection'
         THEN FALSE ELSE TRUE
    END                                                 AS has_detection,

    -- Object type grouping
    CASE
        WHEN dc.detected_class = 'person'
            THEN 'person'
        WHEN dc.detected_class IN (
            'bottle','cup','bowl','vase','book','scissors',
            'toothbrush','hair drier','handbag','backpack',
            'suitcase','umbrella','laptop','cell phone'
        )   THEN 'product_related'
        WHEN dc.detected_class = 'no_detection'
            THEN 'no_detection'
        ELSE 'other_object'
    END                                                 AS object_group,

    -- Engagement metrics from fct_messages
    m.view_count,
    m.forward_count,

    -- Model metadata
    dc.model_name,
    dc.detected_at,

    -- Channel type
    dc.channel_type

FROM detections_with_channel dc
LEFT JOIN messages m
    ON  dc.message_id  = m.message_id
    AND dc.channel_key = m.channel_key
