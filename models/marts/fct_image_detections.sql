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

messages AS (
    SELECT
        message_id,
        channel_name,
        channel_key,
        date_key,
        view_count,
        forward_count,
        has_image
    FROM {{ ref('fct_messages') }}
),

channels AS (
    SELECT channel_key, channel_name, channel_type
    FROM {{ ref('dim_channels') }}
)

SELECT
    -- Surrogate primary key
    {{ dbt_utils.generate_surrogate_key(
        ['d.image_path', 'd.detected_class']
    ) }}                                                AS detection_key,

    -- Message context (FK joins)
    d.message_id,
    m.channel_key,
    m.date_key,

    -- Detection details
    d.image_path,
    d.channel_name,
    d.detected_class,
    ROUND(d.confidence_score::NUMERIC, 4)              AS confidence_score,

    -- Image classification
    d.image_category,

    -- Is this a meaningful detection or a "no_detection" placeholder?
    CASE WHEN d.detected_class = 'no_detection'
         THEN FALSE ELSE TRUE
    END                                                 AS has_detection,

    -- Object type grouping
    CASE
        WHEN d.detected_class = 'person'
            THEN 'person'
        WHEN d.detected_class IN (
            'bottle','cup','bowl','vase','book','scissors',
            'toothbrush','hair drier','handbag','backpack',
            'suitcase','umbrella','laptop','cell phone'
        )   THEN 'product_related'
        WHEN d.detected_class = 'no_detection'
            THEN 'no_detection'
        ELSE 'other_object'
    END                                                 AS object_group,

    -- Engagement metrics from fct_messages (for analysis)
    m.view_count,
    m.forward_count,

    -- Model metadata
    d.model_name,
    d.detected_at,

    -- Channel type for easy filtering
    c.channel_type

FROM detections d
LEFT JOIN messages m
    ON  d.message_id   = m.message_id
    AND d.channel_name = m.channel_name
LEFT JOIN channels c
    ON m.channel_key = c.channel_key
