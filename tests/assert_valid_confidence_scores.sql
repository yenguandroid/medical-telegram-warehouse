-- =============================================================================
-- assert_valid_confidence_scores.sql
-- Custom test: confidence scores must be between 0 and 1 inclusive.
-- Business rule: YOLO confidence is a probability — it cannot exceed 1.0.
-- Returns 0 rows to pass.
-- =============================================================================

SELECT
    detection_key,
    detected_class,
    confidence_score
FROM {{ ref('fct_image_detections') }}
WHERE
    has_detection = TRUE
    AND (confidence_score < 0 OR confidence_score > 1)
