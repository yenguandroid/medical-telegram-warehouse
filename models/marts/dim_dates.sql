-- =============================================================================
-- dim_dates.sql
-- Date dimension spanning the full range of dates in the message data.
-- Grain: one row per calendar date.
-- =============================================================================

WITH date_spine AS (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2020-01-01' as date)",
            end_date="cast(current_date + interval '1 year' as date)"
        )
    }}
),

dates AS (
    SELECT
        date_day::DATE AS full_date
    FROM date_spine
    -- Only keep dates that appear in our actual data (± buffer)
    WHERE date_day::DATE BETWEEN (
        SELECT MIN(message_date_day) - INTERVAL '1 day'
        FROM {{ ref('stg_telegram_messages') }}
    ) AND (
        SELECT MAX(message_date_day) + INTERVAL '30 days'
        FROM {{ ref('stg_telegram_messages') }}
    )
)

SELECT
    -- Surrogate key (integer YYYYMMDD — fast for joining)
    TO_CHAR(full_date, 'YYYYMMDD')::INTEGER         AS date_key,

    -- The date itself
    full_date,

    -- Day-level attributes
    EXTRACT(DAY FROM full_date)::INTEGER            AS day_of_month,
    EXTRACT(DOW FROM full_date)::INTEGER            AS day_of_week,      -- 0=Sunday
    TO_CHAR(full_date, 'Day')                       AS day_name,
    TO_CHAR(full_date, 'Dy')                        AS day_name_short,

    -- Week
    EXTRACT(WEEK FROM full_date)::INTEGER           AS week_of_year,
    DATE_TRUNC('week', full_date)::DATE             AS week_start_date,

    -- Month
    EXTRACT(MONTH FROM full_date)::INTEGER          AS month_number,
    TO_CHAR(full_date, 'Month')                     AS month_name,
    TO_CHAR(full_date, 'Mon')                       AS month_name_short,
    DATE_TRUNC('month', full_date)::DATE            AS month_start_date,

    -- Quarter
    EXTRACT(QUARTER FROM full_date)::INTEGER        AS quarter_number,
    'Q' || EXTRACT(QUARTER FROM full_date)          AS quarter_name,
    DATE_TRUNC('quarter', full_date)::DATE          AS quarter_start_date,

    -- Year
    EXTRACT(YEAR FROM full_date)::INTEGER           AS year_number,

    -- Boolean flags
    CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6)
         THEN TRUE ELSE FALSE END                   AS is_weekend,
    CASE WHEN EXTRACT(DOW FROM full_date) NOT IN (0, 6)
         THEN TRUE ELSE FALSE END                   AS is_weekday,
    CASE WHEN full_date = CURRENT_DATE
         THEN TRUE ELSE FALSE END                   AS is_today

FROM dates
ORDER BY full_date
