{{ config(materialized='table') }}

WITH base AS (
    SELECT
        toDate(date) AS date,
        CAST(is_national_holiday, 'UInt8') AS is_national_holiday,
        CAST(is_new_york_holiday, 'UInt8') AS is_new_york_holiday
    FROM bronze.holidays_raw
),

deduped AS (
    SELECT
        date,
        max(is_national_holiday) AS is_national_holiday,
        max(is_new_york_holiday) AS is_new_york_holiday
    FROM base
    GROUP BY date
)

SELECT
    date,
    is_national_holiday,
    is_new_york_holiday,
    (toDayOfWeek(date) IN (6, 7)) AS is_weekend,
    multiIf(
        toMonth(date) IN (12, 1, 2), 'winter',
        toMonth(date) IN (3, 4, 5),  'spring',
        toMonth(date) IN (6, 7, 8),  'summer',
        toMonth(date) IN (9, 10, 11), 'autumn',
        'unknown'
    ) AS season
FROM deduped
