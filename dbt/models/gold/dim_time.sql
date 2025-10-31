{{ config(materialized='table') }}

WITH d AS (
    SELECT DISTINCT toDate(ride_date) AS ride_date
    FROM {{ ref('silver_trips') }}
),

holidays AS (
    SELECT DISTINCT
        date,
        is_national_holiday,
        is_new_york_holiday,
        is_weekend,
        season
    FROM {{ ref('silver_holidays') }}
),

numbered AS (
    SELECT
        row_number() OVER (ORDER BY d.ride_date) AS time_id,
        d.ride_date,
        coalesce(h.is_national_holiday, 0) AS is_national_holiday,
        coalesce(h.is_new_york_holiday, 0)  AS is_new_york_holiday,
        coalesce(h.is_weekend, 0)            AS is_weekend,
        coalesce(h.season, 'unknown')        AS season
    FROM d
    LEFT JOIN holidays h
        ON d.ride_date = h.date
)

SELECT
    time_id,
    ride_date AS date,
    toDayOfMonth(ride_date) AS day,
    toMonth(ride_date)      AS month,
    toYear(ride_date)       AS year,
    toDayOfWeek(ride_date)  AS weekday,
    is_national_holiday,
    is_new_york_holiday,
    is_weekend,
    season
FROM numbered
