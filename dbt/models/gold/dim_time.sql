{{ config(materialized='table') }}

WITH d AS (
  SELECT DISTINCT toDate(ride_date) AS d
  FROM {{ ref('silver_trips') }}
)
SELECT
  toUInt32(toYYYYMMDD(d)) AS time_id,
  d AS date,
  toDayOfMonth(d) AS day,
  toMonth(d)      AS month,
  toYear(d)       AS year,
  toDayOfWeek(d)  AS weekday
FROM d