{{ config(materialized='table') }}

WITH d AS (
  SELECT DISTINCT toDate(ride_date) AS ride_date
  FROM {{ ref('silver_trips') }}
),

numbered AS (
    SELECT
        row_number() OVER (ORDER BY ride_date) AS time_id, 
        ride_date
    FROM d
)

SELECT
    time_id, 
    ride_date AS date, 
    toDayOfMonth(ride_date)  AS day,
    toMonth(ride_date)       AS month,
    toYear(ride_date)        AS year,
    toDayOfWeek(ride_date)   AS weekday
FROM numbered