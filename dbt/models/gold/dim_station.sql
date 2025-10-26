{{ config(materialized='table') }}

WITH s AS (
  SELECT
    start_station_id AS station_id,
    any(start_station_name) AS station_name,
    any(start_lat) AS lat,
    any(start_lng) AS lng
  FROM {{ ref('silver_trips') }}
  WHERE start_station_id != ''
  GROUP BY start_station_id

  UNION ALL

  SELECT
    end_station_id AS station_id,
    any(end_station_name) AS station_name,
    any(end_lat) AS lat,
    any(end_lng) AS lng
  FROM {{ ref('silver_trips') }}
  WHERE end_station_id != ''
  GROUP BY end_station_id
)
SELECT
  station_id,
  any(station_name) AS station_name,
  any(lat) AS lat,
  any(lng) AS lng
FROM s
GROUP BY station_id
