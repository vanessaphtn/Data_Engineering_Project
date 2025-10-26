{{ config(materialized='incremental', on_schema_change='append_new_columns') }}

WITH base AS (
  SELECT
    toUInt32(toYYYYMMDD(ride_date)) AS time_id,
    start_station_id                AS station_id,
    user_type,
    bike_type,
    count()                         AS ride_count,
    avg(trip_minutes)               AS avg_trip_minutes
  FROM {{ ref('silver_trips') }}
  GROUP BY time_id, station_id, user_type, bike_type
)
SELECT
  b.time_id,
  toUInt32(b.time_id) AS weather_id, -- align day to dim_weather PK
  b.station_id,
  b.user_type,
  b.bike_type,
  b.ride_count,
  b.avg_trip_minutes
FROM base b
{% if is_incremental() %}
WHERE b.time_id > (SELECT coalesce(max(time_id),0) FROM {{ this }})
{% endif %}
