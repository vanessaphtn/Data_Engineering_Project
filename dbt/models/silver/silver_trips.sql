{{ config(materialized='incremental', unique_key='ride_id', on_schema_change='append_new_columns') }}

WITH base AS (
  SELECT
    ride_id,
    started_at,
    ended_at,
    start_station_id,
    start_station_name,
    start_lat,
    start_lng,
    end_station_id,
    end_station_name,
    end_lat,
    end_lng,
    member_casual AS user_type,
    bike_type,
    dateDiff('minute', started_at, ended_at) AS trip_minutes,
    toDate(started_at) AS ride_date,
    _ingested_at
  FROM bronze.trips_raw
  WHERE ended_at >= started_at
),
dedup AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY ride_id ORDER BY _ingested_at DESC) AS rn
  FROM base
)
SELECT
  ride_id,
  started_at,
  ended_at,
  start_station_id,
  start_station_name,
  start_lat,
  start_lng,
  end_station_id,
  end_station_name,
  end_lat,
  end_lng,
  user_type,
  bike_type,
  trip_minutes,
  ride_date
FROM dedup
WHERE rn = 1
{% if is_incremental() %}
  AND ride_date > (SELECT coalesce(max(ride_date), toDate('1900-01-01')) FROM {{ this }})
{% endif %}
