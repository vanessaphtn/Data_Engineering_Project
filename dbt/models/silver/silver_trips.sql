{{ config(materialized='incremental', unique_key='ride_id', on_schema_change='append_new_columns') }}

SELECT
  ride_id,
  started_at,
  ended_at,
  start_station_id,
  end_station_id,
  member_casual AS user_type,
  bike_type,
  dateDiff('minute', started_at, ended_at) AS trip_minutes,
  toDate(started_at) AS ride_date
FROM bronze.trips_raw
WHERE ended_at >= started_at
{% if is_incremental() %}
  AND started_at > (SELECT coalesce(max(ride_date), toDate('1900-01-01')) FROM {{ this }})
{% endif %}
