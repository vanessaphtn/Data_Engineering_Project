{{ config(materialized='incremental', unique_key='ride_id', on_schema_change='append_new_columns') }}

WITH src AS (
  SELECT
    CAST(ride_id, 'String')                                     AS ride_id,
    started_at                                                  AS started_at_ts,   -- juba DateTime
    ended_at                                                    AS ended_at_ts,     -- juba DateTime
    CAST(start_station_id, 'String')                            AS start_station_business_key,
    CAST(end_station_id,   'String')                            AS end_station_business_key,
    CAST(start_station_name, 'String')                          AS start_station_name,
    CAST(end_station_name,   'String')                          AS end_station_name,
    CAST(start_lat, 'Float64')                                  AS start_lat,
    CAST(start_lng, 'Float64')                                  AS start_lng,
    CAST(end_lat,   'Float64')                                  AS end_lat,
    CAST(end_lng,   'Float64')                                  AS end_lng,
    lowerUTF8(CAST(member_casual, 'String'))                    AS user_type,
    lowerUTF8(CAST(bike_type,      'String'))                   AS bike_type,
    toDate(started_at)                                          AS ride_date,
    _ingested_at                                                AS _ingested_at     -- võib olla juba DateTime
  FROM bronze.trips_raw
  WHERE ended_at IS NOT NULL
    AND started_at IS NOT NULL
    AND ended_at >= started_at
),

dedup AS (
  SELECT
    *,
    row_number() OVER (PARTITION BY ride_id ORDER BY _ingested_at DESC) AS rn
  FROM src
)

SELECT
  ride_id,
  started_at_ts AS started_at,
  ended_at_ts   AS ended_at,
  start_station_business_key,
  start_station_name,
  start_lat,
  start_lng,
  end_station_business_key,
  end_station_name,
  end_lat,
  end_lng,
  user_type,
  bike_type,
  dateDiff('minute', started_at_ts, ended_at_ts) AS trip_minutes,
  ride_date
FROM dedup
WHERE rn = 1

{% if is_incremental() %}
  AND ride_date > (SELECT coalesce(max(ride_date), toDate('1900-01-01')) FROM {{ this }})
{% endif %}
