{{ config(materialized='table') }}

WITH s AS (
    SELECT
        start_station_business_key AS station_business_key,
        any(start_station_name) AS station_name,
        any(start_lat) AS lat,
        any(start_lng) AS lng
    FROM {{ ref('silver_trips') }}
    GROUP BY start_station_business_key

    UNION ALL

    SELECT
        end_station_business_key AS station_business_key,
        any(end_station_name) AS station_name,
        any(end_lat) AS lat,
        any(end_lng) AS lng
    FROM {{ ref('silver_trips') }}
    GROUP BY end_station_business_key
),

distinct_stations AS (
    SELECT
        station_business_key,
        any(station_name) AS station_name,
        any(lat) AS lat,
        any(lng) AS lng
    FROM s
    GROUP BY station_business_key
),

numbered AS (
    SELECT
        row_number() OVER (ORDER BY station_business_key) AS station_id,
        station_business_key,
        station_name,
        lat,
        lng
    FROM distinct_stations
)

SELECT
    station_id,
    station_business_key AS station_business_key,
    station_name,
    lat,
    lng
FROM numbered
