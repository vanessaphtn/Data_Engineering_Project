{{ config(materialized='incremental', unique_key='ride_id', on_schema_change='append_new_columns') }}

WITH base AS (
    SELECT
        s.ride_id,
        d.time_id           AS start_date_id,      -- from dim_date
        st.station_id       AS end_station_id,   -- from dim_station
        ut.user_type_id,                            -- from dim_user_type
        bt.bike_type_id,                            -- from dim_bike_type
        w.weather_id,                               -- from dim_weather
        s.trip_minutes,
        s.started_at,
        s.ended_at
    FROM {{ ref('silver_trips') }} s

    -- Join to dim_date
    JOIN {{ ref('dim_time') }} d
        ON toDate(s.started_at) = d.date

    -- Join to dim_station
    JOIN {{ ref('dim_station') }} st
        ON s.end_station_business_key = st.station_business_key

    -- Join to dim_user_type
    JOIN {{ ref('dim_user_type') }} ut
        ON s.user_type = ut.user_type_name

    -- Join to dim_bike_type
    JOIN {{ ref('dim_bike_type') }} bt
        ON s.bike_type = bt.bike_type_name

    -- Join to dim_weather
    LEFT JOIN {{ ref('dim_weather') }} w
        ON toDate(s.started_at) = w.date   -- ensures correct weather per day

    WHERE s.ended_at >= s.started_at
)

SELECT
    b.ride_id,
    b.start_date_id,
    b.end_station_id,
    b.user_type_id,
    b.bike_type_id,
    b.weather_id,
    b.trip_minutes,
    b.started_at,
    b.ended_at
FROM base b
{% if is_incremental() %}
WHERE b.start_date_id > (SELECT coalesce(max(start_date_id), 0) FROM {{ this }})
{% endif %}
