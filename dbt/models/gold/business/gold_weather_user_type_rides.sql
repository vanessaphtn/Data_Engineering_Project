{{ config(materialized='view') }}

SELECT
    ut.user_type_name,
    w.rain_sum,
    w.snowfall_sum,
    w.temperature_max,
    COUNT(*) AS ride_count
FROM {{ ref('fact_bike_rides') }} f
JOIN {{ ref('dim_weather') }} w 
    ON f.weather_id = w.weather_id
JOIN {{ ref('dim_user_type') }} ut
    ON f.user_type_id = ut.user_type_id
GROUP BY
    ut.user_type_name,
    w.rain_sum,
    w.snowfall_sum,
    w.temperature_max
ORDER BY
    w.rain_sum,
    w.snowfall_sum,
    w.temperature_max,
    ut.user_type_name;
