{{ config(materialized='view') }}

SELECT 
    w.temperature_max,
    w.temperature_min,
    COUNT(*) AS ride_count
FROM {{ ref('fact_bike_rides') }} f
JOIN {{ ref('dim_weather') }} w 
    ON f.weather_id = w.weather_id
GROUP BY w.temperature_max, w.temperature_min
ORDER BY w.temperature_max;
