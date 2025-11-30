{{ config(materialized='view') }}

SELECT 
    w.rain_sum,
    COUNT(*) AS ride_count
FROM {{ ref('fact_bike_rides') }} f
JOIN {{ ref('dim_weather') }} w 
    ON f.weather_id = w.weather_id
GROUP BY w.rain_sum
ORDER BY w.rain_sum;
