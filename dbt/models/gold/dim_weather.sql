{{ config(materialized='table') }}

SELECT
    toUInt32(toYYYYMMDD(date)) AS weather_id,
    date,
    weather_code,
    temperature_mean,
    temperature_max,
    temperature_min,
    rain_sum,
    snowfall_sum,
    precipitation_hours,
    wind_speed_10m_max,
    shortwave_radiation_sum
FROM {{ ref('silver_weather') }}