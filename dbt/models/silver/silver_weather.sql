{{ config(
    materialized='incremental',
    unique_key='date'
) }}

WITH base AS (
    SELECT
        date,
        temperature_2m_max, 
        temperature_2m_min,
        temperature_2m_mean ,
        rain_sum,
        snowfall_sum,
        precipitation_hours,
        wind_speed_10m_max,
        shortwave_radiation_sum,
        weather_code
    FROM bronze.weather_raw
),

daily AS (
  SELECT
    date,
    any(temperature_2m_max) AS temperature_max,
    any(temperature_2m_min) AS temperature_min,
    any(temperature_2m_mean) AS temperature_mean,
    any(rain_sum)        AS rain_sum,
    any(snowfall_sum)    AS snowfall_sum,
    any(precipitation_hours) AS precipitation_hours,
    any(wind_speed_10m_max) AS wind_speed_10m_max,
    any(shortwave_radiation_sum) AS shortwave_radiation_sum,
    any(weather_code) AS weather_code
  FROM base
  GROUP BY date
)
SELECT *
FROM daily
{% if is_incremental() %}
WHERE date > (SELECT coalesce(max(date), toDateTime('1900-01-01 00:00:00')) FROM {{ this }})
{% endif %}

