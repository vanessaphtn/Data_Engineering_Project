{{ config(materialized='table') }}

WITH base AS (
  SELECT DISTINCT
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
),

numbered AS (
    SELECT
        row_number() OVER (ORDER BY date) AS weather_id,*
    FROM base
)

SELECT
    weather_id,
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
FROM numbered