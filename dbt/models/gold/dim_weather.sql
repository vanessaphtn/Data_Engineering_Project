{{ config(materialized='table') }}

SELECT
  toUInt32(toYYYYMMDD(date)) AS weather_id,
  date,
  temperature_max,
  temperature_min,
  rain_sum,
  precipitation_hours
FROM {{ ref('silver_weather') }}