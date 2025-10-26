{{ config(materialized='incremental', unique_key='date') }}

WITH base AS (
  SELECT
    date,
    temperature_max,
    temperature_min,
    rain_sum,
    precipitation_hours
  FROM bronze.weather_raw
),
daily AS (
  SELECT
    date,
    any(temperature_max) AS temperature_max,
    any(temperature_min) AS temperature_min,
    any(rain_sum)        AS rain_sum,
    any(precipitation_hours) AS precipitation_hours
  FROM base
  GROUP BY date
)
SELECT *
FROM daily
{% if is_incremental() %}
WHERE date > (SELECT coalesce(max(date), toDate('1900-01-01')) FROM {{ this }})
{% endif %}
