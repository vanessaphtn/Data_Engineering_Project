{{ config(materialized='incremental', unique_key='date') }}

SELECT
  date,
  temperature_max,
  temperature_min,
  rain_sum,
  precipitation_hours
FROM bronze.weather_raw
{% if is_incremental() %}
WHERE date > (SELECT coalesce(max(date), toDate('1900-01-01')) FROM {{ this }})
{% endif %}
