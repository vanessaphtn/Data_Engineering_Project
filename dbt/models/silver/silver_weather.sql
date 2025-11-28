{{ config(
    materialized='incremental',
    unique_key='date'
) }}

WITH src AS (
    SELECT
        toDate(date)                              AS date,
        CAST(temperature_max, 'Float32')          AS temperature_max,
        CAST(temperature_min, 'Float32')          AS temperature_min,
        CAST(temperature_mean, 'Float32')         AS temperature_mean,
        CAST(rain_sum, 'Float32')                 AS rain_sum,
        CAST(snowfall_sum, 'Float32')             AS snowfall_sum,
        CAST(precipitation_hours, 'UInt8')        AS precipitation_hours,
        CAST(wind_speed_max, 'Float32')           AS wind_speed_max,
        CAST(shortwave_radiation_sum, 'Float32')  AS shortwave_radiation_sum,
        CAST(weather_code, 'Int16')               AS weather_code
    FROM bronze.weather_raw
),

daily AS (
  SELECT
    date,
    anyLast(temperature_max)          AS temperature_max,
    anyLast(temperature_min)          AS temperature_min,
    anyLast(temperature_mean)         AS temperature_mean,
    anyLast(rain_sum)                  AS rain_sum,
    anyLast(snowfall_sum)              AS snowfall_sum,
    anyLast(precipitation_hours)       AS precipitation_hours,
    anyLast(wind_speed_max)            AS wind_speed_max,
    anyLast(shortwave_radiation_sum)   AS shortwave_radiation_sum,
    anyLast(weather_code)              AS weather_code
  FROM src
  GROUP BY date
)

SELECT *
FROM daily
{% if is_incremental() %}
WHERE date > (SELECT coalesce(max(date), toDate('1900-01-01')) FROM {{ this }})
{% endif %}
