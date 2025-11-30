{{ config(materialized='view') }}

SELECT
    ut.user_type_name,
    AVG(f.trip_minutes) AS avg_trip_minutes
FROM {{ ref('fact_bike_rides') }} f
JOIN {{ ref('dim_user_type') }} ut
    ON f.user_type_id = ut.user_type_id
GROUP BY ut.user_type_name;
