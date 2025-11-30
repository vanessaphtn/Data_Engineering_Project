{{ config(materialized='view') }}

SELECT
    ut.user_type_name,
    AVG(
        6371 * 2 * ASIN(
            SQRT(
                POWER(SIN(RADIANS(st.lat - 40.750585)/2), 2) +
                COS(RADIANS(40.750585)) * COS(RADIANS(st.lat)) *
                POWER(SIN(RADIANS(st.lng - (-73.994685))/2), 2)
            )
        )
    ) AS avg_distance_km
FROM {{ ref('fact_bike_rides') }} f
JOIN {{ ref('dim_station') }} st
    ON f.end_station_id = st.station_id
JOIN {{ ref('dim_user_type') }} ut
    ON f.user_type_id = ut.user_type_id
GROUP BY ut.user_type_name;
