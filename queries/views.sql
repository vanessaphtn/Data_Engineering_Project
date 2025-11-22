---- FULL ACCESS VIEWS ----
-- 1. full info about the bike ride
CREATE OR REPLACE VIEW default_gold.v_bike_ride_full AS
SELECT 
    br.ride_id,
    br.trip_minutes,
    br.started_at,
    br.ended_at,
    dt.date AS ride_date,
    dt.season,
    dt.is_weekend,
    dt.weekday,
    dt.is_national_holiday,
    dt.is_new_york_holiday,
    wt.temperature_mean,
    wt.temperature_max,
    wt.temperature_min,
    wt.rain_sum,
    wt.snowfall_sum,
    wt.precipitation_hours,
    wt.wind_speed_10m_max,
    wt.shortwave_radiation_sum,
    ut.user_type_name,
    st.station_name AS end_station_name,
    st.lat,
    st.lng,
    bt.bike_type_name 

FROM default_gold.fact_bike_ride br
LEFT JOIN default_gold.dim_weather wt ON wt.weather_id = br.weather_id
LEFT JOIN default_gold.dim_user_type ut ON ut.user_type_id = br.user_type_id
LEFT JOIN default_gold.dim_time dt ON dt.time_id = br.start_date_id
LEFT JOIN default_gold.dim_station st ON st.station_id = br.end_station_id
LEFT JOIN default_gold.dim_bike_type bt ON bt.bike_type_id = br.bike_type_id;

-- 2. Are there consistent differences in usage between weekdays and weekends?
CREATE OR REPLACE VIEW default_gold.v_usage_daily_full AS
SELECT
    COUNT(ride_id) AS total_rides,
    AVG(trip_minutes) AS avg_trip_minutes,
    weekday AS day_number,
    CASE weekday
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_name
FROM default_gold.v_bike_ride_full
GROUP BY day_name, day_number
ORDER BY day_number;




---- MASKED TABLES ----
CREATE OR REPLACE TABLE default_gold.fact_bike_ride_masked 
ENGINE = MergeTree()
ORDER BY ride_id_masked AS
SELECT
    concat('***', right(br.ride_id, 3)) AS ride_id_masked,
    br.trip_minutes,
    br.weather_id,
    br.start_date_id,
    br.user_type_id,
    br.bike_type_id,
    br.end_station_id
FROM default_gold.fact_bike_ride br;

CREATE OR REPLACE TABLE default_gold.dim_station_masked
ENGINE = MergeTree()
ORDER BY station_id AS
SELECT
    s.station_id,
    s.station_name,
    ROUND(s.lat, 2) AS lat_approx,   
    ROUND(s.lng, 2) AS lng_approx
FROM default_gold.dim_station s;

CREATE OR REPLACE TABLE default_gold.dim_time_masked
ENGINE = MergeTree()
ORDER BY time_id
AS
SELECT
    t.time_id,
    t.date AS ride_date,
    IF(t.is_weekend = 1, 'Weekend', 'Weekday') AS day_type,
    t.is_national_holiday,
    t.is_new_york_holiday,
    t.season
FROM default_gold.dim_time t;



---- LIMITED ACCESS VIEWS ----
-- 1. full info about the bike ride (limited version)
CREATE OR REPLACE VIEW default_gold.v_bike_ride_limited AS
SELECT 
    br.ride_id_masked,
    br.trip_minutes,
    dt.ride_date,
    dt.season,
    dt.day_type,
    dt.is_national_holiday,
    dt.is_new_york_holiday,
    wt.temperature_mean,
    wt.temperature_max,
    wt.temperature_min,
    wt.rain_sum,
    wt.snowfall_sum,
    wt.precipitation_hours,
    wt.wind_speed_10m_max,
    wt.shortwave_radiation_sum,
    ut.user_type_name,
    st.station_name AS end_station_name,
    st.lat_approx,   
    st.lng_approx,
    bt.bike_type_name 

FROM default_gold.fact_bike_ride_masked br
LEFT JOIN default_gold.dim_weather wt ON wt.weather_id = br.weather_id
LEFT JOIN default_gold.dim_user_type ut ON ut.user_type_id = br.user_type_id
LEFT JOIN default_gold.dim_time_masked dt ON dt.time_id = br.start_date_id
LEFT JOIN default_gold.dim_station_masked st ON st.station_id = br.end_station_id
LEFT JOIN default_gold.dim_bike_type bt ON bt.bike_type_id = br.bike_type_id;

-- 2. Are there consistent differences in usage between weekdays and weekends?
CREATE OR REPLACE VIEW default_gold.v_usage_daily_limited AS
SELECT
    COUNT(ride_id_masked) AS total_rides,
    AVG(trip_minutes) AS avg_trip_minutes,
    day_type
FROM default_gold.v_bike_ride_limited
GROUP BY day_type
ORDER BY day_type;



---- GIVING RIGHTS TO ROLES ----
-- full rigths 
GRANT SELECT ON default_gold.* TO analyst_full;
GRANT SELECT ON default_gold.v_bike_ride_full TO analyst_full;
GRANT SELECT ON default_gold.v_usage_daily_full TO analyst_full;
-- limited rights 
GRANT SELECT ON default_gold.fact_bike_ride_masked TO analyst_limited;
GRANT SELECT ON default_gold.dim_station_masked TO analyst_limited;
GRANT SELECT ON default_gold.dim_time_masked TO analyst_limited;
GRANT SELECT ON default_gold.dim_weather TO analyst_limited;
GRANT SELECT ON default_gold.dim_user_type TO analyst_limited;
GRANT SELECT ON default_gold.dim_bike_type TO analyst_limited;

GRANT SELECT ON default_gold.v_bike_ride_limited TO analyst_limited;
GRANT SELECT ON default_gold.v_usage_daily_limited TO analyst_limited;