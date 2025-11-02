-- How do daily temperatures influence the number of rides taken from the station? (daily ride count)
SELECT 
    dim_weather.temperature_max,
    dim_weather.temperature_min,
    COUNT(*) AS ride_count
FROM fact_bike_ride
JOIN dim_weather ON fact_bike_ride.weather_id = dim_weather.weather_id
GROUP BY dim_weather.temperature_max, dim_weather.temperature_min
ORDER BY dim_weather.temperature_max;


-- How do precipitation and snowfall affect daily ride counts? (daily ride count)
SELECT 
    dim_weather.rain_sum,
    COUNT(*) AS ride_count
FROM fact_bike_ride
JOIN dim_weather ON fact_bike_ride.weather_id = dim_weather.weather_id
GROUP BY dim_weather.rain_sum
ORDER BY dim_weather.rain_sum;

-- Do members or casual riders contribute more to overall ride volume under different weather conditions? (daily ride count)
SELECT
    dim_user_type.user_type_name,
    dim_weather.rain_sum,
    dim_weather.snowfall_sum,
    dim_weather.temperature_max,
    COUNT(*) AS ride_count
FROM fact_bike_ride
JOIN dim_weather
    ON fact_bike_ride.weather_id = dim_weather.weather_id
JOIN dim_user_type
    ON fact_bike_ride.user_type_id = dim_user_type.user_type_id
GROUP BY
    dim_user_type.user_type_name,
    dim_weather.rain_sum,
    dim_weather.snowfall_sum,
    dim_weather.temperature_max
ORDER BY
    dim_weather.rain_sum,
    dim_weather.snowfall_sum,
    dim_weather.temperature_max,
    dim_user_type.user_type_name;


--Do members or casual riders take longer trips in terms of duration? (trip duration, rider composition)
SELECT
    dim_user_type.user_type_name,
    AVG(fact_bike_ride.trip_minutes)
FROM fact_bike_ride
JOIN dim_user_type
    ON fact_bike_ride.user_type_id = dim_user_type.user_type_id
GROUP BY dim_user_type.user_type_name;


-- Do members or casual riders travel farther in terms of distance? (trip distance, rider composition)
SELECT
    dim_user_type.user_type_name,
    AVG(
        6371 * 2 * ASIN(
            SQRT(
                POWER(SIN(RADIANS(dim_stations_2.lat - 40.750585)/2), 2) +
                COS(RADIANS(40.750585)) * COS(RADIANS(dim_stations_2.lat)) *
                POWER(SIN(RADIANS(dim_stations_2.lng - (-73.994685))/2), 2)
            )
        )
    ) AS avg_distance_km
FROM fact_bike_ride
JOIN dim_station AS dim_stations_2
    ON fact_bike_ride.end_station_id = dim_stations_2.station_id
JOIN dim_user_type
    ON fact_bike_ride.user_type_id = dim_user_type.user_type_id
GROUP BY dim_user_type.user_type_name;
