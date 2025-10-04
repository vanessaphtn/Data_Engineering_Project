
-- How do daily temperatures influence the number of rides taken from the station? (daily ride count)
SELECT 
    DIM_WEATHER.Temperature_max,
    DIM_WEATHER.Temperature_min,
    COUNT(*) AS ride_count
FROM FACT_BIKE_RIDE
JOIN DIM_WEATHER ON FACT_BIKE_RIDE.Weather = DIM_WEATHER.Weather_ID
GROUP BY DIM_WEATHER.Temperature_max, DIM_WEATHER.Temperature_min
ORDER BY DIM_WEATHER.Temperature_max;


-- How do precipitation and snowfall affect daily ride counts? (daily ride count)
SELECT 
    DIM_WEATHER.Rain_sum, -- DIM_WEATHER.Snowfall_sum
    COUNT(*) AS ride_count
FROM FACT_BIKE_RIDE
JOIN DIM_WEATHER ON FACT_BIKE_RIDE.Weather = DIM_WEATHER.Weather_ID
GROUP BY DIM_WEATHER.Rain_sum -- DIM_WEATHER.Snowfall_sum
ORDER BY DIM_WEATHER.Rain_sum; ---- DIM_WEATHER.Snowfall_sum;


-- Do members or casual riders contribute more to overall ride volume under different weather conditions? (daily ride count)
FACT_BIKE_RIDE.User_type,
    DIM_WEATHER.Rain_sum, -- Othwer weather parameters cab be included.
    DIM_WEATHER.Snowfall_sum,
    DIM_WEATHER.Temperature_max,
    COUNT(*) AS ride_count
FROM FACT_BIKE_RIDE
JOIN DIM_WEATHER ON FACT_BIKE_RIDE.Weather = DIM_WEATHER.Weather_ID
GROUP BY FACT_BIKE_RIDE.User_type, DIM_WEATHER.Rain_sum, DIM_WEATHER.Snowfall_sum, DIM_WEATHER.Temperature_max
ORDER BY DIM_WEATHER.Rain_sum, DIM_WEATHER.Snowfall_sum, DIM_WEATHER.Temperature_max, FACT_BIKE_RIDE.User_type;


--Do members or casual riders take longer trips in terms of duration? (trip duration, rider composition)
SELECT 
    FACT_BIKE_RIDE.User_type,
    AVG(EXTRACT(EPOCH FROM (FACT_BIKE_RIDE.End_timestamp - FACT_BIKE_RIDE.Start_timestamp))/60) AS avg_trip_duration_minutes
FROM FACT_BIKE_RIDE
GROUP BY FACT_BIKE_RIDE.User_type;


-- Do members or casual riders travel farther in terms of distance? (trip distance, rider composition)
SELECT 
    FACT_BIKE_RIDE.User_type,
    AVG(
        6371 * 2 * ASIN(
            SQRT(
                POWER(SIN(RADIANS(DIM_STATIONS_2.Latitude - 40.750585)/2),2) + -- Coordinates of Ave & W 31 St station
                COS(RADIANS(40.750580)) * COS(RADIANS(DIM_STATIONS_2.Latitude)) *
                POWER(SIN(RADIANS(DIM_STATIONS_2.Longitude - (-73.994685))/2),2)
            )
        )
    ) AS avg_distance_km
FROM FACT_BIKE_RIDE
JOIN DIM_STATIONS AS DIM_STATIONS_2 ON FACT_BIKE_RIDEN.End_station_ID = DIM_STATIONS_2.Station_ID
GROUP BY FACT_BIKE_RIDE.User_type;


-- How does weather influence the average trip duration from the station? (trip duration)
SELECT 
    DIM_WEATHER.Temperature_max, -- Othwer weather parameters cab be included.
    DIM_WEATHER.Rain_sum,
    DIM_WEATHER.Snowfall_sum,
    AVG(EXTRACT(EPOCH FROM (FACT_BIKE_RIDE.End_timestamp - FACT_BIKE_RIDE.Start_timestamp))/60) AS avg_trip_duration_minutes
FROM FACT_BIKE_RIDE
JOIN DIM_WEATHER ON FACT_BIKE_RIDE.Weather = DIM_WEATHER.Weather_ID
GROUP BY DIM_WEATHER.Temperature_max, DIM_WEATHER.Rain_sum, DIM_WEATHER.Snowfall_sum
ORDER BY DIM_WEATHER.Temperature_max;


-- How does weather influence the average trip distance from the station? (trip distance)
SELECT 
    DIM_WEATHER.Temperature_max, -- Othwer weather parameters cab be included.
    DIM_WEATHER.Rain_sum,
    DIM_WEATHER.Wind_speed_max,
    AVG(
        6371 * 2 * ASIN(
            SQRT(
                POWER(SIN(RADIANS(DIM_STATIONS_2.Latitude - 40.750585)/2),2) + -- Coordinates of Ave & W 31 St station
                COS(RADIANS(40.750580)) * COS(RADIANS(DIM_STATIONS_2.Latitude)) *
                POWER(SIN(RADIANS(DIM_STATIONS_2.Longitude - (-73.994685))/2),2)
            )
        )
    ) AS avg_distance_km
FROM FACT_BIKE_RIDE
JOIN DIM_WEATHER ON FACT_BIKE_RIDE = DIM_WEATHER.Weather_ID
JOIN DIM_STATIONS AS DIM_STATIONS_2 ON FACT_BIKE_RIDEN.End_station_ID = DIM_STATIONS_2.Station_ID
GROUP BY DIM_WEATHER.Temperature_max, DIM_WEATHER.Rain_sum, DIM_WEATHER.Snowfall_sum
ORDER BY DIM_WEATHER.Temperature_max;


-- Are there consistent differences in usage between weekdays and weekends? (ride count, trip duration)
SELECT 
    DIM_TIME.Weekday,
    COUNT(*) AS ride_count,
    AVG(EXTRACT(EPOCH FROM (FACT_BIKE_RIDE.End_timestamp - FACT_BIKE_RIDE.Start_timestamp))/60) AS avg_trip_duration_minutes
FROM FACT_BIKE_RIDE
JOIN DIM_TIME ON FACT_BIKE_RIDE.Start_date = DIM_TIME.Time_ID
GROUP BY DIM_TIME.Weekday
ORDER BY DIM_TIME.Weekday;


-- How do national holidays compare with regular weekdays in terms of ride count and rider type? (ride count, rider composition)
SELECT 
    DIM_TIME.Is_national_holiday,
    FACT_BIKE_RIDE.User_type,
    COUNT(*) AS ride_count
FROM FACT_BIKE_RIDE
JOIN DIM_TIME ON FACT_BIKE_RIDE.Start_date = DIM_TIME.Time_ID
GROUP BY DIM_TIME.Is_national_holiday, FACT_BIKE_RIDE.User_type
ORDER BY DIM_TIME.Is_national_holiday, FACT_BIKE_RIDE.User_type;


-- How do seasonal changes across the year (winter vs summer months) affect ride frequency, trip duration, and distance? (ride count, trip duration, trip distance)
SELECT 
    CASE 
        WHEN DIM_TIME.Month IN (12,1,2) THEN 'Winter'
        WHEN DIM_TIME.Month IN (6,7,8) THEN 'Summer'
        ELSE 'Other'
    END AS season,
    COUNT(*) AS ride_count,
    AVG(EXTRACT(EPOCH FROM (FACT_BIKE_RIDE.End_timestamp - FACT_BIKE_RIDE.Start_timestamp))/60) AS avg_trip_duration_minutes,
    AVG(
        6371 * 2 * ASIN(
            SQRT(
                POWER(SIN(RADIANS(DIM_STATIONS_2.Latitude - 40.750585)/2),2) + -- Coordinates of Ave & W 31 St station
                COS(RADIANS(40.750580)) * COS(RADIANS(DIM_STATIONS_2.Latitude)) *
                POWER(SIN(RADIANS(DIM_STATIONS_2.Longitude - (-73.994685))/2),2)
            )
        )
    ) AS avg_distance_km
FROM FACT_BIKE_RIDE
JOIN DIM_TIME ON FACT_BIKE_RIDE.Start_date = DIM_TIME.Time_ID
JOIN DIM_STATIONS AS DIM_STATIONS_2 ON FACT_BIKE_RIDE.End_station_ID = DIM_STATIONS_2.Station_ID
GROUP BY season
ORDER BY season;
