
-- How do daily temperatures influence the number of rides taken from the station? (daily ride count)
SELECT 
    DIM_TIME.Year,
    DIM_TIME.Day,
    DIM_TIME.Month,
    DIM_WEATHER.Temperature_max,
    COUNT(*) AS daily_ride_count
FROM FACT_BIKE_RIDE
JOIN DIM_TIME ON FACT_BIKE_RIDE.Start_date = DIM_TIME.Time_ID
JOIN DIM_WEATHER ON FACT_BIKE_RIDE.Weather = DIM_WEATHER.Weather_ID
ORDER BY DIM_WEATHER.Temperature_max;

-- Did the extreme heatwave in June 2024 reduce bike usage compared to June 2023? (daily ride count)
SELECT 
    DIM_TIME.Year,
    DIM_TIME.Month,
    COUNT(*) AS total_rides
FROM FACT_BIKE_RIDE
JOIN DIM_TIME ON FACT_BIKE_RIDE.Start_date = DIM_TIME.Time_ID
JOIN DIM_WEATHER ON FACT_BIKE_RIDE.Weather = DIM_WEATHER.Weather_ID
WHERE DIM_TIME.Month = 6
  AND DIM_TIME.Year IN (2023, 2024)
GROUP BY DIM_TIME.Year, DIM_TIME.Month
ORDER BY DIM_TIME.Year;

-- Are casual riders more sensitive to extreme heat than members? (rider composition)
SELECT 
    DIM_WEATHER.Temperature_max,
    FACT_BIKE_RIDE.User_type,
    COUNT(*) AS ride_count
FROM FACT_BIKE_RIDE
JOIN DIM_WEATHER ON FACT_BIKE_RIDE.Weather = DIM_WEATHER.Weather_ID
WHERE DIM_WEATHER.Temperature_max  > 30
GROUP BY DIM_WEATHER.Temperature_max, FACT_BIKE_RIDE.User_type
ORDER BY DIM_WEATHER.Temperature_max, FACT_BIKE_RIDE.User_type;

--Does extreme heat change the average trip duration relative to normal summer days? (average trip duration)
SELECT 
    CASE 
        WHEN DIM_WEATHER.Temperature_max > 30 THEN 'Extreme Heat'
        ELSE 'Normal Summer'
    END AS weather_condition,
    AVG(EXTRACT(EPOCH FROM (FACT_BIKE_RIDE.End_timestamp - FACT_BIKE_RIDE.Start_timestamp))/60) AS avg_duration_minutes
FROM FACT_BIKE_RIDE
JOIN DIM_WEATHER ON FACT_BIKE_RIDE.Start_date = DIM_WEATHER.Weather_ID
JOIN DIM_TIME ON FACT_BIKE_RIDE.Start_date = DIM_TIME.Time_ID
WHERE DIM_TIME.Month IN (6, 7, 8) 
GROUP BY weather_condition;

-- Are weekend trips more influenced by extreme heat than weekday trips? (daily ride count + rider composition)
SELECT 
    CASE 
        WHEN DIM_TIME.Weekday IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    COUNT(*) AS ride_count
FROM FACT_BIKE_RIDE
JOIN DIM_TIME ON FACT_BIKE_RIDE.Start_date = DIM_TIME.Time_ID
GROUP BY day_type;
