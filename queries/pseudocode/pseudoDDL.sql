-- DIMENSION TABLE: DIM_STATIONS
CREATE TABLE DIM_STATIONS (
    Station_ID INT PRIMARY KEY,
    Station_business_key VARCHAR(10),
    Station_name VARCHAR,                        
    Latitude DECIMAL(8,6),                       
    Longitude DECIMAL(9,6),
    Valid_from DATE,
    Valid_to DATE
);

-- DIMENSION TABLE: DIM_WEATHER
CREATE TABLE DIM_WEATHER (
    Weather_ID INT PRIMARY KEY,
    Weather_code VARCHAR,
    Temperature_mean DECIMAL(4,2),
    Temperature_max DECIMAL(4,2),              
    Temperature_min DECIMAL(4,2),               
    Rain_sum FLOAT,                              
    Precipitation_hours INT,                     
    Snowfall_sum FLOAT,                          
    Wind_speed_max FLOAT,                         
    Shortwave_radiation_sum FLOAT               
);

-- DIMENSION TABLE: DIM_TIME
CREATE TABLE DIM_TIME (
    Time_ID INT PRIMARY KEY,                    
    Day INT,                                     
    Month INT,                                   
    Weekday INT,
    Year,
    Is_national_holiday BOOLEAN,
    Is_new_york_holiday BOOLEAN,
    Is_weekend BOOLEAN,
    Season VARCHAR(15)
);

-- DIMENSION TABLE: DIM_BIKE_TYPE
CREATE TABLE DIM_BIKE_TYPE (
    Bike_type_ID INT PRIMARY KEY,                    
    Bike_type VARCHAR(20)
);

-- DIMENSION TABLE: DIM_USER_TYPE
CREATE TABLE DIM_USER_TYPE (
    User_type_ID INT PRIMARY KEY,                    
    User_type VARCHAR(10)
);

-- FACT TABLE: FACT_BIKE_RIDE
CREATE TABLE FACT_BIKE_RIDE (
    Bike_type_ID INT REFERENCES DIM_BIKE_TYPE(Bike_type_ID),                       
    Start_date INT REFERENCES DIM_TIME(Time_ID), 
    Weather_ID INT REFERENCES DIM_WEATHER(Weather_ID), 
    Start_timestamp TIMESTAMP,                   
    End_timestamp TIMESTAMP,                     
    End_station_ID VARCHAR(10) REFERENCES DIM_STATIONS(Station_ID),   
    User_type VARCHAR(10) REFERENCES DIM_USER_TYPE(User_type_ID),
    Trip_duration INT
);
