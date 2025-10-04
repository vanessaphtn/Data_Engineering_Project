-- DIMENSION TABLE: DIM_STATIONS
CREATE TABLE DIM_STATIONS (
    Station_ID VARCHAR(10) PRIMARY KEY,          
    Station_name VARCHAR,                        
    Latitude DECIMAL(8,6),                       
    Longitude DECIMAL(9,6)                      
);

-- DIMENSION TABLE: DIM_WEATHER
CREATE TABLE DIM_WEATHER (
    Weather_ID INT PRIMARY KEY,                  
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
    Is_national_holiday BOOLEAN                 
);

-- FACT TABLE: FACT_BIKE_RIDE
CREATE TABLE FACT_BIKE_RIDE (
    Bike_type VARCHAR(20),                       
    Start_date INT REFERENCES DIM_TIME(Time_ID), 
    End_date INT REFERENCES DIM_TIME(Time_ID),  
    Weather INT REFERENCES DIM_WEATHER(Weather_ID), 
    Start_timestamp TIMESTAMP,                   
    End_timestamp TIMESTAMP,                     
    End_station_ID VARCHAR(10) REFERENCES DIM_STATIONS(Station_ID),   
    User_type VARCHAR(10)                         
);