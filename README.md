# Examining the Influence of Weather on CitiBike Trips in New York City

## 1. Business Brief
### Objective
The project investigates how daily weather conditions influence the use of a selected Citi Bike station in New York City over the course of 2024. Each new month of data is incorporated sequentially, allowing for a continuous view of usage patterns across the year. The station was chosen from a popular area to capture steady demand and provide meaningful comparisons between members and casual riders. The observed station is 5 Ave & W 31 St and it is located in a busy area near Penn Station, Madison Square Garden, and several office buildings. Although the analysis focuses on New York City, the same framework can be applied to other cities or countries, such as Tartu, to explore how weather conditions impact shared bike usage in different urban areas.
### Stakeholders
The results are relevant for city planners and policymakers seeking to understand how weather shapes urban mobility and how cycling infrastructure can better support sustainable transport. Bike-sharing operators gain practical insight into how demand fluctuates under different conditions, which can guide resource allocation, fleet management, and service improvements. Researchers in transportation and environmental studies can use the findings to analyze how climate factors interact with everyday mobility choices in a dense urban setting.
### Key Metrics (KPIs)
- Daily ride count (members vs non-members) 
- Average trip duration 
- Average trip distance 

### Business Questions
1. How do daily temperatures influence the number of rides taken from the station?
2. How do precipitation and snowfall affect daily ride counts?
3. Do members or casual riders contribute more to overall ride volume under different weather conditions?
4. Do members or casual riders take longer trips in terms of duration?
5. Do members or casual riders travel farther in terms of distance?
6. How does weather influence the average trip duration from the station?
7. How does weather influence the average trip distance from the station?
8. Are there consistent differences in usage between weekdays and weekends?
9. How do national holidays compare with regular weekdays in terms of ride count and rider type?
10. How do seasonal changes across the year (winter vs summer months) affect ride frequency, trip duration, and distance?

## 2. Datasets
### [New York City CitiBike Rides Data](https://s3.amazonaws.com/tripdata/index.html)
The CitiBike dataset contains detailed trip-level information about New York City’s public bike-sharing system. Each record represents a single ride and includes fields such as trip start and end times, start and end station names and IDs, station geocoordinates (latitude and longitude), bike ID, bike and membership type. Data is provided as monthly CSV files through CitiBike’s official portal, and each file typically contains hundreds of thousands of rows. Therefore we have decided to take a look at a single station to make the dataset more manageable.  
### [Weather API](https://open-meteo.com/en/docs/historical-weather-api)
The Open-Meteo Historical Weather API provides structured weather data at a daily frequency. It includes variables such as daily minimum and maximum temperature, precipitation, humidity, wind speed etc. The API allows data retrieval for specific locations, enabling us to extract weather observations for New York City. 

## 3. Tooling
In our project, **Docker** provides a reproducible environment for running ingestion scripts, while **Airflow** schedules and automates monthly CitiBike file loads and daily weather API pulls. The raw and processed data is stored in **Postgres**, which also serves as the central data warehouse. **dbt** is used to transform the data and build a star schema and prepare daily aggregates such as ride counts and member ratios. Analytical queries at scale could be handled with **ClickHouse** as an OLAP engine, and **MongoDB** would be suitable for handling semi-structured weather feeds. Governance and security could be addressed through **Open Metadata** and **masking or RBAC** techniques. Results could also be presented as an interactive dashboard using the **Streamlit** application. 

## 4. Data Architecture
<img width="1045" height="273" alt="DataArchitecture" src="https://github.com/user-attachments/assets/f3f3a48b-079f-47cc-882d-bfffcc6d2d9a" />

**Quality checks** (done after ingestion): Ensure that every bike trip has both a valid start station and end station, including non-null latitude and longitude values. Trips missing these fields would be excluded. Also, checking if the latitude and longitude values are realistic and falling within the expected geographic range of New York City.

## 5. Data Model
<img width="1045" height="auto" alt="dataModel2" src="https://github.com/user-attachments/assets/c21f0ec3-1c67-4563-a265-79abf40c20e4" /> 

**Grain**: Each record represents a single ride event that started and ended on a specific date, linking to corresponding time, weather, and station information.

**Slowly Changing Dimension (SCD)**: DIM_STATIONS uses SCD Type 2 to preserve historical changes in station information, while DIM_WEATHER and DIM_TIME are Static, as weather and time data are fixed historical records that do not change over time.

**Possible improvements**: Currently, the fact table represents rides originating from a single station and ending at any number of other stations. To enable monitoring of trips between all possible station pairs, the fact table could be updated by adding a new column, ‘Start_station_ID’. This addition would allow analysis of rides from every start station to every end station, allowing for a deeper examination of station popularity and user patterns.



## 6. Data Dictionary
### FACT_BIKE_RIDE
| Column           | Data type       | PK / FK                   | Description                                      |
|-----------------|----------------|---------------------------|-------------------------------------------------|
| Bike_type       | VARCHAR(20)     |                           | Type of bike used for the ride (classic_bike or electric_bike) |
| Start_date      | INT             | FK (DIM_TIME, Time_ID)    | Ride start time                                  |
| End_date        | INT             | FK (DIM_TIME, Time_ID)    | Ride end time                                    |
| Weather         | INT             | FK (DIM_WEATHER, Weather_ID) | Weather on the day of the ride                |
| Start_timestamp | TIMESTAMP       |                           | Exact timestamp of the ride start time          |
| End_timestamp   | TIMESTAMP       |                           | Exact timestamp of the ride end time            |
| End_station_ID  | VARCHAR(10)     | FK (DIM_STATIONS, Station_ID) | Station where the ride ended                 |
| User_type       | VARCHAR(10)     |                           | Type of the user (member or casual)            |

### DIM_STATIONS
| Column        | Data type            | PK / FK | Description                                    |
|---------------|--------------------|---------|-----------------------------------------------|
| Station_ID    | VARCHAR(10)         | PK      | ID of the station, e.g. ‘HB202’              |
| Station_name  | VARCHAR             |         | Name of the station, e.g. ‘Clinton St & Newark St’ |
| Latitude      | DECIMAL(8,6)        |         | Latitude of the station position              |
| Longitude     | DECIMAL(9,6)        |         | Longitude of the station position             |

### DIM_WEATHER
| Column                 | Data type     | PK / FK | Description                                                |
|------------------------|--------------|---------|------------------------------------------------------------|
| Weather_ID             | INT          | PK      | Primary key for weather                                     |
| Temperature_max        | DECIMAL(4,2) |         | Maximum daily air temperature at 2 meters above ground (°C)|
| Temperature_min        | DECIMAL(4,2) |         | Minimum daily air temperature at 2 meters above ground (°C)|
| Rain_sum               | FLOAT        |         | Sum of daily rain (mm)                                     |
| Precipitation_hours    | INT          |         | The number of hours with rain                               |
| Snowfall_sum           | FLOAT        |         | Sum of daily snowfall (cm)                                  |
| Wind_speed_max         | FLOAT        |         | Maximum wind speed (km/h)                                   |
| Shortwave_radiation_sum| FLOAT        |         | Sum of solar radiation on a given day in Megajoules (MJ/m²)|

### DIM_TIME
| Column             | Data type | PK / FK | Description                                               |
|-------------------|----------|---------|-----------------------------------------------------------|
| Time_ID           | INT      | PK      | Primary key for the time                                  |
| Day               | INT      |         | Day of the month (1–31)                                   |
| Month             | INT      |         | Month of the year (1–12)                                   |
| Weekday           | INT      |         | Day of the week (1–7). 1 meaning Monday, 7 Sunday        |
| Is_national_holiday| BOOLEAN  |         | Whether the day is a federal holiday                      |


## Project start:

```

git clone https://github.com/vanessaphtn/Data_Engineering_Project.git
cd data_engineering_project
docker compose build
docker compose up -d

CMD - käsud(Build Silver/Gold with dbt):
docker compose run --rm dbt debug
docker compose run --rm dbt run
docker compose run --rm dbt test

Query:
curl -u default:clickhouse "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20default_silver"
curl -u default:clickhouse "http://localhost:8123/?query=SHOW%20TABLES%20FROM%20default_gold"
curl -u default:clickhouse "http://localhost:8123/?query=SELECT%20count()%20FROM%20default_gold.fact_bike_ride"


```

Airflow is http://localhost:8080 (admin/admin)

See võiks midagi sellist välja näha:
<img width="1440" height="876" alt="Screenshot 2025-10-25 at 14 00 39" src="https://github.com/user-attachments/assets/00d41efd-3752-4cec-a6f3-765d69bc9d9a" />

Nüüd võiks midagi sellist välja näha (eeldatavasti):
<img width="1256" height="398" alt="Bronze" src="https://github.com/user-attachments/assets/2e777e8f-06fd-401d-abf7-a39001591361" />

Tegelesin veel ja peaks selline olema:
<img width="1259" height="433" alt="Screenshot 2025-10-26 201509" src="https://github.com/user-attachments/assets/3912e376-cf73-447a-bcc1-8ef85297c396" />

