from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from clickhouse_driver import Client
import pandas as pd
import requests
import os
import zipfile
import openmeteo_requests
import requests_cache
from retry_requests import retry

# --------------------------------------------------------------------------------
# Downloads the weather data via API
# --------------------------------------------------------------------------------

def download_weather_data(
    latitude=40.7128,
    longitude=-74.0060,
    start_date="2024-01-01",
    end_date="2025-09-30",
    timezone="America/New_York"
):
    """
    Downloads daily historical weather data from Open-Meteo API for a given location and date range.
    Returns a Pandas DataFrame.
    """

    # Setup cached session with retries
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # API parameters
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "weather_code",
            "temperature_2m_mean",
            "temperature_2m_max",
            "temperature_2m_min",
            "rain_sum",
            "snowfall_sum",
            "precipitation_hours",
            "wind_speed_10m_max",
            "shortwave_radiation_sum"
        ],
        "timezone": timezone,
    }

    # Call the API
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]  # first location

    # Extract daily data
    daily = response.Daily()
    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        ),
        "weather_code": daily.Variables(0).ValuesAsNumpy(),
        "temperature_2m_mean": daily.Variables(1).ValuesAsNumpy(),
        "temperature_2m_max": daily.Variables(2).ValuesAsNumpy(),
        "temperature_2m_min": daily.Variables(3).ValuesAsNumpy(),
        "rain_sum": daily.Variables(4).ValuesAsNumpy(),
        "snowfall_sum": daily.Variables(5).ValuesAsNumpy(),
        "precipitation_hours": daily.Variables(6).ValuesAsNumpy(),
        "wind_speed_10m_max": daily.Variables(7).ValuesAsNumpy(),
        "shortwave_radiation_sum": daily.Variables(8).ValuesAsNumpy(),
    }

    df = pd.DataFrame(daily_data)
    print(f"Downloaded {len(df)} rows of daily weather data for {latitude}, {longitude}")
    return df

# --------------------------------------------------------------------------------
# DAG arguments
# --------------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# --------------------------------------------------------------------------------
# DAG definition
# --------------------------------------------------------------------------------
with DAG(
    dag_id="weather_data_ingestion",
    default_args=default_args,
    description="Download daily weather data and save to CSV",
    start_date=datetime(2025, 10, 24),
    schedule_interval="@daily",  # change as needed
    catchup=False,
    tags=["weather", "bronze"],
) as dag:

    # --------------------------------------------------------------------------------
    # Task: Download and save weather data
    # --------------------------------------------------------------------------------
    def download_and_save(**kwargs):
            execution_date = kwargs["ds"]
            file_path = "/opt/airflow/data/bronze/weather/weather_data.csv"
            os.makedirs("/opt/airflow/data/bronze", exist_ok=True)

            df = download_weather_data(start_date="2024-01-01", end_date=execution_date) # currently rewriting the weather data every time
            df.to_csv(file_path, index=False)


    download_task = PythonOperator(
        task_id="download_weather_data",
        python_callable=download_and_save,
        provide_context=True
    )

    # --------------------------------------------------------------------------------
    #  Load into ClickHouse -- POLE TEHTUD, hetkel mingi chatGPT värk 
    # --------------------------------------------------------------------------------
    # def load_to_clickhouse(**kwargs):
    #     file_path = "data/bronze/weather_data.csv"
    #     df = pd.read_csv(file_path)

    #     # ClickHouse connection (example using BaseHook for Airflow connection)
    #     client = Client(host="clickhouse", port=9000, user="default", password="clickhouse", database="weather_db")
    #     client.execute("CREATE TABLE IF NOT EXISTS bronze_weather (\
    #         date DateTime, \
    #         weather_code Float32, \
    #         temperature_2m_mean Float32, \
    #         temperature_2m_max Float32, \
    #         temperature_2m_min Float32, \
    #         rain_sum Float32, \
    #         snowfall_sum Float32, \
    #         precipitation_hours Float32, \
    #         wind_speed_10m_max Float32, \
    #         shortwave_radiation_sum Float32)\
    #         ENGINE = MergeTree() ORDER BY date;")

    #     # Insert data
    #     records = df.to_dict("records")
    #     client.execute(
    #         "INSERT INTO bronze_weather VALUES",
    #         [tuple(r.values()) for r in records]
    #     )
    #     print(f"Loaded {len(df)} rows into ClickHouse.")

    # load_task = PythonOperator(
    #     task_id="load_weather_to_clickhouse",
    #     python_callable=load_to_clickhouse
    # )

    # # --------------------------------------------------------------------------------
    # # Task dependencies
    # # --------------------------------------------------------------------------------
    # download_task >> load_task

