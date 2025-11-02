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
from defaults import DEFAULT_ARGS

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
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    print(f"Downloaded {len(df)} rows of daily weather data for {latitude}, {longitude}")
    return df

# --------------------------------------------------------------------------------
# DAG definition
# --------------------------------------------------------------------------------
with DAG(
    dag_id="weather_data_ingestion",
    default_args=DEFAULT_ARGS,
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

