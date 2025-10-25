from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from clickhouse_driver import Client
import pandas as pd
import requests
import os, io
import zipfile
from retry_requests import retry

# --------------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------------

def download_citibike_month(month_str, station_name="8 Ave & W 31 St", output_dir="/opt/airflow/data/bronze"):
    """
    Downloads Citibike data for a given month, handles multiple CSVs inside one ZIP,
    filters for a specific station, and saves to /opt/airflow/data/bronze.
    """
    url = f"https://s3.amazonaws.com/tripdata/{month_str}-citibike-tripdata.zip"
    print(f"Downloading {url} ...")

    r = requests.get(url)
    r.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_files = [name for name in z.namelist() if name.endswith(".csv")]

    if not csv_files:
        raise ValueError(f"No CSV files found in {url}")

    dfs = []
    for csv_name in csv_files:
        print(f"Reading {csv_name} ...")
        df = pd.read_csv(z.open(csv_name))
        dfs.append(df)

    # Combine all CSVs into one DataFrame
    df_all = pd.concat(dfs, ignore_index=True)

    # ----- QUALITY CHECKS -----

    # Filter for a specific station
    if station_name is not None and "start_station_name" in df_all.columns:
        df_all = df_all[df_all["start_station_name"] == station_name]

    # Drop NaNs in station names and coordinates 
    df_all = df_all.dropna(subset=["start_station_name", "end_station_name","start_lat", "start_lng", "end_lat", "end_lng"])

    # Check if coordinates range is in NYC 
    lat_min, lat_max = 40.4, 41.0
    lng_min, lng_max = -74.3, -73.6
    df_all = df_all[
        (df_all["start_lat"].between(lat_min, lat_max)) &
        (df_all["start_lng"].between(lng_min, lng_max)) &
        (df_all["end_lat"].between(lat_min, lat_max)) &
        (df_all["end_lng"].between(lng_min, lng_max))
    ]

    print(f"Downloaded {len(df_all)} rows")
    return df_all

# --------------------------------------------------------------------------------
# DAG arguments
# --------------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --------------------------------------------------------------------------------
# DAG definition
# --------------------------------------------------------------------------------
with DAG(
    dag_id="bike_data_ingestion",
    default_args=default_args,
    description="Download monthly bike data and save to CSV",
    start_date=datetime(2025, 10, 20),
    schedule_interval="0 4 15 * *",  # change as needed
    catchup=True,
    tags=["bike", "bronze"],
) as dag:

# --------------------------------------------------------------------------------
# Task: Download and save bike data
# --------------------------------------------------------------------------------
    def download_and_save(**kwargs):
        execution_date = kwargs["ds"]  # e.g. '2025-10-24'
        last_month_date = datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=30)
        last_month = last_month_date.strftime("%Y%m")  # e.g. '202509'

        df_new = download_citibike_month(month_str=last_month) # new data

        bronze_dir = "/opt/airflow/data/bronze"
        os.makedirs(bronze_dir, exist_ok=True)

        # all historic citibike_YYYY.csv
        historic_files = [
            os.path.join(bronze_dir, f)
            for f in os.listdir(bronze_dir)
            if f.startswith("citibike_20") and f.endswith(".csv") and last_month not in f
        ]

        df_historic_list = []
        for file in historic_files:
            df_historic_list.append(pd.read_csv(file))

        df_historic = pd.concat(df_historic_list, ignore_index=True) if df_historic_list else pd.DataFrame()

        # Filter out duplicates
        if not df_historic.empty and "ride_id" in df_new.columns and "ride_id" in df_historic.columns:
            before = len(df_new)
            df_new = df_new[~df_new["ride_id"].isin(df_historic["ride_id"])]
            print(f"Removed {before - len(df_new)} rides already present in historic datasets.")

        # Save or overwrite this month’s file
        if len(df_new) > 0:
            monthly_path = os.path.join(bronze_dir, f"citibike_{last_month}.csv")
            df_new.to_csv(monthly_path, index=False)
            print(f"Saved {len(df_new)} new rides to {monthly_path}")

    download_task = PythonOperator(
        task_id="download_bike_data",
        python_callable=download_and_save,
        provide_context=True
    )

#     # --------------------------------------------------------------------------------
#     #  Load into ClickHouse -- POLE TEHTUD
#     # --------------------------------------------------------------------------------
#     def load_to_clickhouse(**kwargs):
#         return None

#     load_task = PythonOperator(
#         task_id="load_bike_to_clickhouse",
#         python_callable=load_to_clickhouse
#     )

#     # --------------------------------------------------------------------------------
#     # Task dependencies
#     # --------------------------------------------------------------------------------
#     download_task >> load_task

