from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from clickhouse_driver import Client
import pandas as pd
import requests
import os
import zipfile
import requests_cache
from retry_requests import retry

# --------------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------------

def download_bike_data():
    
    return None

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
    start_date=datetime(2025, 10, 24),
    schedule_interval="@monthly",  # change as needed
    catchup=False,
    tags=["bike", "bronze"],
) as dag:

    # --------------------------------------------------------------------------------
    # Task: Download and save bike data
    # --------------------------------------------------------------------------------
    def download_and_save(**kwargs):
        execution_date = kwargs["ds"]  # e.g. '2025-10-24'
        df = download_weather_data(start_date=execution_date, end_date=execution_date)

        os.makedirs("/opt/airflow/data/bronze", exist_ok=True)
        file_path = "/opt/airflow/data/bronze/weather_data.csv"

        # If the file exists, append only if that date is not already present
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path)

            # Remove any old rows for this same date (idempotency)
            existing_df = existing_df[existing_df["date"] != execution_date]

            # Append and sort
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df = combined_df.sort_values("date")

            combined_df.to_csv(file_path, index=False)
            print(f"Appended data for {execution_date} to {file_path}")
        else:
            # If file doesn’t exist yet, just write the first day
            df.to_csv(file_path, index=False)
            print(f"Created new cumulative file: {file_path}")

    download_task = PythonOperator(
        task_id="download_bike_data",
        python_callable=download_and_save,
        provide_context=True
    )

    # --------------------------------------------------------------------------------
    #  Load into ClickHouse -- POLE TEHTUD
    # --------------------------------------------------------------------------------
    def load_to_clickhouse(**kwargs):
        return None

    load_task = PythonOperator(
        task_id="load_bike_to_clickhouse",
        python_callable=load_to_clickhouse
    )

    # --------------------------------------------------------------------------------
    # Task dependencies
    # --------------------------------------------------------------------------------
    download_task >> load_task

