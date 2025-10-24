from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from clickhouse_driver import Client
import pandas as pd
import requests
import os, io
import zipfile
import requests_cache
from retry_requests import retry

# --------------------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------------------

# def download_citibike_month(month_str, station_id=8, output_dir="/opt/airflow/data/bronze"):
#     """
#     Downloads Citibike data for a given month, handles multiple CSVs inside one ZIP,
#     filters for a specific station, and saves to /opt/airflow/data/bronze.
#     """
#     url = f"https://s3.amazonaws.com/tripdata/{month_str}-citibike-tripdata.zip"
#     print(f"Downloading {url} ...")

#     r = requests.get(url)
#     r.raise_for_status()

#     z = zipfile.ZipFile(io.BytesIO(r.content))
#     csv_files = [name for name in z.namelist() if name.endswith(".csv")]

#     if not csv_files:
#         raise ValueError(f"No CSV files found in {url}")

#     dfs = []
#     for csv_name in csv_files:
#         print(f"Reading {csv_name} ...")
#         df = pd.read_csv(z.open(csv_name))
#         dfs.append(df)

#     # Combine all CSVs into one DataFrame
#     df_all = pd.concat(dfs, ignore_index=True)

#     # Filter for a specific station
#     if station_id is not None and "start_station_id" in df_all.columns:
#         df_all = df_all[df_all["start_station_id"] == station_id]

#     os.makedirs(output_dir, exist_ok=True)
#     out_file = os.path.join(output_dir, f"citibike_{month_str}.csv")

#     # Idempotent — overwrite for same month
#     df_all.to_csv(out_file, index=False)
#     print(f"Saved {len(df_all)} rows to {out_file}")
#     return out_file

# # --------------------------------------------------------------------------------
# # DAG arguments
# # --------------------------------------------------------------------------------
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
# }

# # --------------------------------------------------------------------------------
# # DAG definition
# # --------------------------------------------------------------------------------
# with DAG(
#     dag_id="bike_data_ingestion",
#     default_args=default_args,
#     description="Download monthly bike data and save to CSV",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval="@monthly",  # change as needed
#     catchup=False,
#     tags=["bike", "bronze"],
# ) as dag:

#     # --------------------------------------------------------------------------------
#     # Task: Download and save bike data
#     # --------------------------------------------------------------------------------
#     def download_and_save(**kwargs):
#         # --------------------------
#         # Get execution context
#         # --------------------------
#         execution_date = kwargs["ds"]  # e.g. '2025-10-24'
#         last_month = (datetime.strptime(execution_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y%m")

#         # Download CitiBike data for the *previous month*
#         df = download_citibike_month(month_str=last_month, station_id=8)

#         # --------------------------
#         # Define output file
#         # --------------------------
#         os.makedirs("/opt/airflow/data/bronze", exist_ok=True)
#         file_path = "/opt/airflow/data/bronze/citibike_data.csv"

#         # --------------------------
#         # Idempotency logic
#         # --------------------------
#         # If the file already exists → only append *new* data
#         if os.path.exists(file_path):
#             existing_df = pd.read_csv(file_path)

#             # Define a unique identifier — here, trip 'ride_id' (adjust if needed)
#             if "ride_id" in df.columns and "ride_id" in existing_df.columns:
#                 # Keep only new rides not already stored
#                 df = df[~df["ride_id"].isin(existing_df["ride_id"])]

#             # Append the new rows
#             combined_df = pd.concat([existing_df, df], ignore_index=True)
#             combined_df.to_csv(file_path, index=False)
#             print(f"Appended {len(df)} new rows to {file_path}")

#         else:
#             # If the file doesn’t exist yet, create it
#             df.to_csv(file_path, index=False)
#             print(f"Created new file: {file_path} with {len(df)} rows")



#     download_task = PythonOperator(
#         task_id="download_bike_data",
#         python_callable=download_and_save,
#         provide_context=True
#     )

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

