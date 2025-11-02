import pandas as pd
import holidays
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os, io

def load_us_ny_holidays(end):
    start = pd.to_datetime("2024-01-01").date()
    end   = pd.to_datetime(end).date()
    dates = pd.date_range(start, end, freq="D").date

    # 2. Holiday calendars
    us_cal = holidays.US(years=range(2024,2031))
    ny_cal = holidays.country_holidays('US', subdiv='NY', years=range(2024,2031))

    # 3. Build DataFrame
    df = pd.DataFrame({
        "date": pd.to_datetime(list(dates)),
        "is_national_holiday": [int(d in us_cal) for d in dates],
        "is_new_york_holiday": [int((d in ny_cal) and (d not in us_cal)) for d in dates]
    })

    return df

def download_and_save(**kwargs):
        execution_date = kwargs["ds"]  # e.g. '2025-10-24'
        df_new = load_us_ny_holidays(execution_date)

        bronze_dir = "/opt/airflow/data/bronze/holiday/"
        os.makedirs(bronze_dir, exist_ok=True)

        # Save this month’s data
        path = os.path.join(bronze_dir, f"holidays.csv")
        df_new.to_csv(path, index=False)
        print(f"Saved {len(df_new)} new rides to {path}")


with DAG(
    dag_id="holidays_load",
    start_date=datetime(2024, 1, 1),
    schedule=None,       # manual run or trigger before dbt
    catchup=False,
    tags=["bronze", "holidays"],
) as dag:

    load_holidays_task = PythonOperator(
        task_id="load_us_ny_holidays",
        python_callable=download_and_save
    )
