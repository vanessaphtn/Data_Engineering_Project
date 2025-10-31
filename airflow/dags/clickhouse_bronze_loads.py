from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from lib_ch import ch_query, ch_insert_csv

SQL_FILE = "/opt/airflow/sql/clickhouse_init.sql"
WEATHER  = "/opt/airflow/data/bronze/weather/weather_data.csv"
TRIPS    = "/opt/airflow/data/bronze/citibike/citibike_202509.csv"
HOLIDAYS = "/opt/airflow/data/bronze/holiday/holidays.csv"

def run_init_sql():
    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql = f.read()
    # Run statements one-by-one (HTTP API can't execute multi-statement batches)
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        ch_query(stmt)

with DAG(
    dag_id="clickhouse_bronze_loads",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
) as dag:
    init = PythonOperator(
        task_id="init_clickhouse",
        python_callable=run_init_sql,
    )
    load_weather = PythonOperator(
        task_id="load_weather",
        python_callable=lambda: ch_insert_csv("bronze", "weather_raw", WEATHER),
    )
    load_trips = PythonOperator(
        task_id="load_trips",
        python_callable=lambda: ch_insert_csv("bronze", "trips_raw", TRIPS),
    )
    load_holidays = PythonOperator(
        task_id="load_holidays",
        python_callable=lambda: ch_insert_csv("bronze", "holidays_raw", HOLIDAYS),
    )
    dq_trips = PythonOperator(
        task_id="dq_trips_not_null",
        python_callable=lambda: ch_query(
            "SELECT if(count()=0,1,throwIf(1,'NULL ride_id')) "
            "FROM bronze.trips_raw WHERE ride_id='' OR ride_id IS NULL"
        ),
    )

    init >> [load_weather, load_trips, load_holidays] >> dq_trips
