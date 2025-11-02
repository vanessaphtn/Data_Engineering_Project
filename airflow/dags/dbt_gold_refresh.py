# airflow/dags/dbt_gold_refresh.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from defaults import DEFAULT_ARGS

# Paths inside the Airflow containers
DBT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"  # dbt installed for 'airflow' user here

# Common environment for all dbt commands
DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_DIR,          # profiles.yml lives in /opt/airflow/dbt
    "DBT_LOG_PATH": "/tmp/dbt_logs",      # write logs to a writable location
    "DBT_TARGET_PATH": "/tmp/dbt_target", # write manifest/run artifacts to /tmp
}


with DAG(
    dag_id="dbt_gold_refresh",
    description="Run dbt silver and gold models (ClickHouse) and tests",
    start_date=datetime(2025, 10, 20),
    schedule="0 6 16 * *", # After the bronze loads
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dbt", "clickhouse", "gold"],
) as dag:

    # Ensure writable tmp dirs for dbt artifacts/logs (Windows volume perms workaround)
    ensure_tmp = BashOperator(
        task_id="ensure_tmp_dirs",
        bash_command="mkdir -p /tmp/dbt_logs /tmp/dbt_target && chmod -R 777 /tmp/dbt_logs /tmp/dbt_target",
    )

    # Quick sanity check that project loads and connection works
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} debug",
        env=DBT_ENV,
    )

    # Build silver (clean/standardize) models
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run --select path:models/silver",
        env=DBT_ENV,
    )

    # Build gold (star schema) models
    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run --select path:models/gold",
        env=DBT_ENV,
    )

    # Run tests (unique/not_null, etc.)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} test",
        env=DBT_ENV,
    )

    ensure_tmp >> dbt_debug >> dbt_run_silver >> dbt_run_gold >> dbt_test
