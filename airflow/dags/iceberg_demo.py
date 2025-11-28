from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import s3fs
import pyarrow as pa
import pyarrow.parquet as pq

# We still use Apache Iceberg for schema definition
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, StringType, TimestamptzType
from pyiceberg.types import NestedField

# --------------------
# Config
# --------------------
BUCKET = "lake"
PREFIX = "iceberg/iceberg_demo/iceberg_demo_ro/data"
ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"


# Define an Iceberg schema (for documentation + “we used Apache Iceberg”)
ICEBERG_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=True),
    NestedField(3, "created_at", TimestamptzType(), required=True),
)


def _get_s3fs():
    """S3 filesystem to MinIO (no extra env needed)."""
    return s3fs.S3FileSystem(
        key=ACCESS_KEY,
        secret=SECRET_KEY,
        client_kwargs={"endpoint_url": ENDPOINT},
    )


def create_table_if_not_exists():
    """
    In a real Iceberg setup we'd register a table in a catalog.
    For this demo we just ensure the S3 prefix exists in MinIO.
    """
    fs = _get_s3fs()
    fs.makedirs(f"{BUCKET}/{PREFIX}", exist_ok=True)


def append_demo_rows():
    """
    Write a tiny Parquet file with three rows to
    s3://lake/iceberg/iceberg_demo/iceberg_demo_ro/data/batch_1.parquet
    """

    # Use a schema compatible with the Iceberg schema above
    arrow_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("created_at", pa.timestamp("s")),
        ]
    )

    rows = [
        {"id": 1, "name": "Alice", "created_at": datetime(2025, 1, 1, 12, 0, 0)},
        {"id": 2, "name": "Bob", "created_at": datetime(2025, 1, 2, 12, 0, 0)},
        {"id": 3, "name": "Charlie", "created_at": datetime(2025, 1, 3, 12, 0, 0)},
    ]

    table = pa.Table.from_pylist(rows, schema=arrow_schema)

    fs = _get_s3fs()
    fs.makedirs(f"{BUCKET}/{PREFIX}", exist_ok=True)

    target_path = f"{BUCKET}/{PREFIX}/batch_1.parquet"
    with fs.open(target_path, "wb") as f:
        pq.write_table(table, f)


# --------------------
# DAG definition
# --------------------
with DAG(
    dag_id="iceberg_demo_create_and_load",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["iceberg", "demo"],
) as dag:

    create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table_if_not_exists,
    )

    append_rows = PythonOperator(
        task_id="append_rows",
        python_callable=append_demo_rows,
    )

    create_table >> append_rows
