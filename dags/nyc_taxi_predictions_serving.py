"""NYC Taxi Predictions Serving — refresh the DuckDB file.

Reads the Iceberg ML parquet and rebuilds output/predictions.duckdb,
which the Streamlit dashboard and LLM agent consume.
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

sys.path.insert(0, os.path.dirname(__file__))
from _common import (  # noqa: E402
    COMMON_DAG_START_DATE,
    DEFAULT_ARGS,
    SPARK_MASTER_CONTAINER,
    with_tags,
)

REFRESH_CMD = (
    f"docker exec {SPARK_MASTER_CONTAINER} bash -c "
    f"'PYTHONPATH=/workspace/streamlit_app "
    f"python3 /workspace/jobs/generate_predictions.py'"
)

PREDICTIONS_DEFAULT_ARGS = {
    **DEFAULT_ARGS,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nyc_taxi_predictions_serving",
    default_args=PREDICTIONS_DEFAULT_ARGS,
    description="Rebuild the DuckDB serving table for Streamlit + LLM agent",
    schedule_interval=None,
    start_date=COMMON_DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=with_tags("serving", "duckdb", "streamlit"),
) as dag:

    refresh_predictions = BashOperator(
        task_id="refresh_predictions",
        bash_command=REFRESH_CMD,
        execution_timeout=timedelta(minutes=15),
    )
