"""
nyc_taxi_predictions_serving.py — Airflow DAG for the serving track.

Refreshes the DuckDB serving table that the Streamlit app + LLM agent read.
The job runs inside the spark-master container (consistent with the other
DAGs in this repo) so /workspace is mounted to the project root and the
script can read the Iceberg ML parquet and write to output/predictions.duckdb.

  refresh_predictions  →  output/predictions.duckdb
                              ↑ Streamlit app + LLM agent both read this
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_MASTER = "data228groupproject-spark-master-1"

# DuckDB is embedded — no Spark JVM is needed for the serving step. We still
# run inside the spark-master container because it already has the workspace
# mounted at /workspace and the right Python interpreter on PATH.
# duckdb / pandas / pyarrow are baked into the spark-master image
# (see docker/spark/Dockerfile), so we don't pip-install on every trigger.
REFRESH_CMD = (
    f"docker exec {SPARK_MASTER} bash -c "
    f"'PYTHONPATH=/workspace/streamlit_app "
    f"python3 /workspace/jobs/generate_predictions.py'"
)

default_args = {
    "owner": "data228",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nyc_taxi_predictions_serving",
    default_args=default_args,
    description="Refresh DuckDB predictions for the Streamlit demo + LLM agent",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data228", "nyc_taxi", "serving", "duckdb"],
) as dag:

    refresh_predictions = BashOperator(
        task_id="refresh_predictions",
        bash_command=REFRESH_CMD,
        execution_timeout=timedelta(minutes=15),
    )
