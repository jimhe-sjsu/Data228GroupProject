"""NYC Taxi ML pipeline — ingest, clean, build Iceberg, analyze.

Yellow Taxi and HVFHV run in parallel (different schemas), then merge
into one Iceberg ML table for downstream consumption.
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, os.path.dirname(__file__))
from _common import (  # noqa: E402
    COMMON_DAG_START_DATE,
    DEFAULT_ARGS,
    SPARK_SUBMIT_BASE,
    SPARK_SUBMIT_ICEBERG,
    with_tags,
)

with DAG(
    dag_id="nyc_taxi_ml_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingest NYC TLC data, clean Yellow + HVFHV, build Iceberg ML table",
    schedule_interval=None,
    start_date=COMMON_DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=with_tags("ml", "iceberg", "spark"),
    params={
        "start_year": Param(
            2023, type="integer", minimum=2009, maximum=2030,
            title="Start year for ingest",
        ),
        "end_year": Param(
            2025, type="integer", minimum=2009, maximum=2030,
            title="End year for ingest",
        ),
        "ingest_workers": Param(
            2, type="integer", minimum=1, maximum=8,
            title="Parallel HTTP workers",
        ),
    },
) as dag:

    # Ingest (parallel — Yellow and HVFHV are independent sources)
    with TaskGroup(group_id="ingest") as tg_ingest:
        ingest_yellow = BashOperator(
            task_id="ingest_yellow_tlc",
            bash_command=(
                f"{SPARK_SUBMIT_BASE}"
                f"  /workspace/jobs/ingest_tlc_tripdata.py"
                f"  --dataset yellow"
                f"  --start-year {{{{ params.start_year }}}}"
                f"  --end-year {{{{ params.end_year }}}}"
                f"  --workers {{{{ params.ingest_workers }}}}"
            ),
            execution_timeout=timedelta(hours=4),
        )

        ingest_hvfhv = BashOperator(
            task_id="ingest_hvfhv_tlc",
            bash_command=(
                f"{SPARK_SUBMIT_BASE}"
                f"  /workspace/jobs/ingest_tlc_tripdata.py"
                f"  --dataset fhvhv"
                f"  --start-year {{{{ params.start_year }}}}"
                f"  --end-year {{{{ params.end_year }}}}"
                f"  --workers {{{{ params.ingest_workers }}}}"
            ),
            execution_timeout=timedelta(hours=4),
        )

    # Clean (parallel — different raw schemas per source)
    with TaskGroup(group_id="clean") as tg_clean:
        clean_yellow = BashOperator(
            task_id="clean_yellow_taxi",
            bash_command=(
                f"{SPARK_SUBMIT_BASE}"
                f"  /workspace/jobs/run_yellow_taxi_clean_pipeline.py"
            ),
            execution_timeout=timedelta(hours=3),
        )

        clean_hvfhv = BashOperator(
            task_id="clean_hvfhv",
            bash_command=(
                f"{SPARK_SUBMIT_BASE}"
                f"  /workspace/jobs/run_hvfhv_clean_pipeline.py"
            ),
            execution_timeout=timedelta(hours=3),
        )

    # Merge both cleaned sources into Iceberg ML table
    build_iceberg = BashOperator(
        task_id="build_iceberg_tables",
        bash_command=(
            f"{SPARK_SUBMIT_ICEBERG}"
            f"  /workspace/jobs/build_taxi_demand_ml_to_iceberg.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    analyze_ml = BashOperator(
        task_id="analyze_ml_data",
        bash_command=(
            f"{SPARK_SUBMIT_ICEBERG}"
            f"  /workspace/jobs/analyze_taxi_demand_ml.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    # Wiring
    ingest_yellow >> clean_yellow
    ingest_hvfhv >> clean_hvfhv
    [clean_yellow, clean_hvfhv] >> build_iceberg >> analyze_ml
