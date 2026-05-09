"""NYC Taxi Medallion ETL — Bronze → Silver → Gold → MySQL.

Implements the medallion architecture for Yellow Taxi data, producing
a business-ready view in MySQL for BI consumers.
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, os.path.dirname(__file__))
from _common import (  # noqa: E402
    COMMON_DAG_START_DATE,
    DEFAULT_ARGS,
    SPARK_SUBMIT_BASE,
    SPARK_SUBMIT_MYSQL,
    with_tags,
)

with DAG(
    dag_id="nyc_taxi_medallion_etl",
    default_args=DEFAULT_ARGS,
    description="Bronze → Silver → Gold → MySQL refinement for Yellow Taxi data",
    schedule_interval=None,
    start_date=COMMON_DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=with_tags("medallion", "mysql", "spark"),
) as dag:

    with TaskGroup(group_id="refine") as tg_refine:
        ingest_to_bronze = BashOperator(
            task_id="ingest_to_bronze",
            bash_command=(
                f"{SPARK_SUBMIT_BASE}"
                f"  /workspace/jobs/ingest_to_bronze.py"
            ),
            execution_timeout=timedelta(hours=2),
        )

        bronze_to_silver = BashOperator(
            task_id="bronze_to_silver",
            bash_command=(
                f"{SPARK_SUBMIT_BASE}"
                f"  /workspace/jobs/bronze_to_silver.py"
            ),
            execution_timeout=timedelta(hours=2),
        )

        silver_to_gold = BashOperator(
            task_id="silver_to_gold",
            bash_command=(
                f"{SPARK_SUBMIT_BASE}"
                f"  /workspace/jobs/silver_to_gold.py"
            ),
            execution_timeout=timedelta(hours=2),
        )

        ingest_to_bronze >> bronze_to_silver >> silver_to_gold

    gold_to_mysql = BashOperator(
        task_id="gold_to_mysql",
        bash_command=(
            f"{SPARK_SUBMIT_MYSQL}"
            f"  /workspace/jobs/gold_to_mysql.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    tg_refine >> gold_to_mysql
