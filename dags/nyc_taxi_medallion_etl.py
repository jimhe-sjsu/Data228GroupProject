"""NYC Taxi Medallion ETL — Bronze → Silver → Gold → MySQL.

This DAG implements the classic medallion architecture for the Yellow
Taxi feed only. It runs *in parallel* with the ML pipeline DAG and
produces a relational, business-friendly view of the data in MySQL for
downstream BI consumers.

The ML pipeline (Iceberg-backed) and this Medallion track are
deliberately separate: they have different consumers, different SLAs,
and different rebuild characteristics. Coupling them into one DAG would
mean a slow ML rebuild blocks the cheap MySQL refresh and vice versa.
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


DAG_DOC = """
### NYC Taxi Medallion ETL

**Purpose:** Refine raw Yellow Taxi data through Bronze, Silver, and
Gold layers and export the curated Gold view to MySQL for relational
consumers (BI tools, ad-hoc dashboards, external services).

**Graph shape**
```
ingest_to_bronze ─▶ bronze_to_silver ─▶ silver_to_gold ─▶ gold_to_mysql
```

**Layer contract**
| Layer | Path | Schema discipline | Purpose |
|-------|------|-------------------|---------|
| Bronze | `/data/bronze/nyc_taxi/` | Untyped (raw bytes) | Faithful copy of source; reproducibility |
| Silver | `/data/silver/nyc_taxi/` | Canonical schema, validated | Cleaned analytical layer |
| Gold | `/data/gold/nyc_taxi/` | Schema + derived features | Business-ready features |
| MySQL | `data228.nyc_taxi_cleaned` | Same as Gold | Serving for relational consumers |

**Idempotency**
* All HDFS writes use `mode("overwrite")` per-month or skip-if-exists.
* MySQL write is `mode("overwrite")` — atomic table replacement, no
  partial-state risk.

**Owner:** data228-platform
"""

with DAG(
    dag_id="nyc_taxi_medallion_etl",
    default_args=DEFAULT_ARGS,
    description="Bronze → Silver → Gold → MySQL refinement for Yellow Taxi data",
    doc_md=DAG_DOC,
    schedule_interval=None,
    start_date=COMMON_DAG_START_DATE,
    catchup=False,
    # MySQL overwrite is atomic, but two concurrent runs would still
    # race on the JDBC handle — keep this single-runner.
    max_active_runs=1,
    tags=with_tags("medallion", "mysql", "spark"),
) as dag:

    # ── Refinement stage ────────────────────────────────────────────────────
    with TaskGroup(
        group_id="refine",
        tooltip="Bronze → Silver → Gold per-monthly-file refinement",
    ) as tg_refine:
        ingest_to_bronze = BashOperator(
            task_id="ingest_to_bronze",
            doc_md="Copy local raw Parquet files into the HDFS Bronze layer "
                   "**verbatim**. Skips files already present. The Bronze "
                   "layer is the immutable, audit-friendly copy of source.",
            bash_command=(
                f"{SPARK_SUBMIT_BASE}"
                f"  /workspace/jobs/ingest_to_bronze.py"
            ),
            execution_timeout=timedelta(hours=2),
        )

        bronze_to_silver = BashOperator(
            task_id="bronze_to_silver",
            doc_md="Per-month schema enforcement, null filling, and row-level "
                   "validation. Skips months already materialized in Silver "
                   "to keep re-runs cheap; force-rebuild requires deleting "
                   "the relevant Silver subdirectory first.",
            bash_command=(
                f"{SPARK_SUBMIT_BASE}"
                f"  /workspace/jobs/bronze_to_silver.py"
            ),
            execution_timeout=timedelta(hours=2),
        )

        silver_to_gold = BashOperator(
            task_id="silver_to_gold",
            doc_md="Feature engineering: trip duration, speed, day-of-week, "
                   "weekend flag, payment-type label, etc. Output is the "
                   "business-ready Gold layer.",
            bash_command=(
                f"{SPARK_SUBMIT_BASE}"
                f"  /workspace/jobs/silver_to_gold.py"
            ),
            execution_timeout=timedelta(hours=2),
        )

        ingest_to_bronze >> bronze_to_silver >> silver_to_gold

    # ── Serve stage (Gold → MySQL) ──────────────────────────────────────────
    gold_to_mysql = BashOperator(
        task_id="gold_to_mysql",
        doc_md="Export the Gold layer to MySQL using `mode('overwrite')`. "
               "MySQL JDBC driver is supplied via `--packages` at submit "
               "time so the spark-master image stays clean.",
        bash_command=(
            f"{SPARK_SUBMIT_MYSQL}"
            f"  /workspace/jobs/gold_to_mysql.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    tg_refine >> gold_to_mysql
