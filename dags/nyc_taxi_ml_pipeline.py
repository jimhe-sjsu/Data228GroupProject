"""
nyc_taxi_ml_pipeline.py — Airflow DAG for the ML pipeline track.

Orchestrates the end-to-end flow for building ML-ready Iceberg tables:

  ingest_yellow_tlc ──→ clean_yellow_taxi ──┐
                                             ├→ build_iceberg_tables → analyze_ml_data
  ingest_hvfhv_tlc  ──→ clean_hvfhv ────────┘

Yellow Taxi and HVFHV ingestion/cleaning run in parallel (independent schemas).
Iceberg build waits for both cleaned outputs before aggregating.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_MASTER = "data228groupproject-spark-master-1"

SPARK_SUBMIT = (
    f"docker exec"
    f"  -e PYSPARK_PYTHON=python3"
    f"  -e PYSPARK_DRIVER_PYTHON=python3"
    f"  {SPARK_MASTER}"
    f"  /opt/spark/bin/spark-submit"
    f"  --master 'local[1]'"
    f"  --driver-memory 2200m"
    f"  --driver-java-options '-XX:MaxDirectMemorySize=256m -XX:MaxMetaspaceSize=256m'"
    f"  --conf spark.sql.shuffle.partitions=12"
    f"  --conf spark.default.parallelism=12"
    f"  --conf 'spark.sql.parquet.enableVectorizedReader=false'"
    f"  --conf spark.memory.fraction=0.6"
    f"  --conf spark.memory.storageFraction=0.1"
)

ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
SPARK_SUBMIT_ICEBERG = f"{SPARK_SUBMIT}  --packages {ICEBERG_PKG}"

default_args = {
    "owner": "data228",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_ml_pipeline",
    default_args=default_args,
    description="NYC Taxi ML pipeline: Ingest → Clean → Iceberg EDA/ML tables → Analysis",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data228", "nyc_taxi", "ml", "iceberg"],
) as dag:

    # ── 1a. Download Yellow Taxi (2023-2025) from TLC → HDFS raw ────────────
    ingest_yellow = BashOperator(
        task_id="ingest_yellow_tlc",
        bash_command=(
            f"{SPARK_SUBMIT}"
            f"  /workspace/jobs/ingest_tlc_tripdata.py"
            f"  --dataset yellow"
            f"  --start-year 2023"
            f"  --end-year 2025"
            f"  --workers 2"
        ),
        execution_timeout=timedelta(hours=4),
    )

    # ── 1b. Download HVFHV (2023-2025) from TLC → HDFS raw ──────────────────
    ingest_hvfhv = BashOperator(
        task_id="ingest_hvfhv_tlc",
        bash_command=(
            f"{SPARK_SUBMIT}"
            f"  /workspace/jobs/ingest_tlc_tripdata.py"
            f"  --dataset fhvhv"
            f"  --start-year 2023"
            f"  --end-year 2025"
            f"  --workers 2"
        ),
        execution_timeout=timedelta(hours=4),
    )

    # ── 2a. Clean & standardize Yellow Taxi → shared schema ─────────────────
    clean_yellow = BashOperator(
        task_id="clean_yellow_taxi",
        bash_command=f"{SPARK_SUBMIT}  /workspace/jobs/run_yellow_taxi_clean_pipeline.py",
        execution_timeout=timedelta(hours=3),
    )

    # ── 2b. Clean & standardize HVFHV → shared schema ───────────────────────
    clean_hvfhv = BashOperator(
        task_id="clean_hvfhv",
        bash_command=f"{SPARK_SUBMIT}  /workspace/jobs/run_hvfhv_clean_pipeline.py",
        execution_timeout=timedelta(hours=3),
    )

    # ── 3. Aggregate both cleaned sources → Iceberg ML + EDA tables ─────────
    build_iceberg = BashOperator(
        task_id="build_iceberg_tables",
        bash_command=f"{SPARK_SUBMIT_ICEBERG}  /workspace/jobs/build_taxi_demand_ml_to_iceberg.py",
        execution_timeout=timedelta(hours=2),
    )

    # ── 4. Inspect ML table, export summary CSVs and charts ─────────────────
    analyze_ml = BashOperator(
        task_id="analyze_ml_data",
        bash_command=f"{SPARK_SUBMIT_ICEBERG}  /workspace/jobs/analyze_taxi_demand_ml.py",
        execution_timeout=timedelta(hours=1),
    )

    # Yellow and HVFHV ingest+clean run in parallel; Iceberg waits for both.
    ingest_yellow >> clean_yellow
    ingest_hvfhv >> clean_hvfhv
    [clean_yellow, clean_hvfhv] >> build_iceberg >> analyze_ml
