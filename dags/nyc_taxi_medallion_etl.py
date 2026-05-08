"""
nyc_taxi_medallion_etl.py — Airflow DAG for the Medallion ETL track.

Orchestrates the Bronze → Silver → Gold → MySQL pipeline for Yellow Taxi data:

  ingest_to_bronze → bronze_to_silver → silver_to_gold → gold_to_mysql
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

default_args = {
    "owner": "data228",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_medallion_etl",
    default_args=default_args,
    description="NYC Taxi Medallion ETL: Bronze → Silver → Gold → MySQL",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data228", "nyc_taxi", "medallion", "mysql"],
) as dag:

    # ── 1. Copy local parquet files → HDFS Bronze layer ─────────────────────
    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=f"{SPARK_SUBMIT}  /workspace/jobs/ingest_to_bronze.py",
        execution_timeout=timedelta(hours=2),
    )

    # ── 2. Bronze → Silver: schema enforcement + cleaning ───────────────────
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{SPARK_SUBMIT}  /workspace/jobs/bronze_to_silver.py",
        execution_timeout=timedelta(hours=2),
    )

    # ── 3. Silver → Gold: feature engineering ───────────────────────────────
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"{SPARK_SUBMIT}  /workspace/jobs/silver_to_gold.py",
        execution_timeout=timedelta(hours=2),
    )

    # ── 4. Gold → MySQL: export to serving layer ─────────────────────────────
    gold_to_mysql = BashOperator(
        task_id="gold_to_mysql",
        bash_command=(
            f"{SPARK_SUBMIT}"
            f"  --packages mysql:mysql-connector-java:8.0.33"
            f"  /workspace/jobs/gold_to_mysql.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    ingest_to_bronze >> bronze_to_silver >> silver_to_gold >> gold_to_mysql
