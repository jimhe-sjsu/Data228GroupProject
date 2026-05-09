"""Shared configuration for the NYC Taxi Airflow DAGs."""
from __future__ import annotations

from datetime import datetime, timedelta

SPARK_MASTER_CONTAINER = "data228groupproject-spark-master-1"

SPARK_DRIVER_MEMORY = "2200m"
SPARK_SHUFFLE_PARTITIONS = 12
SPARK_DEFAULT_PARALLELISM = 12

SPARK_SUBMIT_BASE = (
    f"docker exec"
    f"  -e PYSPARK_PYTHON=python3"
    f"  -e PYSPARK_DRIVER_PYTHON=python3"
    f"  {SPARK_MASTER_CONTAINER}"
    f"  /opt/spark/bin/spark-submit"
    f"  --master 'local[1]'"
    f"  --driver-memory {SPARK_DRIVER_MEMORY}"
    f"  --driver-java-options '-XX:MaxDirectMemorySize=256m -XX:MaxMetaspaceSize=256m'"
    f"  --conf spark.sql.shuffle.partitions={SPARK_SHUFFLE_PARTITIONS}"
    f"  --conf spark.default.parallelism={SPARK_DEFAULT_PARALLELISM}"
    f"  --conf 'spark.sql.parquet.enableVectorizedReader=false'"
    f"  --conf spark.memory.fraction=0.6"
    f"  --conf spark.memory.storageFraction=0.1"
)

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
MYSQL_JDBC_PACKAGE = "mysql:mysql-connector-java:8.0.33"

SPARK_SUBMIT_ICEBERG = f"{SPARK_SUBMIT_BASE}  --packages {ICEBERG_PACKAGE}"
SPARK_SUBMIT_MYSQL = f"{SPARK_SUBMIT_BASE}  --packages {MYSQL_JDBC_PACKAGE}"

DEFAULT_ARGS = {
    "owner": "data228-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

COMMON_DAG_START_DATE = datetime(2026, 1, 1)
COMMON_DAG_TAGS_BASE = ("data228", "nyc_taxi")


def with_tags(*extra: str) -> list[str]:
    """Compose the DAG tags list with project-wide defaults."""
    return list(COMMON_DAG_TAGS_BASE) + list(extra)
