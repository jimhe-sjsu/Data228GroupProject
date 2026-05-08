"""
ingest_to_bronze.py — Local parquet files → HDFS Bronze layer.

Copies raw parquet files from /workspace/data/ into HDFS Bronze path,
skipping any files that already exist in HDFS.
"""
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingest_to_bronze")

sys.path.insert(0, os.path.dirname(__file__))
from io_utils import load_config, create_spark_session

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/workspace/config/pipeline_config.yaml")


def main():
    config = load_config(CONFIG_PATH)
    bronze_path = config["hdfs"]["bronze_path"]

    spark = create_spark_session(app_name="IngestToBronze", config=config)

    try:
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.java.net.URI("hdfs://namenode:9000"), conf
        )
        Path = jvm.org.apache.hadoop.fs.Path

        # Ensure bronze directory exists
        bronze_hdfs_path = Path(bronze_path)
        if not fs.exists(bronze_hdfs_path):
            fs.mkdirs(bronze_hdfs_path)
            logger.info(f"Created HDFS directory: {bronze_path}")

        # Find local parquet files
        local_data_dir = "/workspace/data"
        local_files = sorted([
            f for f in os.listdir(local_data_dir)
            if f.endswith(".parquet") and f.startswith("yellow_tripdata_")
        ])

        if not local_files:
            logger.warning(f"No parquet files found in {local_data_dir}")
            return

        logger.info(f"Found {len(local_files)} local parquet file(s)")

        ingested = 0
        skipped = 0

        for fname in local_files:
            hdfs_dest = Path(f"{bronze_path}/{fname}")

            if fs.exists(hdfs_dest):
                logger.info(f"  SKIP (exists): {fname}")
                skipped += 1
                continue

            local_path = os.path.join(local_data_dir, fname)
            local_hdfs_path = Path(f"file://{local_path}")

            fs.copyFromLocalFile(False, True, local_hdfs_path, hdfs_dest)
            logger.info(f"  INGESTED: {fname}")
            ingested += 1

        logger.info(
            f"Bronze ingestion complete. Ingested: {ingested}, Skipped: {skipped}, "
            f"Total in bronze: {ingested + skipped}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
