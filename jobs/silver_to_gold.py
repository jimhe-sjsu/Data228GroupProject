"""
silver_to_gold.py — HDFS Silver → Gold layer.

Reads cleaned parquet from the Silver layer one file at a time,
applies feature engineering (duration, speed, time features, labels),
and writes the enriched output to the Gold layer in HDFS.

Supports resume: skips months that already have output in Gold.
"""
import logging
import os
import sys
import gc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("silver_to_gold")

sys.path.insert(0, os.path.dirname(__file__))
from io_utils import load_config, create_spark_session, list_hdfs_files_recursive
from feature_engineering import run_feature_engineering

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/workspace/config/pipeline_config.yaml")


def main():
    config = load_config(CONFIG_PATH)
    silver_path = config["hdfs"]["silver_path"]
    gold_path = config["hdfs"]["gold_path"]

    spark = create_spark_session(app_name="SilverToGold", config=config)

    try:
        # Enumerate Silver layer directories (each is a month like yellow_tripdata_2022-01/)
        jvm = spark._jvm
        conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(
            jvm.java.net.URI("hdfs://namenode:9000"), conf
        )
        Path = jvm.org.apache.hadoop.fs.Path

        silver_hdfs = Path(silver_path)
        if not fs.exists(silver_hdfs):
            logger.warning(f"Silver layer does not exist: {silver_path}")
            return

        # List subdirectories in Silver (each represents a month)
        statuses = fs.listStatus(silver_hdfs)
        silver_dirs = sorted([
            str(s.getPath()) for s in statuses
            if s.isDirectory() and "yellow_tripdata_" in str(s.getPath())
        ])

        if not silver_dirs:
            logger.warning("No monthly directories found in Silver layer")
            return

        logger.info(f"Found {len(silver_dirs)} month(s) in Silver layer")

        processed = 0
        skipped = 0

        for idx, silver_dir in enumerate(silver_dirs):
            dir_name = os.path.basename(silver_dir)  # e.g. yellow_tripdata_2022-01
            gold_output = f"{gold_path}/{dir_name}"

            # Resume support: skip months already in Gold
            existing = list_hdfs_files_recursive(spark, gold_output)
            if existing:
                logger.info(f"  [{idx+1}/{len(silver_dirs)}] SKIP (exists in Gold): {dir_name}")
                skipped += 1
                continue

            logger.info(f"  [{idx+1}/{len(silver_dirs)}] Processing: {dir_name}")

            df = spark.read.option("mergeSchema", "false").parquet(silver_dir)
            df = run_feature_engineering(df)
            df.coalesce(1).write.mode("overwrite").parquet(gold_output)

            logger.info(f"  [{idx+1}/{len(silver_dirs)}] Done: {dir_name}")
            processed += 1

            # Cleanup between files
            spark.catalog.clearCache()
            gc.collect()
            spark._jvm.System.gc()

        logger.info(
            f"Silver → Gold complete. Processed: {processed}, Skipped: {skipped}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
