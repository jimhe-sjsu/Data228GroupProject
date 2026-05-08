"""
bronze_to_silver.py — HDFS Bronze → Silver layer.

Reads raw parquet from the Bronze layer one file at a time,
applies schema enforcement, null filling, and data cleaning,
then writes the cleaned output to the Silver layer in HDFS.

Supports resume: skips months that already have output in Silver.
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
logger = logging.getLogger("bronze_to_silver")

sys.path.insert(0, os.path.dirname(__file__))
from io_utils import load_config, create_spark_session, list_hdfs_files_recursive, _expand_glob
from schema import get_canonical_schema, enforce_schema
from cleaning import fill_nulls, run_all_cleaning_steps

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/workspace/config/pipeline_config.yaml")


def _process_one_file(spark, file_path, config, canonical_schema, output_path):
    """Read one Bronze parquet file, clean it, and write to Silver."""
    df = spark.read.option("mergeSchema", "false").parquet(file_path)
    df = enforce_schema(df, canonical_schema)
    df = fill_nulls(df)
    df, _, _ = run_all_cleaning_steps(df, config)

    # Single-partition write to avoid Docker OOM from sort-for-dynamic-partition
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    # Aggressive cleanup between files
    spark.catalog.clearCache()
    gc.collect()
    spark._jvm.System.gc()


def main():
    config = load_config(CONFIG_PATH)
    bronze_path = config["hdfs"]["bronze_path"]
    silver_path = config["hdfs"]["silver_path"]
    canonical_schema = get_canonical_schema()

    spark = create_spark_session(app_name="BronzeToSilver", config=config)

    try:
        # Enumerate all parquet files in Bronze
        bronze_glob = f"{bronze_path}/yellow_tripdata_*.parquet"
        bronze_files = sorted(_expand_glob(spark, bronze_glob))

        if not bronze_files:
            logger.warning(f"No parquet files found in Bronze layer: {bronze_glob}")
            return

        logger.info(f"Found {len(bronze_files)} file(s) in Bronze layer")

        processed = 0
        skipped = 0

        for idx, file_path in enumerate(bronze_files):
            fname = os.path.basename(file_path)
            stem = os.path.splitext(fname)[0]  # e.g. yellow_tripdata_2022-01
            output_file_path = f"{silver_path}/{stem}"

            # Resume support: skip files already in Silver
            existing = list_hdfs_files_recursive(spark, output_file_path)
            if existing:
                logger.info(f"  [{idx+1}/{len(bronze_files)}] SKIP (exists in Silver): {fname}")
                skipped += 1
                continue

            logger.info(f"  [{idx+1}/{len(bronze_files)}] Processing: {fname}")
            _process_one_file(spark, file_path, config, canonical_schema, output_file_path)
            logger.info(f"  [{idx+1}/{len(bronze_files)}] Done: {fname} → {stem}")
            processed += 1

        logger.info(
            f"Bronze → Silver complete. Processed: {processed}, Skipped: {skipped}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
