import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_pipeline")

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from schema import get_canonical_schema, enforce_schema
from cleaning import audit_nulls, fill_nulls, run_all_cleaning_steps
from feature_engineering import run_feature_engineering
from io_utils import (
    load_config, create_spark_session,
    _expand_glob,
    count_rows_from_parquet_metadata, list_hdfs_files_recursive,
    write_parquet_to_hdfs, write_to_mysql, write_cleaning_report
)

try:
    from visualizations import (
        plot_null_counts, plot_row_counts_by_step,
        plot_trip_distance_histogram, plot_fare_distribution
    )
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    logger.warning("matplotlib/seaborn not installed, skipping chart generation")

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/workspace/config/pipeline_config.yaml")

_NULL_AUDIT_COLS = [
    "passenger_count", "RatecodeID",
    "congestion_surcharge", "airport_fee", "store_and_fwd_flag",
]


def _process_one_file(spark, file_path, config, canonical_schema, output_file_path):
    """Read, clean, feature-engineer and write ONE parquet file (flat, no partitionBy).

    Writing without partitionBy avoids Spark's sort-for-dynamic-partition step, which
    materialises all rows (2.4M × 29 cols ≈ 600 MB) in memory at once and triggers the
    Docker OOM killer.  We write each input month as its own output parquet file instead.
    """
    df = spark.read.option("mergeSchema", "false").parquet(file_path)
    df = enforce_schema(df, canonical_schema)
    df = fill_nulls(df)
    df, _, _ = run_all_cleaning_steps(df, config)
    df = run_feature_engineering(df)

    # single-partition write (no shuffle/sort) — streaming row-group buffer ≤ 128 MB
    df.coalesce(1).write.mode("overwrite").parquet(output_file_path)

    # Aggressive cleanup between files to prevent cumulative JVM memory growth:
    # drop Spark plan/codegen caches, then request both Python and JVM full GC.
    spark.catalog.clearCache()
    import gc as _gc
    _gc.collect()
    spark._jvm.System.gc()   # triggers JVM full GC to reclaim old-gen objects


def main():
    logger.info("Starting pipeline")
    config = load_config(CONFIG_PATH)
    spark = create_spark_session(app_name="NYCTaxiCleaning", config=config)

    try:
        canonical_schema = get_canonical_schema()
        output_path = config["hdfs"]["output_path"]

        # ── Enumerate raw files ──────────────────────────────────────────────
        individual_files = []
        for glob_path in config["hdfs"]["input_paths"]:
            individual_files.extend(_expand_glob(spark, glob_path))

        if not individual_files:
            raise RuntimeError("No parquet files found under configured input_paths")

        logger.info(f"Found {len(individual_files)} raw parquet file(s)")

        # ── Raw row count from footer metadata (instant, no data scan) ───────
        raw_count = count_rows_from_parquet_metadata(spark, individual_files)
        logger.info(f"Raw row count (from metadata): {raw_count:,}")

        # ── Null audit & before-samples from first file only ─────────────────
        #    Limits memory: one file plan, 10 % sample, 5 columns
        first_df = spark.read.option("mergeSchema", "false").parquet(individual_files[0])
        first_df = enforce_schema(first_df, canonical_schema)

        null_dict = audit_nulls(
            first_df.sample(False, 0.10, seed=42),
            cols=_NULL_AUDIT_COLS,
        )
        logger.info(f"Null audit (first file 10% sample): {null_dict}")

        if HAS_MATPLOTLIB:
            before_distance_sample = (
                first_df.select("trip_distance").sample(False, 0.05, seed=42).toPandas()
            )
            before_fare_sample = (
                first_df.select("fare_amount").sample(False, 0.05, seed=42).toPandas()
            )

        # Free the first-file plan before the main loop
        spark.catalog.clearCache()
        del first_df

        # ── Main processing loop — one file at a time ─────────────────────────
        #    Each input month file becomes one output parquet file in output_path/.
        #    This avoids any cross-file union plan and the sort-for-dynamic-partition
        #    that is required when using partitionBy (which causes Docker OOM).
        for idx, file_path in enumerate(individual_files):
            fname = os.path.basename(file_path)
            # e.g. yellow_tripdata_2022-01.parquet → cleaned/yellow_tripdata_2022-01/
            stem = os.path.splitext(fname)[0]
            output_file_path = f"{output_path}/{stem}"

            # Resume support: skip files already written to HDFS
            already_done = list_hdfs_files_recursive(spark, output_file_path)
            if already_done:
                logger.info(
                    f"[{idx+1}/{len(individual_files)}] Skipping {fname} (already in HDFS)"
                )
                continue

            logger.info(
                f"[{idx+1}/{len(individual_files)}] Processing {fname} → {stem}"
            )
            _process_one_file(
                spark, file_path, config, canonical_schema, output_file_path
            )
            logger.info(f"[{idx+1}/{len(individual_files)}] Done: {fname}")

        # ── Cleaned row count from output footer metadata ─────────────────────
        out_files     = list_hdfs_files_recursive(spark, output_path)
        cleaned_count = count_rows_from_parquet_metadata(spark, out_files)
        logger.info(f"Cleaned row count (from output metadata): {cleaned_count:,}")

        removed_total = (raw_count - cleaned_count) if raw_count else None
        step_log = [
            {
                "step":    "All cleaning filters combined",
                "before":  raw_count,
                "after":   cleaned_count,
                "removed": removed_total,
            }
        ]

        # ── After-cleaning samples for charts ─────────────────────────────────
        # The cleaned output lives in named subdirs (yellow_tripdata_2022-01/...),
        # so read via glob to let Spark find the actual parquet files.
        cleaned_glob = f"{output_path}/yellow_tripdata_*"
        if HAS_MATPLOTLIB:
            cleaned_df = spark.read.option("mergeSchema", "false").parquet(cleaned_glob)
            after_distance_sample = (
                cleaned_df.select("trip_distance").sample(False, 0.01, seed=42).toPandas()
            )
            after_fare_sample = (
                cleaned_df.select("fare_amount").sample(False, 0.01, seed=42).toPandas()
            )

        # ── MySQL sink (optional — skip gracefully if unavailable) ────────────
        try:
            cleaned_df_for_mysql = spark.read.option("mergeSchema", "false").parquet(cleaned_glob)
            write_to_mysql(cleaned_df_for_mysql, config)
        except Exception as exc:
            logger.warning(f"MySQL write skipped: {exc}")

        # ── Charts ────────────────────────────────────────────────────────────
        if HAS_MATPLOTLIB:
            charts_dir = config["charts"]["output_dir"]
            plot_null_counts(null_dict, charts_dir)
            plot_row_counts_by_step(step_log, charts_dir)
            plot_trip_distance_histogram(before_distance_sample, after_distance_sample, charts_dir)
            plot_fare_distribution(before_fare_sample, after_fare_sample, charts_dir)
            logger.info(f"Charts saved to {charts_dir}")

        # ── Cleaning report ───────────────────────────────────────────────────
        report_path = config["cleaning_report"]["output_path"]
        write_cleaning_report(step_log, raw_count, cleaned_count, report_path)

        logger.info(f"Pipeline complete. Raw: {raw_count:,}  Cleaned: {cleaned_count:,}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
