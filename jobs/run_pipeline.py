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
    load_config, create_spark_session, read_parquet_from_hdfs,
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

def main():
    logger.info("Starting pipeline")
    config = load_config(CONFIG_PATH)
    spark = create_spark_session(app_name="NYCTaxiCleaning", config=config)

    try:
        raw_df = read_parquet_from_hdfs(spark, config["hdfs"]["input_paths"])
        raw_count = raw_df.count()
        logger.info(f"Raw row count: {raw_count}")

        canonical_schema = get_canonical_schema()
        df = enforce_schema(raw_df, canonical_schema)

        null_dict = audit_nulls(df)

        if HAS_MATPLOTLIB:
            before_distance_sample = df.select("trip_distance").sample(False, 0.01, seed=42).toPandas()
            before_fare_sample = df.select("fare_amount").sample(False, 0.01, seed=42).toPandas()

        df = fill_nulls(df)
        df, step_log = run_all_cleaning_steps(df, config)
        cleaned_count = df.count()

        df = run_feature_engineering(df)
        df.cache()

        if HAS_MATPLOTLIB:
            after_distance_sample = df.select("trip_distance").sample(False, 0.01, seed=42).toPandas()
            after_fare_sample = df.select("fare_amount").sample(False, 0.01, seed=42).toPandas()

        write_parquet_to_hdfs(df, config["hdfs"]["output_path"], config["hdfs"]["partition_cols"])

        try:
            write_to_mysql(df, config)
        except RuntimeError:
            logger.warning("MySQL write skipped (container not available)")

        if HAS_MATPLOTLIB:
            charts_dir = config["charts"]["output_dir"]
            plot_null_counts(null_dict, charts_dir)
            plot_row_counts_by_step(step_log, charts_dir)
            plot_trip_distance_histogram(before_distance_sample, after_distance_sample, charts_dir)
            plot_fare_distribution(before_fare_sample, after_fare_sample, charts_dir)
            logger.info(f"Charts saved to {charts_dir}")

        report_path = config["cleaning_report"]["output_path"]
        write_cleaning_report(step_log, raw_count, cleaned_count, report_path)

        logger.info(f"Done. Raw: {raw_count}, Cleaned: {cleaned_count}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
