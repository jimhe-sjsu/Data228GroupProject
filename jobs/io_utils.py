import logging
import os
import yaml
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

def load_config(path):
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)

def create_spark_session(app_name="NYCTaxiCleaning", config=None):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.parquet.mergeSchema", "false")
        # disable vectorized reader so Spark uses the Java-based reader
        # which supports zStandard compression (some 2023 NYC Taxi files use it)
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    )
    return builder.getOrCreate()

def read_parquet_from_hdfs(spark, paths):
    # read each path separately then union to avoid cross-year schema conflicts
    # (e.g. VendorID is bigint in 2022 files, int in 2023 files)
    try:
        dfs = [spark.read.option("mergeSchema", "false").parquet(p) for p in paths]
        result = dfs[0]
        for df in dfs[1:]:
            # align columns to match the first dataframe before union
            result = result.union(df.select(result.columns))
        return result
    except Exception as exc:
        raise RuntimeError(f"Failed to read Parquet from HDFS. Paths: {paths}. Error: {exc}") from exc

def write_parquet_to_hdfs(df, path, partition_cols):
    try:
        df.write.mode("overwrite").partitionBy(*partition_cols).parquet(path)
    except Exception as exc:
        raise RuntimeError(f"Failed to write Parquet to HDFS at {path}. Error: {exc}") from exc

def write_to_mysql(df, config):
    mysql_cfg = config["mysql"]
    jdbc_url = f"jdbc:mysql://{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['database']}"

    subset_cols = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "PULocationID", "DOLocationID", "payment_type",
        "fare_amount", "tip_amount", "total_amount",
        "congestion_surcharge", "airport_fee",
        "trip_duration_mins", "pickup_hour", "pickup_day_of_week",
        "pickup_month", "is_weekend", "speed_mph",
        "payment_type_label", "ratecode_label", "vendor_label", "day_name",
    ]

    try:
        (
            df.select(subset_cols)
            .write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", mysql_cfg["table"])
            .option("user", mysql_cfg["user"])
            .option("password", mysql_cfg["password"])
            .option("driver", mysql_cfg["driver"])
            .mode("overwrite")
            .save()
        )
    except Exception as exc:
        logger.error(f"MySQL write failed: {exc}")
        raise RuntimeError(f"Failed to write to MySQL: {exc}") from exc

def write_cleaning_report(step_log, raw_count, final_count, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    lines = [
        "# Data Cleaning Report",
        "",
        "## Cleaning Steps",
        "",
        "| # | Step | Rows Before | Rows After | Rows Removed | % Removed |",
        "|---|------|-------------|------------|--------------|-----------|",
    ]

    for idx, entry in enumerate(step_log, start=1):
        pct = (entry["removed"] / entry["before"] * 100) if entry["before"] > 0 else 0.0
        lines.append(f"| {idx} | {entry['step']} | {entry['before']:,} | {entry['after']:,} | {entry['removed']:,} | {pct:.2f}% |")

    total_removed = raw_count - final_count
    total_pct = (total_removed / raw_count * 100) if raw_count > 0 else 0.0
    lines += [
        "",
        "## Summary",
        "",
        f"- **Raw rows in:** {raw_count:,}",
        f"- **Cleaned rows out:** {final_count:,}",
        f"- **Total rows removed:** {total_removed:,}",
        f"- **Overall reduction:** {total_pct:.2f}%",
        "",
    ]

    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
