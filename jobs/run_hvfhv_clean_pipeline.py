# -*- coding: utf-8 -*-
import logging
import os
import sys

import pyspark.sql.functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_hvfhv_pipeline")

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from io_utils import (  # noqa: E402
    _expand_glob,
    count_rows_from_parquet_metadata,
    create_spark_session,
    list_hdfs_files_recursive,
    load_config,
    write_cleaning_report,
)

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/workspace/config/pipeline_config.yaml")

HVFHV_NULL_AUDIT_COLS = [
    "hvfhs_license_num",
    "originating_base_num",
    "airport_fee",
    "tips",
    "driver_pay",
]

STANDARDIZED_OUTPUT_COLS = [
    "source_id",
    "source_name",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_hour_ts",
    "pickup_date",
    "pickup_year",
    "pickup_month",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",
    "PULocationID",
    "DOLocationID",
    "trip_duration_seconds",
    "trip_duration_mins",
    "trip_miles",
    "speed_mph",
]


def output_has_standardized_columns(spark, output_file_path):
    existing_cols = (
        spark.read.option("mergeSchema", "false").parquet(output_file_path).columns
    )
    if existing_cols != STANDARDIZED_OUTPUT_COLS:
        logger.info(
            "Existing output %s does not exactly match standardized columns",
            output_file_path,
        )
        return False
    return True


def get_hvfhv_schema():
    return StructType(
        [
            StructField("hvfhs_license_num", StringType(), True),
            StructField("dispatching_base_num", StringType(), True),
            StructField("originating_base_num", StringType(), True),
            StructField("request_datetime", TimestampType(), True),
            StructField("on_scene_datetime", TimestampType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("PULocationID", LongType(), True),
            StructField("DOLocationID", LongType(), True),
            StructField("trip_miles", DoubleType(), True),
            StructField("trip_time", LongType(), True),
            StructField("base_passenger_fare", DoubleType(), True),
            StructField("tolls", DoubleType(), True),
            StructField("bcf", DoubleType(), True),
            StructField("sales_tax", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("airport_fee", DoubleType(), True),
            StructField("tips", DoubleType(), True),
            StructField("driver_pay", DoubleType(), True),
            StructField("shared_request_flag", StringType(), True),
            StructField("shared_match_flag", StringType(), True),
            StructField("access_a_ride_flag", StringType(), True),
            StructField("wav_request_flag", StringType(), True),
            StructField("wav_match_flag", StringType(), True),
        ]
    )


def enforce_hvfhv_schema(df, schema):
    col_map = {c.lower(): c for c in df.columns}
    select_exprs = []
    for field in schema.fields:
        actual_col = col_map.get(field.name.lower())
        if actual_col:
            select_exprs.append(F.col(actual_col).cast(field.dataType).alias(field.name))
        else:
            select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(select_exprs)


def audit_nulls(df, cols):
    target_cols = [c for c in cols if c in df.columns]
    agg_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in target_cols
    ]
    row = df.select(target_cols).agg(*agg_exprs).collect()[0]
    return {c: int(row[c] or 0) for c in target_cols}


def fill_hvfhv_nulls(df):
    return df.fillna(
        {
            "originating_base_num": "Unknown",
            "tolls": 0.0,
            "bcf": 0.0,
            "sales_tax": 0.0,
            "congestion_surcharge": 0.0,
            "airport_fee": 0.0,
            "tips": 0.0,
            "driver_pay": 0.0,
            "shared_request_flag": "N",
            "shared_match_flag": "N",
            "access_a_ride_flag": "N",
            "wav_request_flag": "N",
            "wav_match_flag": "N",
        }
    )


def run_hvfhv_cleaning_steps(df, config):
    hvfhv_cfg = config["hvfhv"]
    valid_years = hvfhv_cfg["valid_years"]
    max_trip_miles = hvfhv_cfg["max_trip_miles"]
    min_trip_seconds = hvfhv_cfg["min_trip_seconds"]
    max_trip_seconds = hvfhv_cfg["max_trip_seconds"]
    min_base_fare = hvfhv_cfg["min_base_passenger_fare"]

    step_defs = [
        (
            "Filter required identifiers and timestamps",
            lambda d: d.filter(
                F.col("hvfhs_license_num").isNotNull()
                & F.col("pickup_datetime").isNotNull()
                & F.col("dropoff_datetime").isNotNull()
                & F.col("PULocationID").isNotNull()
                & F.col("DOLocationID").isNotNull()
            ),
        ),
        (
            "Filter invalid pickup year",
            lambda d: d.filter(F.year(F.col("pickup_datetime")).isin(valid_years)),
        ),
        (
            "Filter impossible trips",
            lambda d: d.filter(F.col("dropoff_datetime") > F.col("pickup_datetime")),
        ),
        (
            "Filter trip distance",
            lambda d: d.filter((F.col("trip_miles") > 0) & (F.col("trip_miles") <= max_trip_miles)),
        ),
        (
            "Filter trip duration",
            lambda d: d.filter(
                (F.col("trip_time") >= min_trip_seconds)
                & (F.col("trip_time") <= max_trip_seconds)
            ),
        ),
        (
            "Filter base passenger fare",
            lambda d: d.filter(F.col("base_passenger_fare") >= min_base_fare),
        ),
    ]

    step_names = []
    for name, fn in step_defs:
        df = fn(df)
        step_names.append(name)
        logger.info("Applied filter: %s (lazy - will scan on write)", name)

    return df, [{"step": name, "before": None, "after": None, "removed": None} for name in step_names]


def add_hvfhv_features(df):
    hours = F.col("trip_duration_seconds") / 3600.0
    company_col = (
        F.when(F.col("hvfhs_license_num") == "HV0002", F.lit("Juno"))
        .when(F.col("hvfhs_license_num") == "HV0003", F.lit("Uber"))
        .when(F.col("hvfhs_license_num") == "HV0004", F.lit("Via"))
        .when(F.col("hvfhs_license_num") == "HV0005", F.lit("Lyft"))
        .otherwise(F.lit("Other"))
    )

    return (
        df.withColumn("source_id", F.lit(1).cast("int"))
        .withColumn("source_name", F.lit("hvfhv"))
        .withColumn("pickup_datetime", F.col("pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast("timestamp"))
        .withColumn("pickup_hour_ts", F.date_trunc("hour", F.col("pickup_datetime")))
        .withColumn("pickup_date", F.to_date(F.col("pickup_datetime")))
        .withColumn("trip_duration_seconds", F.col("trip_time").cast("double"))
        .withColumn("trip_duration_mins", F.round(F.col("trip_duration_seconds") / 60.0, 2))
        .withColumn("pickup_hour", F.hour(F.col("pickup_datetime")))
        .withColumn("pickup_year", F.year(F.col("pickup_datetime")))
        .withColumn("pickup_day_of_week", F.dayofweek(F.col("pickup_datetime")))
        .withColumn("pickup_month", F.month(F.col("pickup_datetime")))
        .withColumn("is_weekend", F.dayofweek(F.col("pickup_datetime")).isin([1, 7]))
        .withColumn("speed_mph", F.when(hours > 0, F.round(F.col("trip_miles") / hours, 2)))
        .withColumn("hvfhv_company", company_col)
    )


def process_one_file(spark, file_path, config, schema, output_file_path):
    df = spark.read.option("mergeSchema", "false").parquet(file_path)
    df = enforce_hvfhv_schema(df, schema)
    df = fill_hvfhv_nulls(df)
    df, _ = run_hvfhv_cleaning_steps(df, config)
    df = add_hvfhv_features(df)
    df = df.select(*STANDARDIZED_OUTPUT_COLS)
    df.write.mode("overwrite").option("compression", "snappy").parquet(output_file_path)

    spark.catalog.clearCache()
    import gc as _gc

    _gc.collect()
    spark._jvm.System.gc()


def main():
    logger.info("Starting HVFHV cleaning pipeline")
    config = load_config(CONFIG_PATH)
    spark = create_spark_session(app_name="NYCTaxiHVFHVCleaning", config=config)

    try:
        hvfhv_cfg = config["hvfhv"]
        input_paths = hvfhv_cfg["input_paths"]
        output_path = hvfhv_cfg["output_path"]
        schema = get_hvfhv_schema()

        individual_files = []
        for glob_path in input_paths:
            individual_files.extend(_expand_glob(spark, glob_path))
        individual_files = sorted(individual_files)

        if not individual_files:
            raise RuntimeError("No HVFHV parquet files found under configured input_paths")

        logger.info("Found %s HVFHV raw parquet file(s)", len(individual_files))
        raw_count = count_rows_from_parquet_metadata(spark, individual_files)
        logger.info("HVFHV raw row count (from metadata): %s", f"{raw_count:,}")

        first_df = spark.read.option("mergeSchema", "false").parquet(individual_files[0])
        first_df = enforce_hvfhv_schema(first_df, schema)
        null_dict = audit_nulls(first_df.sample(False, 0.01, seed=42), HVFHV_NULL_AUDIT_COLS)
        logger.info("HVFHV null audit (first file 1%% sample): %s", null_dict)
        del first_df

        for idx, file_path in enumerate(individual_files):
            fname = os.path.basename(file_path)
            stem = os.path.splitext(fname)[0]
            output_file_path = f"{output_path}/{stem}"

            existing_output = list_hdfs_files_recursive(spark, output_file_path)
            if existing_output and output_has_standardized_columns(spark, output_file_path):
                logger.info("[%s/%s] Skipping %s (already in HDFS)", idx + 1, len(individual_files), fname)
                continue
            if existing_output:
                logger.info(
                    "[%s/%s] Reprocessing %s to refresh standardized columns",
                    idx + 1,
                    len(individual_files),
                    fname,
                )

            logger.info("[%s/%s] Processing %s -> %s", idx + 1, len(individual_files), fname, stem)
            process_one_file(spark, file_path, config, schema, output_file_path)
            logger.info("[%s/%s] Done: %s", idx + 1, len(individual_files), fname)

        out_files = list_hdfs_files_recursive(spark, output_path)
        cleaned_count = count_rows_from_parquet_metadata(spark, out_files)
        logger.info("HVFHV cleaned row count (from output metadata): %s", f"{cleaned_count:,}")

        step_log = [
            {
                "step": "All HVFHV cleaning filters combined",
                "before": raw_count,
                "after": cleaned_count,
                "removed": raw_count - cleaned_count,
            }
        ]
        write_cleaning_report(
            step_log,
            raw_count,
            cleaned_count,
            hvfhv_cfg["cleaning_report_path"],
        )

        logger.info("HVFHV pipeline complete. Raw: %s  Cleaned: %s", f"{raw_count:,}", f"{cleaned_count:,}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
