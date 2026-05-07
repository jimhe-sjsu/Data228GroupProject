import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


EXPECTED_STANDARDIZED_COLUMNS = [
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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate cleaned standardized NYC Taxi parquet outputs."
    )
    parser.add_argument(
        "--yellow-path",
        default="hdfs://namenode:9000/user/data/nyc_taxi/cleaned/yellow_taxi/yellow_tripdata_*",
        help="Glob path for cleaned Yellow Taxi parquet folders.",
    )
    parser.add_argument(
        "--hvfhv-path",
        default="hdfs://namenode:9000/user/data/nyc_taxi/cleaned/fhvhv/fhvhv_tripdata_*",
        help="Glob path for cleaned HVFHV parquet folders.",
    )
    parser.add_argument(
        "--skip-yellow",
        action="store_true",
        help="Skip Yellow Taxi validation.",
    )
    parser.add_argument(
        "--skip-hvfhv",
        action="store_true",
        help="Skip HVFHV validation.",
    )
    return parser.parse_args()


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ValidateCleanedParquet")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def validate_dataset(spark, name, path):
    print("\n===== VALIDATING {} =====".format(name.upper()))
    print("Path: {}".format(path))

    df = spark.read.option("mergeSchema", "false").parquet(path)

    missing = [col for col in EXPECTED_STANDARDIZED_COLUMNS if col not in df.columns]
    print("Missing standardized columns: {}".format(missing if missing else "None"))

    row_count = df.count()
    print("Row count: {:,}".format(row_count))

    print("\nSchema for standardized columns:")
    df.select(EXPECTED_STANDARDIZED_COLUMNS).printSchema()

    print("\nQuality checks:")
    quality = df.select(
        F.sum(F.when(F.col("pickup_datetime").isNull(), 1).otherwise(0)).alias(
            "null_pickup_datetime"
        ),
        F.sum(F.when(F.col("dropoff_datetime").isNull(), 1).otherwise(0)).alias(
            "null_dropoff_datetime"
        ),
        F.sum(F.when(F.col("PULocationID").isNull(), 1).otherwise(0)).alias(
            "null_pu_location"
        ),
        F.sum(F.when(F.col("DOLocationID").isNull(), 1).otherwise(0)).alias(
            "null_do_location"
        ),
        F.sum(F.when(F.col("dropoff_datetime") <= F.col("pickup_datetime"), 1).otherwise(0)).alias(
            "bad_time_order"
        ),
        F.sum(F.when(F.col("trip_duration_seconds") < 60, 1).otherwise(0)).alias(
            "duration_under_60s"
        ),
        F.sum(F.when(F.col("trip_duration_seconds") > 10800, 1).otherwise(0)).alias(
            "duration_over_3h"
        ),
        F.sum(F.when(F.col("trip_miles") <= 0, 1).otherwise(0)).alias(
            "non_positive_miles"
        ),
        F.sum(F.when(F.col("trip_miles") > 100, 1).otherwise(0)).alias(
            "miles_over_100"
        ),
        F.sum(F.when(~F.col("pickup_year").isin([2023, 2024, 2025]), 1).otherwise(0)).alias(
            "bad_year"
        ),
    )
    quality.show(truncate=False)

    print("\nRows by year/source:")
    (
        df.groupBy("pickup_year", "source_name")
        .count()
        .orderBy("pickup_year", "source_name")
        .show(50, truncate=False)
    )

    if missing:
        raise RuntimeError(
            "{} cleaned data is missing standardized columns: {}".format(
                name, ", ".join(missing)
            )
        )

    if row_count == 0:
        raise RuntimeError("{} cleaned data has zero rows".format(name))


def main():
    args = parse_args()
    spark = create_spark_session()

    try:
        if not args.skip_yellow:
            validate_dataset(spark, "yellow", args.yellow_path)
        if not args.skip_hvfhv:
            validate_dataset(spark, "hvfhv", args.hvfhv_path)
        print("\n===== CLEANED PARQUET VALIDATION COMPLETE =====")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
