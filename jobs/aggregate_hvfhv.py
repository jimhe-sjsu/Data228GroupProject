from __future__ import annotations

import shutil
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

YEAR = 2023
MONTHS = [1]
CURATED_HDFS_BASE = "hdfs://namenode:9000/data/nyc_tlc/hvfhv/curated"
ANALYTICS_HDFS_BASE = "hdfs://namenode:9000/data/nyc_tlc/hvfhv/analytics"
LOCAL_EXPORT_BASE = Path("/workspace/output/eda")
ZONE_LOOKUP_PATH = "/workspace/data/taxi_zone_lookup.csv"


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("aggregate_hvfhv").getOrCreate()


def curated_hdfs_path(year: int, month: int) -> str:
    return f"{CURATED_HDFS_BASE}/year={year}/month={month:02d}"


def months_scope(year: int, months: list[int]) -> str:
    label = "-".join(f"{int(month):02d}" for month in months)
    return f"year={year}/months={label}"


def analytics_hdfs_path(dataset_name: str) -> str:
    return f"{ANALYTICS_HDFS_BASE}/{dataset_name}/{months_scope(YEAR, MONTHS)}"


def local_export_path(dataset_name: str) -> Path:
    return LOCAL_EXPORT_BASE / dataset_name


def read_curated_months(spark: SparkSession) -> DataFrame:
    dataframes = [spark.read.parquet(curated_hdfs_path(YEAR, month)) for month in MONTHS]
    return reduce(lambda left, right: left.unionByName(right), dataframes[1:], dataframes[0])


def read_zone_lookup(spark: SparkSession, zone_lookup_path: str) -> DataFrame:
    if not Path(zone_lookup_path).exists():
        raise FileNotFoundError(f"Zone lookup file not found: {zone_lookup_path}")

    return (
        spark.read.option("header", True)
        .csv(zone_lookup_path)
        .select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
            F.col("service_zone").alias("service_zone"),
        )
    )


def build_hourly_zone_demand(curated_df: DataFrame, zone_lookup_df: DataFrame) -> DataFrame:
    hourly_df = (
        curated_df.withColumn("pickup_hour_ts", F.date_trunc("hour", "pickup_datetime"))
        .groupBy("pickup_hour_ts", "PULocationID")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_duration_seconds").alias("avg_trip_duration_seconds"),
            F.avg("trip_miles").alias("avg_trip_miles"),
        )
        .withColumnRenamed("PULocationID", "location_id")
        .join(zone_lookup_df, on="location_id", how="left")
        .orderBy("pickup_hour_ts", "location_id")
    )
    return hourly_df


def build_overall_hourly_demand(curated_df: DataFrame) -> DataFrame:
    return (
        curated_df.withColumn("pickup_hour_ts", F.date_trunc("hour", "pickup_datetime"))
        .groupBy("pickup_hour_ts")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("pickup_hour_ts")
    )


def build_weekday_weekend_demand(curated_df: DataFrame) -> DataFrame:
    return (
        curated_df.groupBy("pickup_hour", "is_weekend")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("pickup_hour", "is_weekend")
    )


def build_top_pickup_zones(curated_df: DataFrame, zone_lookup_df: DataFrame) -> DataFrame:
    return (
        curated_df.groupBy("PULocationID")
        .agg(F.count("*").alias("trip_count"))
        .withColumnRenamed("PULocationID", "location_id")
        .join(zone_lookup_df, on="location_id", how="left")
        .orderBy(F.desc("trip_count"), "location_id")
        .limit(20)
    )


def build_top_dropoff_zones(curated_df: DataFrame, zone_lookup_df: DataFrame) -> DataFrame:
    return (
        curated_df.groupBy("DOLocationID")
        .agg(F.count("*").alias("trip_count"))
        .withColumnRenamed("DOLocationID", "location_id")
        .join(zone_lookup_df, on="location_id", how="left")
        .orderBy(F.desc("trip_count"), "location_id")
        .limit(20)
    )


def write_local_csv_export(spark: SparkSession, dataset_df: DataFrame, hdfs_csv_path: str, local_dir: Path) -> None:
    local_dir.parent.mkdir(parents=True, exist_ok=True)
    if local_dir.exists():
        shutil.rmtree(local_dir)

    dataset_df.coalesce(1).write.mode("overwrite").option("header", True).csv(hdfs_csv_path)

    jvm = spark._jvm
    uri = jvm.java.net.URI(hdfs_csv_path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, spark._jsc.hadoopConfiguration())
    j_path = jvm.org.apache.hadoop.fs.Path
    fs.copyToLocalFile(False, j_path(hdfs_csv_path), j_path(local_dir.as_uri()))


def write_dataset_outputs(spark: SparkSession, dataset_name: str, dataset_df: DataFrame) -> None:
    hdfs_parquet_path = analytics_hdfs_path(dataset_name)
    hdfs_csv_path = analytics_hdfs_path(f"{dataset_name}_csv")
    local_csv_path = local_export_path(dataset_name)

    dataset_df.write.mode("overwrite").parquet(hdfs_parquet_path)
    write_local_csv_export(spark, dataset_df, hdfs_csv_path, local_csv_path)


def main() -> None:
    spark = build_spark_session()

    try:
        curated_df = read_curated_months(spark)
        zone_lookup_df = read_zone_lookup(spark, ZONE_LOOKUP_PATH)

        datasets = {
            "hourly_zone_demand": build_hourly_zone_demand(curated_df, zone_lookup_df),
            "overall_hourly_demand": build_overall_hourly_demand(curated_df),
            "weekday_weekend_demand": build_weekday_weekend_demand(curated_df),
            "top_pickup_zones": build_top_pickup_zones(curated_df, zone_lookup_df),
            "top_dropoff_zones": build_top_dropoff_zones(curated_df, zone_lookup_df),
        }

        for dataset_name, dataset_df in datasets.items():
            print(f"Writing dataset: {dataset_name}")
            write_dataset_outputs(spark, dataset_name, dataset_df)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
