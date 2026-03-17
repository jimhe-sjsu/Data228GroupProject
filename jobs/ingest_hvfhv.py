from __future__ import annotations

import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

TMP_DOWNLOAD_DIR = Path("/tmp/hvfhv_downloads")
YEAR = 2023
MONTHS = [1]
RAW_HDFS_BASE = "hdfs://namenode:9000/data/nyc_tlc/hvfhv/raw"
CURATED_HDFS_BASE = "hdfs://namenode:9000/data/nyc_tlc/hvfhv/curated"
SELECT_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tolls",
    "tips",
    "driver_pay",
    "shared_request_flag",
    "shared_match_flag",
]


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ingest_hvfhv")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def raw_file_name(year: int, month: int) -> str:
    return f"fhvhv_tripdata_{year}-{month:02d}.parquet"


def month_url(year: int, month: int) -> str:
    return (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"{raw_file_name(year, month)}"
    )


def raw_hdfs_file_path(year: int, month: int) -> str:
    return f"{RAW_HDFS_BASE}/year={year}/month={month:02d}/{raw_file_name(year, month)}"


def curated_hdfs_path(year: int, month: int) -> str:
    return f"{CURATED_HDFS_BASE}/year={year}/month={month:02d}"


def download_month_file(year: int, month: int) -> Path:
    TMP_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    local_path = TMP_DOWNLOAD_DIR / raw_file_name(year, month)
    url = month_url(year, month)
    print(f"Downloading {url} -> {local_path}")
    urllib.request.urlretrieve(url, local_path)
    return local_path


def upload_to_hdfs(spark: SparkSession, local_path: Path, hdfs_target: str) -> None:
    jvm = spark._jvm
    uri = jvm.java.net.URI(hdfs_target)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, spark._jsc.hadoopConfiguration())
    j_path = jvm.org.apache.hadoop.fs.Path

    target_path = j_path(hdfs_target)
    parent_path = target_path.getParent()
    fs.mkdirs(parent_path)
    fs.copyFromLocalFile(False, True, j_path(local_path.as_uri()), target_path)


def clean_hvfhv(raw_df):
    available_columns = [column for column in SELECT_COLUMNS if column in raw_df.columns]
    missing_required = {
        "pickup_datetime",
        "dropoff_datetime",
        "PULocationID",
        "DOLocationID",
    } - set(available_columns)
    if missing_required:
        raise RuntimeError(f"Missing required columns: {sorted(missing_required)}")

    df = raw_df.select(*available_columns)
    df = (
        df.withColumn("pickup_datetime", F.to_timestamp("pickup_datetime"))
        .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("PULocationID").isNotNull())
        .filter(F.col("DOLocationID").isNotNull())
    )

    duration_from_timestamps = (
        F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")
    ).cast("double")
    if "trip_time" in df.columns:
        trip_duration = F.coalesce(F.col("trip_time").cast("double"), duration_from_timestamps)
    else:
        trip_duration = duration_from_timestamps

    df = (
        df.withColumn("trip_duration_seconds", trip_duration)
        .filter(F.col("trip_duration_seconds") > 0)
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_weekday_num", F.dayofweek("pickup_datetime"))
        .withColumn("is_weekend", F.col("pickup_weekday_num").isin(1, 7))
        .withColumn("year", F.year("pickup_datetime"))
        .withColumn("month", F.month("pickup_datetime"))
    )

    if "trip_miles" in df.columns:
        df = df.filter(F.col("trip_miles") >= 0)

    if "base_passenger_fare" in df.columns:
        df = df.filter(F.col("base_passenger_fare") >= 0)

    return df


def main() -> None:
    spark = build_spark_session()

    try:
        for month in MONTHS:
            local_file = download_month_file(YEAR, month)
            raw_hdfs_path = raw_hdfs_file_path(YEAR, month)
            curated_path = curated_hdfs_path(YEAR, month)

            print(f"Uploading {local_file} -> {raw_hdfs_path}")
            upload_to_hdfs(spark, local_file, raw_hdfs_path)

            print(f"Reading raw parquet from {raw_hdfs_path}")
            raw_df = spark.read.parquet(raw_hdfs_path)
            curated_df = clean_hvfhv(raw_df)

            print(f"Writing curated parquet -> {curated_path}")
            curated_df.write.mode("overwrite").parquet(curated_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
