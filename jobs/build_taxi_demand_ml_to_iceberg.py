import logging
import os
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from io_utils import (  # noqa: E402
    _expand_glob,
    count_rows_from_parquet_metadata,
    load_config,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("build_taxi_demand_ml_to_iceberg")


CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/workspace/config/pipeline_config.yaml")
YELLOW_CLEANED_PATH = "hdfs://namenode:9000/user/data/nyc_taxi/cleaned/yellow_taxi/yellow_tripdata_*"
HVFHV_CLEANED_PATH = "hdfs://namenode:9000/user/data/nyc_taxi/cleaned/fhvhv/fhvhv_tripdata_*"
ZONE_LOOKUP_PATH = os.environ.get("ZONE_LOOKUP_PATH", "/workspace/data/taxi_zone_lookup.csv")
ICEBERG_WAREHOUSE = "hdfs://namenode:9000/user/data/warehouse"
ML_ICEBERG_TABLE = "nyc.taxi_demand_ml"
ML_CSV_OUTPUT = "/workspace/output/ml_dataset"

EDA_TABLES = {
    "cleaning_summary": "nyc.eda_cleaning_summary",
    "curated_feature_profile": "nyc.eda_curated_feature_profile",
    "overall_hourly_demand": "nyc.eda_overall_hourly_demand",
    "borough_hourly_demand": "nyc.eda_borough_hourly_demand",
    "weekday_weekend_demand": "nyc.eda_weekday_weekend_demand",
    "weekday_hourly_demand": "nyc.eda_weekday_hourly_demand",
    "trip_metrics_by_hour": "nyc.eda_trip_metrics_by_hour",
    "top_pickup_zones": "nyc.eda_top_pickup_zones",
    "top_dropoff_zones": "nyc.eda_top_dropoff_zones",
}

PROFILE_COLUMNS = [
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

STANDARDIZED_COLUMN_TYPES = {
    "source_id": "int",
    "source_name": "string",
    "pickup_datetime": "timestamp",
    "dropoff_datetime": "timestamp",
    "pickup_hour_ts": "timestamp",
    "pickup_date": "date",
    "pickup_year": "int",
    "pickup_month": "int",
    "pickup_hour": "int",
    "pickup_day_of_week": "int",
    "is_weekend": "boolean",
    "PULocationID": "long",
    "DOLocationID": "long",
    "trip_duration_seconds": "double",
    "trip_duration_mins": "double",
    "trip_miles": "double",
    "speed_mph": "double",
}

STANDARDIZED_COLUMNS = list(STANDARDIZED_COLUMN_TYPES.keys())


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Build Taxi Demand ML and EDA Iceberg Tables")
        .config("spark.sql.catalog.nyc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nyc.type", "hadoop")
        .config("spark.sql.catalog.nyc.warehouse", ICEBERG_WAREHOUSE)
        .getOrCreate()
    )


def hdfs_path_exists(spark, path):
    j_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = j_path.getFileSystem(spark._jsc.hadoopConfiguration())
    statuses = fs.globStatus(j_path)
    return statuses is not None and len(statuses) > 0


def select_standardized_cleaned_columns(df, source_label):
    missing_cols = [col for col in STANDARDIZED_COLUMNS if col not in df.columns]
    if missing_cols:
        missing = ", ".join(missing_cols)
        raise RuntimeError(
            f"{source_label} cleaned data is missing standardized column(s): {missing}. "
            "Re-run its cleaning pipeline before building Iceberg tables."
        )

    return df.select(
        *[
            F.col(col_name).cast(data_type).alias(col_name)
            for col_name, data_type in STANDARDIZED_COLUMN_TYPES.items()
        ]
    )


def read_standardized_cleaned_data(spark):
    datasets = []

    if hdfs_path_exists(spark, YELLOW_CLEANED_PATH):
        logger.info("Reading Yellow Taxi cleaned data from %s", YELLOW_CLEANED_PATH)
        yellow_df = spark.read.parquet(YELLOW_CLEANED_PATH)
        datasets.append(select_standardized_cleaned_columns(yellow_df, "Yellow Taxi"))
    else:
        logger.warning("Yellow Taxi cleaned path not found: %s", YELLOW_CLEANED_PATH)

    if hdfs_path_exists(spark, HVFHV_CLEANED_PATH):
        logger.info("Reading HVFHV cleaned data from %s", HVFHV_CLEANED_PATH)
        hvfhv_df = spark.read.parquet(HVFHV_CLEANED_PATH)
        datasets.append(select_standardized_cleaned_columns(hvfhv_df, "HVFHV"))
    else:
        logger.warning("HVFHV cleaned path not found: %s", HVFHV_CLEANED_PATH)

    if not datasets:
        raise RuntimeError("No cleaned datasets found for Iceberg builder")

    combined_df = datasets[0]
    for df in datasets[1:]:
        combined_df = combined_df.unionByName(df)

    return combined_df.dropna(
        subset=[
            "source_id",
            "source_name",
            "pickup_datetime",
            "pickup_hour_ts",
            "pickup_year",
            "pickup_month",
            "pickup_hour",
            "pickup_day_of_week",
            "is_weekend",
            "PULocationID",
            "DOLocationID",
        ]
    )


def read_zone_lookup(spark):
    zones = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ZONE_LOOKUP_PATH)
    )
    col_map = {col.lower(): col for col in zones.columns}
    return zones.select(
        F.col(col_map["locationid"]).cast("long").alias("location_id"),
        F.col(col_map["borough"]).alias("borough"),
        F.col(col_map["zone"]).alias("zone"),
        F.col(col_map.get("service_zone", col_map["zone"])).alias("service_zone"),
    )


def raw_count_for_paths(spark, input_paths):
    files = []
    for path in input_paths:
        files.extend(_expand_glob(spark, path))
    if not files:
        return None
    return count_rows_from_parquet_metadata(spark, sorted(files))


def build_cleaning_summary(spark, df, config):
    raw_counts = {
        "yellow": raw_count_for_paths(spark, config["hdfs"]["input_paths"]),
        "hvfhv": raw_count_for_paths(spark, config["hvfhv"]["input_paths"]),
    }
    cleaned_counts = {
        row["source_name"]: row["trip_count"]
        for row in df.groupBy("source_name").agg(F.count("*").alias("trip_count")).collect()
    }

    rows = []
    for source_id, source_name in [(0, "yellow"), (1, "hvfhv")]:
        raw_count = raw_counts.get(source_name)
        cleaned_count = cleaned_counts.get(source_name, 0)
        metrics = [("curated_row_count", float(cleaned_count))]
        if raw_count is not None:
            removed_count = raw_count - cleaned_count
            removed_pct = (removed_count / raw_count * 100.0) if raw_count else 0.0
            metrics.extend(
                [
                    ("raw_row_count", float(raw_count)),
                    ("removed_row_count", float(removed_count)),
                    ("removed_pct", float(removed_pct)),
                ]
            )
        for metric, value in metrics:
            rows.append((source_id, source_name, metric, value))

    return spark.createDataFrame(rows, ["source_id", "source_name", "metric", "value"])


def build_cleaning_summary_from_ml(spark, ml_df, config):
    raw_counts = {
        "yellow": raw_count_for_paths(spark, config["hdfs"]["input_paths"]),
        "hvfhv": raw_count_for_paths(spark, config["hvfhv"]["input_paths"]),
    }
    cleaned_counts = {
        row["source_name"]: row["trip_count"]
        for row in (
            ml_df.groupBy("source_name")
            .agg(F.sum("trip_count").cast("long").alias("trip_count"))
            .collect()
        )
    }

    rows = []
    for source_id, source_name in [(0, "yellow"), (1, "hvfhv")]:
        raw_count = raw_counts.get(source_name)
        cleaned_count = cleaned_counts.get(source_name, 0)
        metrics = [("curated_row_count", float(cleaned_count))]
        if raw_count is not None:
            removed_count = raw_count - cleaned_count
            removed_pct = (removed_count / raw_count * 100.0) if raw_count else 0.0
            metrics.extend(
                [
                    ("raw_row_count", float(raw_count)),
                    ("removed_row_count", float(removed_count)),
                    ("removed_pct", float(removed_pct)),
                ]
            )
        for metric, value in metrics:
            rows.append((source_id, source_name, metric, value))

    return spark.createDataFrame(rows, ["source_id", "source_name", "metric", "value"])


def build_curated_feature_profile(df):
    data_types = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    agg_exprs = [F.count("*").alias("row_count")]
    for col_name in PROFILE_COLUMNS:
        agg_exprs.extend(
            [
                F.count(F.col(col_name)).alias(f"{col_name}__non_null"),
                F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(f"{col_name}__null"),
                F.approx_count_distinct(F.col(col_name)).alias(f"{col_name}__distinct"),
            ]
        )

    wide_df = df.groupBy("source_id", "source_name").agg(*agg_exprs)
    profile_rows = []
    for col_name in PROFILE_COLUMNS:
        profile_rows.append(
            F.struct(
                F.lit(col_name).alias("feature_name"),
                F.lit(data_types[col_name]).alias("data_type"),
                F.col(f"{col_name}__distinct").cast("long").alias("approx_distinct_count"),
                F.col(f"{col_name}__non_null").cast("long").alias("non_null_count"),
                F.col(f"{col_name}__null").cast("long").alias("null_count"),
                F.when(
                    F.col("row_count") > 0,
                    F.round(F.col(f"{col_name}__null") / F.col("row_count") * 100.0, 4),
                ).otherwise(F.lit(0.0)).alias("null_pct"),
                F.col("row_count").cast("long").alias("row_count"),
            )
        )

    return (
        wide_df.select("source_id", "source_name", F.explode(F.array(*profile_rows)).alias("profile"))
        .select("source_id", "source_name", "profile.*")
    )


def build_ml_table(df):
    return (
        df.withColumn("pickup_day", F.dayofmonth(F.col("pickup_date")))
        .groupBy(
            "source_id",
            "source_name",
            "PULocationID",
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "pickup_day",
            "pickup_hour",
            "pickup_day_of_week",
            "is_weekend",
        )
        .agg(F.count("*").alias("trip_count"))
    )


def pickup_hour_ts_expr():
    return F.to_timestamp(
        F.concat(
            F.col("pickup_date").cast("string"),
            F.lit(" "),
            F.format_string("%02d:00:00", F.col("pickup_hour")),
        )
    )


def build_overall_hourly_demand_from_ml(ml_df):
    return (
        ml_df.groupBy(
            "source_id",
            "source_name",
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "pickup_day_of_week",
            "pickup_hour",
        )
        .agg(F.sum("trip_count").cast("long").alias("trip_count"))
        .withColumn("pickup_hour_ts", pickup_hour_ts_expr())
        .select(
            "source_id",
            "source_name",
            "pickup_hour_ts",
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "pickup_day_of_week",
            "pickup_hour",
            "trip_count",
        )
    )


def build_overall_hourly_demand(df):
    return (
        df.groupBy(
            "source_id",
            "source_name",
            "pickup_hour_ts",
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "pickup_day_of_week",
            "pickup_hour",
        )
        .agg(F.count("*").alias("trip_count"))
    )


def build_borough_hourly_demand_from_ml(ml_df, zones):
    pickup_joined = ml_df.join(zones, ml_df.PULocationID == zones.location_id, "left")
    return (
        pickup_joined.groupBy(
            "source_id",
            "source_name",
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "pickup_day_of_week",
            "pickup_hour",
            "borough",
        )
        .agg(F.sum("trip_count").cast("long").alias("trip_count"))
        .withColumn("pickup_hour_ts", pickup_hour_ts_expr())
        .select(
            "source_id",
            "source_name",
            "pickup_hour_ts",
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "pickup_day_of_week",
            "pickup_hour",
            "borough",
            "trip_count",
        )
    )


def build_borough_hourly_demand(df, zones):
    pickup_joined = df.join(zones, df.PULocationID == zones.location_id, "left")
    return (
        pickup_joined.groupBy(
            "source_id",
            "source_name",
            "pickup_hour_ts",
            "pickup_date",
            "pickup_year",
            "pickup_month",
            "pickup_day_of_week",
            "pickup_hour",
            "borough",
        )
        .agg(F.count("*").alias("trip_count"))
    )


def build_weekday_weekend_demand_from_ml(ml_df):
    return (
        ml_df.groupBy("source_id", "source_name", "pickup_hour", "is_weekend")
        .agg(F.sum("trip_count").cast("long").alias("trip_count"))
    )


def build_weekday_weekend_demand(df):
    return (
        df.groupBy("source_id", "source_name", "pickup_hour", "is_weekend")
        .agg(F.count("*").alias("trip_count"))
    )


def build_weekday_hourly_demand_from_ml(ml_df):
    return (
        ml_df.groupBy(
            "source_id",
            "source_name",
            "pickup_day_of_week",
            "pickup_hour",
            "is_weekend",
        )
        .agg(F.sum("trip_count").cast("long").alias("trip_count"))
    )


def build_weekday_hourly_demand(df):
    return (
        df.groupBy(
            "source_id",
            "source_name",
            "pickup_day_of_week",
            "pickup_hour",
            "is_weekend",
        )
        .agg(F.count("*").alias("trip_count"))
    )


def build_trip_metrics_by_hour(df):
    return (
        df.groupBy("source_id", "source_name", "pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_duration_seconds").alias("avg_trip_duration_seconds"),
            F.avg("trip_miles").alias("avg_trip_miles"),
            F.avg("speed_mph").alias("avg_speed_mph"),
        )
    )


def build_top_zones(df, zones, location_col):
    counts = (
        df.select(
            "source_id",
            "source_name",
            F.col(location_col).cast("long").alias("location_id"),
        )
        .groupBy("source_id", "source_name", "location_id")
        .agg(F.count("*").alias("trip_count"))
    )
    joined = counts.join(zones, "location_id", "left")
    rank_window = Window.partitionBy("source_id").orderBy(F.desc("trip_count"), F.asc("location_id"))
    return (
        joined.withColumn("source_rank", F.row_number().over(rank_window))
        .filter(F.col("source_rank") <= 20)
    )


def build_top_pickup_zones_from_ml(ml_df, zones):
    counts = (
        ml_df.select(
            "source_id",
            "source_name",
            F.col("PULocationID").cast("long").alias("location_id"),
            "trip_count",
        )
        .groupBy("source_id", "source_name", "location_id")
        .agg(F.sum("trip_count").cast("long").alias("trip_count"))
    )
    joined = counts.join(zones, "location_id", "left")
    rank_window = Window.partitionBy("source_id").orderBy(F.desc("trip_count"), F.asc("location_id"))
    return (
        joined.withColumn("source_rank", F.row_number().over(rank_window))
        .filter(F.col("source_rank") <= 20)
    )


def write_iceberg_table(df, table_name):
    logger.info("Writing Iceberg table %s", table_name)
    df.writeTo(table_name).createOrReplace()


def write_ml_csv_export(ml_df):
    logger.info("Exporting ML dataset to CSV: %s", ML_CSV_OUTPUT)
    (
        ml_df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(ML_CSV_OUTPUT)
    )


def main():
    config = load_config(CONFIG_PATH)
    spark = create_spark_session()
    df = None
    ml_df = None

    try:
        logger.info("Reading standardized cleaned Yellow Taxi and HVFHV data")
        df = read_standardized_cleaned_data(spark)
        zones = read_zone_lookup(spark)

        ml_df = build_ml_table(df).persist(StorageLevel.MEMORY_AND_DISK)
        write_iceberg_table(ml_df, ML_ICEBERG_TABLE)
        write_ml_csv_export(ml_df)

        logger.info("Building optimized EDA Iceberg outputs")
        eda_outputs = {
            "cleaning_summary": build_cleaning_summary_from_ml(spark, ml_df, config),
            "curated_feature_profile": build_curated_feature_profile(df),
            "overall_hourly_demand": build_overall_hourly_demand_from_ml(ml_df),
            "borough_hourly_demand": build_borough_hourly_demand_from_ml(ml_df, zones),
            "weekday_weekend_demand": build_weekday_weekend_demand_from_ml(ml_df),
            "weekday_hourly_demand": build_weekday_hourly_demand_from_ml(ml_df),
            "trip_metrics_by_hour": build_trip_metrics_by_hour(df),
            "top_pickup_zones": build_top_pickup_zones_from_ml(ml_df, zones),
            "top_dropoff_zones": build_top_zones(df, zones, "DOLocationID"),
        }

        for name, table in EDA_TABLES.items():
            write_iceberg_table(eda_outputs[name], table)

        print("\n===== ICEBERG BUILD COMPLETE =====")
        print("ML table: {}".format(ML_ICEBERG_TABLE))
        print("EDA tables:")
        for table in EDA_TABLES.values():
            print(" - {}".format(table))

    finally:
        if ml_df is not None:
            ml_df.unpersist()
        if df is not None:
            df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
