import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Optional



def write_csv(df: DataFrame, path: str) -> None:
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(path)
    )


def load_input(spark: SparkSession, input_path: str, input_format: str) -> DataFrame:
    if input_format.lower() == "parquet":
        return spark.read.parquet(input_path)
    if input_format.lower() == "csv":
        return (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(input_path)
        )
    raise ValueError(f"Unsupported input_format: {input_format}")


def normalize_columns(df: DataFrame) -> DataFrame:
    renamed = df
    column_map = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "RatecodeID": "rate_code_id",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "airport_fee": "airport_fee",
        "Airport_fee": "airport_fee",
    }

    for old, new in column_map.items():
        if old in renamed.columns and new not in renamed.columns:
            renamed = renamed.withColumnRenamed(old, new)

    return renamed


def add_features(df: DataFrame) -> DataFrame:
    df = df.withColumn("pickup_datetime", F.to_timestamp("pickup_datetime"))
    df = df.withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))

    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0,
    )

    df = df.withColumn("pickup_date", F.to_date("pickup_datetime"))
    df = df.withColumn("pickup_hour", F.hour("pickup_datetime"))
    df = df.withColumn("pickup_day_of_week", F.date_format("pickup_datetime", "E"))
    df = df.withColumn("pickup_month", F.date_format("pickup_datetime", "yyyy-MM"))
    df = df.withColumn("pickup_hour_ts", F.date_trunc("hour", F.col("pickup_datetime")))

    df = df.withColumn(
        "is_weekend",
        F.when(F.dayofweek("pickup_datetime").isin([1, 7]), F.lit("Weekend")).otherwise(F.lit("Weekday")),
    )

    df = df.withColumn(
        "avg_speed_mph",
        F.when(F.col("trip_duration_minutes") > 0,
               F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0))
         .otherwise(F.lit(None))
    )

    return df


def read_zone_lookup(spark: SparkSession, zone_lookup_path: str) -> DataFrame:
    z = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(zone_lookup_path)
    )

    expected_cols = {c.lower(): c for c in z.columns}

    location_col = expected_cols.get("locationid")
    borough_col = expected_cols.get("borough")
    zone_col = expected_cols.get("zone")

    if not location_col or not borough_col or not zone_col:
        raise ValueError("taxi_zone_lookup.csv must contain LocationID, Borough, Zone columns")

    return (
        z.select(
            F.col(location_col).alias("location_id"),
            F.col(borough_col).alias("borough"),
            F.col(zone_col).alias("zone"),
        )
    )


    
def export_cleaning_summary(spark: SparkSession, df: DataFrame, output_base: str) -> None:
    total_rows = df.count()

    null_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]
    nulls = df.select(null_exprs)

    summary_rows = [("total_rows", total_rows)]
    summary_df = spark.createDataFrame(summary_rows, ["metric", "value"])

    write_csv(summary_df, f"{output_base}/cleaning_summary_metrics")
    write_csv(nulls, f"{output_base}/cleaning_summary_nulls")
    
    
 


def export_curated_feature_profile(df: DataFrame, output_base: str) -> None:
    metrics = df.select(
        F.count("*").alias("row_count"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("fare_amount").alias("avg_fare_amount"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
        F.avg("avg_speed_mph").alias("avg_speed_mph"),
        F.min("pickup_datetime").alias("min_pickup_datetime"),
        F.max("pickup_datetime").alias("max_pickup_datetime"),
    )
    write_csv(metrics, f"{output_base}/curated_feature_profile")


def export_overall_hourly_demand(df: DataFrame, output_base: str) -> None:
    out = (
        df.groupBy("pickup_hour_ts")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("pickup_hour_ts")
    )
    write_csv(out, f"{output_base}/overall_hourly_demand")


def export_weekday_weekend_demand(df: DataFrame, output_base: str) -> None:
    out = (
        df.groupBy("is_weekend", "pickup_hour")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("is_weekend", "pickup_hour")
    )
    write_csv(out, f"{output_base}/weekday_weekend_demand")


def export_weekday_hourly_demand(df: DataFrame, output_base: str) -> None:
    out = (
        df.groupBy("pickup_day_of_week", "pickup_hour")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("pickup_day_of_week", "pickup_hour")
    )
    write_csv(out, f"{output_base}/weekday_hourly_demand")


def export_trip_metrics_by_hour(df: DataFrame, output_base: str) -> None:
    out = (
        df.groupBy("pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
            F.avg("avg_speed_mph").alias("avg_speed_mph"),
        )
        .orderBy("pickup_hour")
    )
    write_csv(out, f"{output_base}/trip_metrics_by_hour")


def export_zone_outputs(df: DataFrame, zones: DataFrame, output_base: str) -> None:
    pickup_joined = (
        df.join(zones, df.pu_location_id == zones.location_id, "left")
        .drop("location_id")
    )

    dropoff_joined = (
        df.join(zones, df.do_location_id == zones.location_id, "left")
        .drop("location_id")
    )

    top_pickup = (
        pickup_joined.groupBy("pu_location_id", "borough", "zone")
        .agg(F.count("*").alias("trip_count"))
        .orderBy(F.desc("trip_count"))
        .limit(20)
    )
    write_csv(top_pickup, f"{output_base}/top_pickup_zones")

    top_dropoff = (
        dropoff_joined.groupBy("do_location_id", "borough", "zone")
        .agg(F.count("*").alias("trip_count"))
        .orderBy(F.desc("trip_count"))
        .limit(20)
    )
    write_csv(top_dropoff, f"{output_base}/top_dropoff_zones")

    borough_hourly = (
        pickup_joined.groupBy("pickup_hour_ts", "borough")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("pickup_hour_ts", "borough")
    )
    write_csv(borough_hourly, f"{output_base}/borough_hourly_demand")

    hourly_zone = (
        pickup_joined.groupBy("pickup_hour_ts", "borough", "zone", "pu_location_id")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("pickup_hour_ts", "borough", "zone")
    )
    write_csv(hourly_zone, f"{output_base}/hourly_zone_demand")


def export_holiday_comparison(df: DataFrame, output_base: str) -> None:
    holiday_dates = ["2023-01-01"]
    compare_dates = ["2023-01-08"]

    tagged = (
        df.withColumn(
            "day_type",
            F.when(F.col("pickup_date").cast("string").isin(holiday_dates), F.lit("Holiday"))
             .when(F.col("pickup_date").cast("string").isin(compare_dates), F.lit("Comparison"))
             .otherwise(F.lit(None))
        )
        .filter(F.col("day_type").isNotNull())
    )

    out = (
        tagged.groupBy("day_type", "pickup_hour")
        .agg(F.count("*").alias("trip_count"))
        .orderBy("day_type", "pickup_hour")
    )

    write_csv(out, f"{output_base}/holiday_comparison")


def filter_month(df: DataFrame, month: Optional[str]) -> DataFrame:
    if not month:
        return df
    return df.filter(F.date_format("pickup_datetime", "yyyy-MM") == month)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input dataset path")
    parser.add_argument("--input-format", default="parquet", choices=["parquet", "csv"])
    parser.add_argument("--zone-lookup", required=True, help="Path to taxi_zone_lookup.csv")
    parser.add_argument("--output-base", required=True, help="Base output folder")
    parser.add_argument("--month", default=None, help="Optional month filter, e.g. 2023-01")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("YellowTaxiSparkEDA")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df = load_input(spark, args.input, args.input_format)
    df = normalize_columns(df)
    df = add_features(df)
    df = filter_month(df, args.month)

    zones = read_zone_lookup(spark, args.zone_lookup)

    export_cleaning_summary(spark, df, args.output_base)
    export_curated_feature_profile(df, args.output_base)
    export_overall_hourly_demand(df, args.output_base)
    export_weekday_weekend_demand(df, args.output_base)
    export_weekday_hourly_demand(df, args.output_base)
    export_trip_metrics_by_hour(df, args.output_base)
    export_zone_outputs(df, zones, args.output_base)
    export_holiday_comparison(df, args.output_base)

    spark.stop()


if __name__ == "__main__":
    main()