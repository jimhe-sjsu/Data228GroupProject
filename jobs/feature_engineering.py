import logging
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

def add_trip_duration(df):
    duration_seconds = F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")
    return df.withColumn("trip_duration_mins", F.round(duration_seconds / 60.0, 2))

def add_time_features(df):
    return (
        df.withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime")))
        .withColumn("pickup_day_of_week", F.dayofweek(F.col("tpep_pickup_datetime")))
        .withColumn("pickup_month", F.month(F.col("tpep_pickup_datetime")))
        .withColumn("is_weekend", F.dayofweek(F.col("tpep_pickup_datetime")).isin([1, 7]))
    )

def add_speed(df):
    hours = F.col("trip_duration_mins") / 60.0
    return df.withColumn(
        "speed_mph",
        F.when(hours > 0, F.round(F.col("trip_distance") / hours, 2)).otherwise(None)
    )

def add_label_columns(df):
    payment_labels = {
        1: "Credit Card", 2: "Cash", 3: "No Charge",
        4: "Dispute", 5: "Unknown", 6: "Voided Trip",
    }
    payment_col = F.lit("Other")
    for code, label in payment_labels.items():
        payment_col = F.when(F.col("payment_type") == code, F.lit(label)).otherwise(payment_col)

    ratecode_map = {
        1: "Standard", 2: "JFK", 3: "Newark",
        4: "Nassau/Westchester", 5: "Negotiated", 6: "Group Ride", 99: "Unknown",
    }
    ratecode_col = F.lit("Other")
    for code, label in ratecode_map.items():
        ratecode_col = F.when(F.col("RatecodeID") == float(code), F.lit(label)).otherwise(ratecode_col)

    vendor_col = (
        F.when(F.col("VendorID") == 1, F.lit("Creative Mobile Technologies"))
        .when(F.col("VendorID") == 2, F.lit("VeriFone Inc."))
        .otherwise(F.lit("Unknown"))
    )

    day_map = {
        1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday",
        5: "Thursday", 6: "Friday", 7: "Saturday",
    }
    day_col = F.lit("Unknown")
    for code, label in day_map.items():
        day_col = F.when(F.col("pickup_day_of_week") == code, F.lit(label)).otherwise(day_col)

    return (
        df.withColumn("payment_type_label", payment_col)
        .withColumn("ratecode_label", ratecode_col)
        .withColumn("vendor_label", vendor_col)
        .withColumn("day_name", day_col)
    )

def run_feature_engineering(df):
    df = add_trip_duration(df)
    df = add_time_features(df)
    df = add_speed(df)
    df = add_label_columns(df)
    return df
