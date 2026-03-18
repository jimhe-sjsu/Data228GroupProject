import logging
import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

def _log_step(name, before, after):
    removed = before - after
    pct = (removed / before * 100) if before > 0 else 0.0
    logger.info(f"[{name}] before={before} after={after} removed={removed} ({pct:.2f}%)")
    return {"step": name, "before": before, "after": after, "removed": removed}

def audit_nulls(df, cols=None):
    # limit to specified columns (or all if None)
    target_cols = cols if cols is not None else df.columns
    agg_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in target_cols
    ]
    row = df.select(target_cols).agg(*agg_exprs).collect()[0]
    return {c: int(row[c]) for c in target_cols}

def fill_nulls(df):
    fill_map = {
        "passenger_count": 1.0,
        "RatecodeID": 1.0,
        "congestion_surcharge": 0.0,
        "airport_fee": 0.0,
        "store_and_fwd_flag": "N",
    }
    return df.fillna(fill_map)

def filter_invalid_year(df, valid_years):
    return df.filter(F.year(F.col("tpep_pickup_datetime")).isin(valid_years))

def filter_impossible_trips(df):
    return df.filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))

def filter_trip_duration(df, max_hours):
    # calculate trip duration in minutes and filter by max hours parameter
    duration_seconds = F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")
    duration_minutes = duration_seconds / 60.0
    max_minutes = max_hours * 60.0
    return df.filter((duration_minutes >= 1.0) & (duration_minutes <= max_minutes))

def filter_trip_distance(df, max_miles):
    return df.filter((F.col("trip_distance") > 0) & (F.col("trip_distance") <= max_miles))

def filter_passenger_count(df, max_passengers):
    return df.filter((F.col("passenger_count") >= 1) & (F.col("passenger_count") <= max_passengers))

def filter_fare_amount(df, min_fare):
    return df.filter((F.col("fare_amount") >= min_fare) & (F.col("total_amount") >= min_fare))

def filter_invalid_codes(df, vendor_ids, ratecodes, payment_types):
    return df.filter(
        F.col("VendorID").isin(vendor_ids)
        & F.col("RatecodeID").isin([float(r) for r in ratecodes])
        & F.col("payment_type").isin(payment_types)
    )

def run_all_cleaning_steps(df, config, raw_count=None):
    thresholds = config["thresholds"]

    step_defs = [
        ("Filter invalid year",   lambda d: filter_invalid_year(d, thresholds["valid_years"])),
        ("Filter impossible trips", lambda d: filter_impossible_trips(d)),
        ("Filter trip duration",  lambda d: filter_trip_duration(d, thresholds["max_trip_duration_hours"])),
        ("Filter trip distance",  lambda d: filter_trip_distance(d, thresholds["max_trip_distance_miles"])),
        ("Filter passenger count", lambda d: filter_passenger_count(d, thresholds["max_passenger_count"])),
        ("Filter fare amount",    lambda d: filter_fare_amount(d, thresholds["min_fare_amount"])),
        ("Filter invalid codes",  lambda d: filter_invalid_codes(
            d,
            thresholds["valid_vendor_ids"],
            thresholds["valid_ratecode_ids"],
            thresholds["valid_payment_types"],
        )),
    ]

    # Apply all filters lazily — zero intermediate count() calls.
    # The caller is responsible for counting once after caching the result.
    step_names = []
    for name, fn in step_defs:
        df = fn(df)
        step_names.append(name)
        logger.info(f"Applied filter: {name} (lazy — will scan on cache/write)")

    # Build step_log with placeholder counts; caller fills them after cache+count.
    step_log = [
        {"step": name, "before": None, "after": None, "removed": None}
        for name in step_names
    ]
    # The last entry gets the overall before→after summary (caller fills "after")
    step_log[-1]["before"] = raw_count

    return df, step_log, None  # cleaned_count resolved by caller
