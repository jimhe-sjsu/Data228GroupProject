import logging
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType,
    DoubleType, StringType, TimestampType
)

logger = logging.getLogger(__name__)

def get_canonical_schema():
    # uniform schema for all files so we don't get type mismatch errors
    return StructType([
        StructField("VendorID", LongType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
    ])

def enforce_schema(df, schema):
    # build a lowercase -> actual column name lookup to handle casing differences
    # across years (e.g. Airport_fee in 2022 vs airport_fee in 2023)
    col_map = {c.lower(): c for c in df.columns}
    select_exprs = []

    for field in schema.fields:
        actual_col = col_map.get(field.name.lower())
        if actual_col:
            select_exprs.append(F.col(actual_col).cast(field.dataType).alias(field.name))
        else:
            select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))

    return df.select(select_exprs)
