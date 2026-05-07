import argparse
import logging

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml_local_spark")


FEATURE_COLS = [
    "source_id",
    "PULocationID",
    "pickup_hour",
    "pickup_day_of_week",
    "pickup_month",
    "is_weekend_int",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Train a Spark MLlib tree model on the aggregated taxi demand dataset."
    )
    parser.add_argument(
        "--input-format",
        choices=["csv", "iceberg"],
        default="csv",
        help="Read the ML dataset from local CSV export or an Iceberg table.",
    )
    parser.add_argument(
        "--input",
        default="/workspace/output/ml_dataset",
        help="Path to the CSV dataset produced by jobs/build_taxi_demand_ml_to_iceberg.py.",
    )
    parser.add_argument(
        "--iceberg-table",
        default="nyc.taxi_demand_ml",
        help="Iceberg table to read when --input-format iceberg is used.",
    )
    parser.add_argument(
        "--iceberg-warehouse",
        default="hdfs://namenode:9000/user/data/warehouse",
        help="Iceberg Hadoop catalog warehouse path.",
    )
    parser.add_argument(
        "--trees",
        type=int,
        default=30,
        help="Number of trees for RandomForestRegressor.",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=8,
        help="Maximum depth for each tree.",
    )
    parser.add_argument(
        "--train-years",
        default="2023,2024",
        help="Comma-separated pickup years used for training.",
    )
    parser.add_argument(
        "--test-year",
        type=int,
        default=2025,
        help="Pickup year used for testing.",
    )
    return parser.parse_args()


def create_spark_session(args):
    builder = (
        SparkSession.builder
        .appName("NYC Taxi Spark MLlib Local Test")
    )

    if args.input_format == "iceberg":
        builder = (
            builder
            .config("spark.sql.catalog.nyc", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.nyc.type", "hadoop")
            .config("spark.sql.catalog.nyc.warehouse", args.iceberg_warehouse)
        )

    return builder.getOrCreate()


def normalize_dataset(df):
    if "source_id" not in df.columns:
        logger.warning(
            "source_id is missing; treating all rows as Yellow Taxi for backward compatibility."
        )
        df = df.withColumn("source_id", F.lit(0))

    required_cols = [
        "source_id",
        "PULocationID",
        "pickup_year",
        "pickup_hour",
        "pickup_day_of_week",
        "pickup_month",
        "is_weekend",
        "trip_count",
    ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError("Missing required columns: {}".format(", ".join(missing)))

    return (
        df.select(required_cols)
        .dropna(subset=required_cols)
        .withColumn("source_id", F.col("source_id").cast("int"))
        .withColumn("pickup_year", F.col("pickup_year").cast("int"))
        .withColumn("is_weekend_int", F.col("is_weekend").cast("int"))
        .withColumn("label", F.col("trip_count").cast("double"))
    )


def parse_years(value):
    years = [int(part.strip()) for part in value.split(",") if part.strip()]
    if not years:
        raise ValueError("--train-years must include at least one year")
    return years


def load_dataset(spark, args):
    if args.input_format == "iceberg":
        logger.info("Reading ML dataset from Iceberg table %s", args.iceberg_table)
        df = spark.read.table(args.iceberg_table)
    else:
        logger.info("Reading ML dataset from CSV path %s", args.input)
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(args.input)
        )

    return normalize_dataset(df)


def main():
    args = parse_args()
    spark = create_spark_session(args)

    try:
        df = load_dataset(spark, args)

        row_count = df.count()
        logger.info("Training rows available: %s", row_count)
        if row_count == 0:
            raise RuntimeError("No rows found in ML dataset: {}".format(args.input))

        train_years = parse_years(args.train_years)
        test_year = args.test_year
        train_df = df.filter(F.col("pickup_year").isin(train_years))
        test_df = df.filter(F.col("pickup_year") == test_year)
        train_count = train_df.count()
        test_count = test_df.count()
        logger.info("Train years: %s", train_years)
        logger.info("Test year: %s", test_year)
        logger.info("Train rows: %s", train_count)
        logger.info("Test rows: %s", test_count)
        if train_count == 0:
            raise RuntimeError("No training rows found for years: {}".format(train_years))
        if test_count == 0:
            raise RuntimeError("No test rows found for year: {}".format(test_year))

        pipeline = Pipeline(stages=[
            VectorAssembler(inputCols=FEATURE_COLS, outputCol="features"),
            RandomForestRegressor(
                featuresCol="features",
                labelCol="label",
                predictionCol="prediction",
                numTrees=args.trees,
                maxDepth=args.max_depth,
                seed=42,
            ),
        ])

        logger.info(
            "Training RandomForestRegressor with numTrees=%s maxDepth=%s",
            args.trees,
            args.max_depth,
        )
        model = pipeline.fit(train_df)

        predictions = model.transform(test_df).select(
            "source_id",
            "PULocationID",
            "pickup_year",
            "pickup_hour",
            "pickup_day_of_week",
            "pickup_month",
            "is_weekend",
            "trip_count",
            "label",
            "prediction",
        )

        rmse = RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="rmse",
        ).evaluate(predictions)
        mae = RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="mae",
        ).evaluate(predictions)
        r2 = RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="r2",
        ).evaluate(predictions)

        print("\n===== SPARK MLLIB TREE MODEL TEST COMPLETE =====")
        print("Input rows: {:,}".format(row_count))
        print("Train years: {}".format(",".join(str(year) for year in train_years)))
        print("Test year: {}".format(test_year))
        print("Train rows: {:,}".format(train_count))
        print("Test rows: {:,}".format(test_count))
        print("Model: RandomForestRegressor")
        print("RMSE: {:.4f}".format(rmse))
        print("MAE: {:.4f}".format(mae))
        print("R2: {:.4f}".format(r2))
        print("\nSample predictions:")
        predictions.orderBy("PULocationID", "pickup_month", "pickup_hour").show(
            10,
            truncate=False,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
