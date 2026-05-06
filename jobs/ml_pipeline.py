import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml_pipeline")


def create_spark_session():
    return (
        SparkSession.builder
        .appName("NYC Taxi ML Pipeline")

        # Iceberg config
        .config("spark.sql.catalog.nyc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nyc.type", "hadoop")
        .config("spark.sql.catalog.nyc.warehouse", "hdfs://namenode:9000/user/data/warehouse")

        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    try:
        # STEP 1: Read cleaned data
        logger.info("Reading cleaned data from HDFS")

        df = spark.read.parquet(
            "hdfs://namenode:9000/user/data/nyc_taxi/cleaned/yellow_tripdata_*"
        )

        # STEP 2: Aggregation (ML dataset)
        logger.info("Creating ML dataset (zone-hour demand)")

        ml_df = (
            df.groupBy(
                "PULocationID",
                "pickup_hour",
                "pickup_day_of_week",
                "pickup_month",
                "is_weekend"
            )
            .agg(count("*").alias("trip_count"))
        )

        logger.info(f"Aggregated dataset rows: {ml_df.count()}")

        # STEP 3: Write to Iceberg
        logger.info("Writing ML dataset to Iceberg table")

        ml_df.writeTo("nyc.taxi_demand_ml").createOrReplace()

        # STEP 4: Read back from Iceberg (validation)
        logger.info("Reading ML dataset from Iceberg")

        ml_df = spark.read.table("nyc.taxi_demand_ml")

        ml_df.show(5)

        # STEP 5: Export dataset for ML
        logger.info("Exporting ML dataset to CSV")

        output_path = "/workspace/output/ml_dataset"

        (
            ml_df.coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(output_path)
        )

        logger.info(f"ML dataset exported to {output_path}")

        print("\n===== PIPELINE COMPLETE =====")
        print("✔ Data cleaned using Spark")
        print("✔ Aggregated demand dataset created")
        print("✔ Stored in Iceberg table: nyc.taxi_demand_ml")
        print("✔ Exported for ML training (CSV)")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()