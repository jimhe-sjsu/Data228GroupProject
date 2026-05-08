"""
gold_to_mysql.py — HDFS Gold layer → MySQL serving layer.

Reads feature-engineered parquet from the Gold layer and writes
a subset of columns to the MySQL database for dashboard/BI access.
"""
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("gold_to_mysql")

sys.path.insert(0, os.path.dirname(__file__))
from io_utils import load_config, create_spark_session, write_to_mysql

CONFIG_PATH = os.environ.get("PIPELINE_CONFIG", "/workspace/config/pipeline_config.yaml")


def main():
    config = load_config(CONFIG_PATH)
    gold_path = config["hdfs"]["gold_path"]

    spark = create_spark_session(app_name="GoldToMySQL", config=config)

    try:
        gold_glob = f"{gold_path}/yellow_tripdata_*"
        logger.info(f"Reading Gold layer from: {gold_glob}")

        df = spark.read.option("mergeSchema", "false").parquet(gold_glob)
        row_count = df.count()
        logger.info(f"Gold layer row count: {row_count:,}")

        logger.info("Writing to MySQL...")
        write_to_mysql(df, config)
        logger.info("Gold → MySQL complete")

    except Exception as exc:
        logger.error(f"Gold → MySQL failed: {exc}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
