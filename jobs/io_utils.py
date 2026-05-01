import logging
import os
import yaml
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

def load_config(path):
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)

def create_spark_session(app_name="NYCTaxiCleaning", config=None):
    spark_cfg     = (config or {}).get("spark", {})
    master        = spark_cfg.get("master",             "local[1]")
    driver_memory = spark_cfg.get("driver_memory",      "2500m")
    shuffle_parts = spark_cfg.get("shuffle_partitions", 48)
    default_par   = spark_cfg.get("default_parallelism",48)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.memory",                      driver_memory)
        .config("spark.sql.shuffle.partitions",             str(shuffle_parts))
        .config("spark.default.parallelism",                str(default_par))
        .config("spark.memory.fraction",                    "0.6")
        .config("spark.memory.storageFraction",             "0.3")
        .config("spark.sql.parquet.mergeSchema",            "false")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    )

    # add cluster-mode configs only when not running locally
    if not master.startswith("local"):
        executor_memory = spark_cfg.get("executor_memory", "1500m")
        executor_cores  = spark_cfg.get("executor_cores",  2)
        cores_max       = spark_cfg.get("cores_max",       4)
        builder = (
            builder
            .config("spark.executor.memory",            executor_memory)
            .config("spark.executor.cores",             str(executor_cores))
            .config("spark.cores.max",                  str(cores_max))
            .config("spark.network.timeout",            "600s")
            .config("spark.executor.heartbeatInterval", "60s")
            .config("spark.rpc.message.maxSize",        "256")
            .config("spark.executorEnv.PYSPARK_PYTHON", "python3")
        )

    return builder.getOrCreate()

def list_hdfs_files_recursive(spark, hdfs_dir):
    """Return all parquet file paths under an HDFS directory (recursive).
    Returns [] if the directory does not exist.
    """
    jvm  = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs   = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(hdfs_dir.split("/user/")[0] + "//"),
        conf,
    )
    path = jvm.org.apache.hadoop.fs.Path(hdfs_dir)
    # Silently return empty list if the path doesn't exist yet
    if not fs.exists(path):
        return []
    file_iter = fs.listFiles(path, True)
    results   = []
    while file_iter.hasNext():
        status = file_iter.next()
        p = str(status.getPath())
        # Skip Spark's internal staging directories and zero-byte temp files
        if p.endswith(".parquet") and "_temporary" not in p:
            results.append(p)
    return results

def count_rows_from_parquet_metadata(spark, individual_files):
    """Read exact row counts from parquet file footers — no data scan needed."""
    jvm  = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    ParquetFileReader = jvm.org.apache.parquet.hadoop.ParquetFileReader
    Path = jvm.org.apache.hadoop.fs.Path
    total = 0
    for file_path in individual_files:
        reader = ParquetFileReader.open(conf, Path(file_path))
        try:
            blocks = reader.getFooter().getBlocks()
            for i in range(blocks.size()):
                total += blocks[i].getRowCount()
        finally:
            reader.close()
    return total

def _expand_glob(spark, glob_path):  # also importable as a public helper
    """Return a list of individual HDFS file paths matching a glob."""
    jvm    = spark._jvm
    conf   = spark._jsc.hadoopConfiguration()
    path   = jvm.org.apache.hadoop.fs.Path(glob_path)
    fs     = path.getFileSystem(conf)
    statuses = fs.globStatus(path)
    if statuses is None:
        return []
    return [str(s.getPath()) for s in statuses]

def read_parquet_from_hdfs(spark, paths, enforce_fn=None):
    # Expand every glob to individual file paths and read each file separately.
    # Spark 3.0 mergeSchema cannot reconcile bigint/int within a glob, and
    # providing a schema at read time does binary mapping (not a cast), which
    # also fails on type mismatches.  Reading one file at a time avoids both
    # problems: each file has a self-consistent schema, and enforce_fn (if given)
    # applies Spark SQL cast expressions to produce the canonical output schema.
    try:
        individual_files = []
        for glob_path in paths:
            individual_files.extend(_expand_glob(spark, glob_path))

        if not individual_files:
            raise RuntimeError(f"No parquet files found for paths: {paths}")

        logger.info(f"Found {len(individual_files)} parquet file(s) to read")

        def _read_one(file_path):
            df = spark.read.option("mergeSchema", "false").parquet(file_path)
            if enforce_fn is not None:
                df = enforce_fn(df)
            return df

        dfs = [_read_one(f) for f in individual_files]
        result = dfs[0]
        for df in dfs[1:]:
            result = result.union(df.select(result.columns))
        return result, individual_files
    except Exception as exc:
        raise RuntimeError(f"Failed to read Parquet from HDFS. Paths: {paths}. Error: {exc}") from exc

def write_parquet_to_hdfs(df, path, partition_cols):
    try:
        df.write.mode("overwrite").partitionBy(*partition_cols).parquet(path)
    except Exception as exc:
        raise RuntimeError(f"Failed to write Parquet to HDFS at {path}. Error: {exc}") from exc

def write_to_mysql(df, config):
    mysql_cfg = config["mysql"]
    jdbc_url = f"jdbc:mysql://{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['database']}"

    subset_cols = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "PULocationID", "DOLocationID", "payment_type",
        "fare_amount", "tip_amount", "total_amount",
        "congestion_surcharge", "airport_fee",
        "trip_duration_mins", "pickup_hour", "pickup_day_of_week",
        "pickup_month", "is_weekend", "speed_mph",
        "payment_type_label", "ratecode_label", "vendor_label", "day_name",
    ]

    try:
        (
            df.select(subset_cols)
            .write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", mysql_cfg["table"])
            .option("user", mysql_cfg["user"])
            .option("password", mysql_cfg["password"])
            .option("driver", mysql_cfg["driver"])
            .mode("overwrite")
            .save()
        )
    except Exception as exc:
        logger.error(f"MySQL write failed: {exc}")
        raise RuntimeError(f"Failed to write to MySQL: {exc}") from exc

def write_cleaning_report(step_log, raw_count, final_count, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    lines = [
        "# Data Cleaning Report",
        "",
        "## Cleaning Steps",
        "",
        "| # | Step | Rows Before | Rows After | Rows Removed | % Removed |",
        "|---|------|-------------|------------|--------------|-----------|",
    ]

    for idx, entry in enumerate(step_log, start=1):
        b, a, r = entry.get("before"), entry.get("after"), entry.get("removed")
        if b is not None and r is not None:
            pct_s = f"{(r / b * 100):.2f}%" if b > 0 else "0.00%"
            lines.append(f"| {idx} | {entry['step']} | {b:,} | {a:,} | {r:,} | {pct_s} |")
        else:
            lines.append(f"| {idx} | {entry['step']} | — | — | — | — |")

    total_removed = raw_count - final_count
    total_pct = (total_removed / raw_count * 100) if raw_count > 0 else 0.0
    lines += [
        "",
        "## Summary",
        "",
        f"- **Raw rows in:** {raw_count:,}",
        f"- **Cleaned rows out:** {final_count:,}",
        f"- **Total rows removed:** {total_removed:,}",
        f"- **Overall reduction:** {total_pct:.2f}%",
        "",
    ]

    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
