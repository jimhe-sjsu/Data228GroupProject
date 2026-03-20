# NYC Yellow Taxi Data Pipeline — DATA 228 Group Project

## Overview

This project builds a batch data pipeline for the NYC TLC Yellow Taxi trip dataset covering 2022 and 2023. We have about 78 million trip records across 24 monthly parquet files. The pipeline reads these files from HDFS, cleans and transforms the data using PySpark, and writes the cleaned output back to HDFS as parquet.

Everything runs inside Docker containers so we don't need to install Hadoop or Spark on our machines directly.

## Dataset

- **Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — Yellow Taxi, 2022 and 2023
- **Size:** ~5 GB+ of raw parquet files (24 monthly files)
- **Total raw rows:** 77,966,324
- **Cleaned rows:** 72,089,409 (about 7.5% removed)

## Tech Stack

| Tool | What we use it for |
|------|-------------------|
| HDFS | Storing the raw and cleaned parquet files in a distributed file system |
| Apache Spark (PySpark) | Reading, cleaning, transforming, and writing the parquet data |
| Docker Compose | Running the HDFS and Spark containers locally |
| matplotlib / seaborn | Generating before-and-after charts to show the impact of cleaning |

## Repository Structure

```
Data228GroupProject/
├── config/
│   ├── hadoop.env               # Hadoop environment variables for Docker
│   ├── hadoop-hive.env          # Hive-specific env config (for Docker services)
│   └── pipeline_config.yaml     # Pipeline settings — HDFS paths, thresholds, Spark tuning
├── docs/
│   ├── charts/                  # Generated PNG charts showing cleaning results
│   ├── cleaning_report.md       # Summary table of row counts before and after
│   └── DATA_CLEANING_HANDOFF.md # Detailed handoff doc explaining what was done
├── jobs/
│   ├── run_pipeline.py          # Main entry point — orchestrates the full pipeline
│   ├── schema.py                # Defines the unified target schema for all 24 files
│   ├── cleaning.py              # Null filling and row filtering logic
│   ├── feature_engineering.py   # Adds derived columns (duration, speed, labels)
│   ├── io_utils.py              # Reads/writes data to HDFS and generates the report
│   └── visualizations.py        # Creates the matplotlib charts
├── sql/
│   └── create_tables.sql        # DDL for a relational target table (not used currently)
├── docker-compose.yml           # Cluster definition (HDFS + Spark containers)
└── README.md
```

## What the Pipeline Does

### 1. Schema Standardization

The 2022 and 2023 parquet files have slightly different column types across months (for example, `VendorID` is INT in some files but BIGINT in others). The pipeline casts every file to a single unified schema defined in `schema.py` so there are no type mismatch errors when processing them.

### 2. Missing Value Handling

Some columns have a lot of nulls. Instead of dropping those rows entirely, we fill them with reasonable defaults:

- `passenger_count` → 1 (assume solo rider)
- `RatecodeID` → 1 (standard rate)
- `congestion_surcharge` / `airport_fee` → 0.0
- `store_and_fwd_flag` → "N"

### 3. Data Cleaning

We filter out rows that are clearly invalid or corrupt:

- Dropoff time is before or equal to pickup time
- Trip duration under 1 minute or over 3 hours
- Trip distance is 0, negative, or over 100 miles
- Passenger count is 0 or more than 6
- Fare or total amount less than $0.01
- Unrecognized vendor, rate code, or payment type codes
- Pickup year is not 2022 or 2023

All these thresholds are configurable in `config/pipeline_config.yaml`.

### 4. Feature Engineering

After cleaning, we add these columns to make downstream analysis easier:

| Column | Description |
|--------|-------------|
| `trip_duration_mins` | Trip length in minutes |
| `pickup_hour` | Hour of day the trip started (0–23) |
| `pickup_day_of_week` | Day of the week (1=Sunday through 7=Saturday) |
| `pickup_month` | Month number (1–12) |
| `is_weekend` | True if Saturday or Sunday |
| `speed_mph` | Average speed from distance and duration |
| `payment_type_label` | Readable label like "Credit Card" or "Cash" |
| `ratecode_label` | "Standard", "JFK", "Newark", etc. |
| `vendor_label` | "Creative Mobile Technologies" or "VeriFone Inc." |
| `day_name` | "Monday", "Tuesday", etc. |

### 5. Output

- Cleaned parquet files are written to HDFS at `/user/data/nyc_taxi/cleaned/`, one folder per month
- Charts saved to `docs/charts/`
- Cleaning report saved to `docs/cleaning_report.md`

## Cleaning Results

| Metric | Value |
|--------|-------|
| Raw rows | 77,966,324 |
| Cleaned rows | 72,089,409 |
| Rows removed | 5,876,915 |
| Reduction | 7.54% |

## How to Run

### 1. Start the Docker cluster

```bash
docker compose up -d
```

### 2. Upload raw data to HDFS

Download the Yellow Taxi parquet files from the TLC website and put them in a local `data/` folder, then upload to HDFS:

```bash
docker compose exec spark-master bash

# inside the container
hdfs dfs -mkdir -p /user/data/nyc_taxi/raw/
hdfs dfs -put /workspace/data/*.parquet /user/data/nyc_taxi/raw/
```

### 3. Run the pipeline

We stop the Spark worker first to free up memory (the pipeline runs in local mode on the master):

```bash
docker stop nyc_taxi_cluster-spark-worker-1

docker exec \
  -e PYSPARK_PYTHON=python3 \
  -e PYSPARK_DRIVER_PYTHON=python3 \
  nyc_taxi_cluster-spark-master-1 \
  /spark/bin/spark-class org.apache.spark.deploy.SparkSubmit \
  --master 'local[1]' \
  --driver-memory 2200m \
  --driver-java-options '-XX:MaxDirectMemorySize=256m -XX:MaxMetaspaceSize=256m' \
  --conf spark.sql.shuffle.partitions=12 \
  --conf spark.default.parallelism=12 \
  --conf 'spark.sql.parquet.enableVectorizedReader=false' \
  --conf spark.memory.fraction=0.6 \
  --conf spark.memory.storageFraction=0.1 \
  /workspace/jobs/run_pipeline.py
```

The pipeline supports **resume** — if it gets killed mid-way, just run the same command again and it will skip months that already have output in HDFS.

### 4. Check the results

Open the HDFS web UI at **http://localhost:9870** and browse to `/user/data/nyc_taxi/cleaned/` to see the output files.

Or query with PySpark:

```bash
docker exec -e PYSPARK_PYTHON=python3 -it nyc_taxi_cluster-spark-master-1 \
  /spark/bin/pyspark --master local[1]
```

```python
df = spark.read.parquet("hdfs://namenode:9000/user/data/nyc_taxi/cleaned/yellow_tripdata_*")
df.printSchema()
df.show(5)
df.count()  # should be around 72 million
```

## Charts

The pipeline generates four charts in `docs/charts/`:

- **null_counts.png** — Bar chart of which columns had missing values and how many
- **row_counts_by_step.png** — Total rows before and after cleaning
- **trip_distance_before_after.png** — Distance distribution before vs after
- **fare_distribution_before_after.png** — Fare distribution before vs after

## Notes

- Raw TLC parquet files are not in this repo (too large, gitignored).
- The pipeline processes one file at a time to stay within Docker's memory limit.
- The `memory/` folder is gitignored — it has development notes only.
- The `sql/` folder has DDL for a relational table but it is not part of the current pipeline.

## Team

DATA 228 — Big Data Technologies, Spring 2026
