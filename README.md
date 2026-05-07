# NYC Yellow Taxi Data Pipeline — DATA 228 Group Project

## Overview

This project builds a batch data pipeline for NYC TLC trip datasets. Yellow Taxi and High Volume FHV (HVFHV) both use the same 2023-2025 monthly range so the sources have matching file counts. The pipelines read monthly parquet files from HDFS, clean and transform the data using PySpark, and write cleaned output back to HDFS as parquet.

Everything runs inside Docker containers so we don't need to install Hadoop or Spark on our machines directly.

## Dataset

- **Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — Yellow Taxi and High Volume FHV trip records
- **Size:** raw parquet files are too large to track in Git
- **Tracked cleaning report:** generated from the latest local cleaning run

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
│   ├── ingest_tlc_tripdata.py   # Downloads TLC parquet files into HDFS
│   ├── run_yellow_taxi_clean_pipeline.py # Cleans Yellow Taxi data
│   ├── run_hvfhv_clean_pipeline.py       # Cleans High Volume FHV data
│   ├── schema.py                # Defines the unified target schema
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

The parquet files can have slightly different column types across months (for example, `VendorID` is INT in some files but BIGINT in others). The pipeline casts every file to a single unified schema defined in `schema.py` so there are no type mismatch errors when processing them.

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
- Pickup year is outside the configured valid range

All these thresholds are configurable in `config/pipeline_config.yaml`.

### 4. Feature Engineering

After cleaning, we add these columns to make downstream analysis easier:

| Column | Description |
|--------|-------------|
| `trip_duration_mins` | Trip length in minutes |
| `pickup_hour` | Hour of day the trip started (0–23) |
| `pickup_year` | Year the trip started |
| `pickup_day_of_week` | Day of the week (1=Sunday through 7=Saturday) |
| `pickup_month` | Month number (1–12) |
| `is_weekend` | True if Saturday or Sunday |
| `speed_mph` | Average speed from distance and duration |
| `payment_type_label` | Readable label like "Credit Card" or "Cash" |
| `ratecode_label` | "Standard", "JFK", "Newark", etc. |
| `vendor_label` | "Creative Mobile Technologies" or "VeriFone Inc." |
| `day_name` | "Monday", "Tuesday", etc. |

### 5. Output

- Cleaned parquet files are written to HDFS under `/user/data/nyc_taxi/cleaned/yellow_taxi/` and `/user/data/nyc_taxi/cleaned/fhvhv/`, one folder per month
- Charts saved to `docs/charts/`
- Cleaning report saved to `docs/cleaning_report.md`

## Cleaning Results

These values are from the tracked report. Re-run the pipeline after ingesting the full configured range to refresh them.

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

### 2. Ingest raw data to HDFS

Download the Yellow Taxi parquet files from the TLC website directly into HDFS:

```bash
docker compose exec \
  -e PYSPARK_PYTHON=python3 \
  -e PYSPARK_DRIVER_PYTHON=python3 \
  spark-master \
  /opt/spark/bin/spark-submit /workspace/jobs/ingest_tlc_tripdata.py \
  --start-year 2023 \
  --end-year 2025 \
  --workers 4
```

This writes the raw Yellow Taxi parquet files to `/user/data/nyc_taxi/raw/yellow_taxi/`.

To ingest matching High Volume FHV data, use the same year range with `--dataset fhvhv`:

```bash
docker compose exec \
  -e PYSPARK_PYTHON=python3 \
  -e PYSPARK_DRIVER_PYTHON=python3 \
  spark-master \
  /opt/spark/bin/spark-submit /workspace/jobs/ingest_tlc_tripdata.py \
  --dataset fhvhv \
  --start-year 2023 \
  --end-year 2025 \
  --workers 4
```

This writes HVFHV raw parquet files to `/user/data/nyc_taxi/raw/hvfhv/`. The TLC file prefix is `fhvhv_tripdata_YYYY-MM.parquet`.

### 3. Run the pipeline

We stop the Spark worker first to free up memory (the pipeline runs in local mode on the master):

```bash
docker compose stop spark-worker

docker compose exec \
  -e PYSPARK_PYTHON=python3 \
  -e PYSPARK_DRIVER_PYTHON=python3 \
  -e SPARK_MASTER_URL='local[1]' \
  -e SPARK_DRIVER_MEMORY=2200m \
  -e SPARK_SHUFFLE_PARTITIONS=12 \
  -e SPARK_DEFAULT_PARALLELISM=12 \
  spark-master \
  /opt/spark/bin/spark-submit \
  --master 'local[1]' \
  --driver-memory 2200m \
  --driver-java-options '-XX:MaxDirectMemorySize=256m -XX:MaxMetaspaceSize=256m' \
  --conf spark.sql.shuffle.partitions=12 \
  --conf spark.default.parallelism=12 \
  --conf 'spark.sql.parquet.enableVectorizedReader=false' \
  --conf spark.memory.fraction=0.6 \
  --conf spark.memory.storageFraction=0.1 \
  /workspace/jobs/run_yellow_taxi_clean_pipeline.py
```

The pipeline supports **resume** — if it gets killed mid-way, just run the same command again and it will skip months that already have standardized output in HDFS. Older cleaned folders that do not have the shared columns are reprocessed automatically.

The Yellow Taxi cleaned output is written under `/user/data/nyc_taxi/cleaned/yellow_taxi/`, with one folder per monthly input file.

The cleaned outputs include these shared standardized columns used by the Iceberg builder:

```text
source_id
source_name
pickup_datetime
dropoff_datetime
pickup_hour_ts
pickup_date
pickup_year
pickup_month
pickup_hour
pickup_day_of_week
is_weekend
PULocationID
DOLocationID
trip_duration_seconds
trip_duration_mins
trip_miles
speed_mph
```

To clean the HVFHV data:

```bash
docker compose stop spark-worker

docker compose exec \
  -e PYSPARK_PYTHON=python3 \
  -e PYSPARK_DRIVER_PYTHON=python3 \
  -e SPARK_MASTER_URL='local[1]' \
  -e SPARK_DRIVER_MEMORY=2200m \
  -e SPARK_SHUFFLE_PARTITIONS=12 \
  -e SPARK_DEFAULT_PARALLELISM=12 \
  spark-master \
  /opt/spark/bin/spark-submit \
  --master 'local[1]' \
  --driver-memory 2200m \
  --driver-java-options '-XX:MaxDirectMemorySize=256m -XX:MaxMetaspaceSize=256m' \
  --conf spark.sql.shuffle.partitions=12 \
  --conf spark.default.parallelism=12 \
  --conf 'spark.sql.parquet.enableVectorizedReader=false' \
  --conf spark.memory.fraction=0.6 \
  --conf spark.memory.storageFraction=0.1 \
  /workspace/jobs/run_hvfhv_clean_pipeline.py
```

The HVFHV cleaned output is written under `/user/data/nyc_taxi/cleaned/fhvhv/`, with one folder per monthly input file, and its report is written to `docs/hvfhv_cleaning_report.md`. It uses the same standardized columns as Yellow Taxi, so downstream aggregation can treat both sources the same way.

### 4. Build the combined ML and EDA Iceberg tables

After both Yellow Taxi and HVFHV cleaning have run, build the shared ML and EDA aggregate tables:

```bash
docker compose exec \
  -e PYSPARK_PYTHON=python3 \
  -e PYSPARK_DRIVER_PYTHON=python3 \
  spark-master \
  /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  /workspace/jobs/build_taxi_demand_ml_to_iceberg.py
```

This reads the standardized cleaned Yellow Taxi and HVFHV features, then writes one ML table and the EDA aggregate tables used by the analysis notebook. The ML dataset includes `source_id` and `pickup_year` so the same model can learn Yellow Taxi and HVFHV demand patterns together and test on 2025:

- `0` = Yellow Taxi
- `1` = HVFHV

The main ML output is written to the Iceberg table `nyc.taxi_demand_ml`. The same job also writes these EDA tables in Iceberg:

- `nyc.eda_cleaning_summary`
- `nyc.eda_curated_feature_profile`
- `nyc.eda_overall_hourly_demand`
- `nyc.eda_borough_hourly_demand`
- `nyc.eda_weekday_weekend_demand`
- `nyc.eda_weekday_hourly_demand`
- `nyc.eda_trip_metrics_by_hour`
- `nyc.eda_top_pickup_zones`
- `nyc.eda_top_dropoff_zones`

The job still exports `/workspace/output/ml_dataset` for quick CSV-based ML experiments, but Iceberg is the source of truth for the project tables.

Run the Spark MLlib RandomForest test from Iceberg:

```bash
docker compose exec \
  -e PYSPARK_PYTHON=python3 \
  -e PYSPARK_DRIVER_PYTHON=python3 \
  spark-master \
  /opt/spark/bin/spark-submit \
  --master 'local[1]' \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  /workspace/ml_local.py \
  --input-format iceberg \
  --iceberg-table nyc.taxi_demand_ml
```

By default, `ml_local.py` trains on 2023-2024 and tests on 2025.

### 5. Open the Spark MLlib demo notebook

The ML demo notebook is at `notebooks/taxi_demand_mllib_demo.ipynb`. For Iceberg mode, run it from the Spark container so the notebook can reach HDFS using the Docker service names.

Rebuild and restart the Spark containers after Dockerfile or Compose changes:

```bash
docker compose build spark-master spark-worker
docker compose up -d --force-recreate spark-master spark-worker datanode
```

Start Jupyter inside `spark-master`:

```bash
docker compose exec spark-master bash -lc '
cd /workspace &&
python3 -m notebook \
  --ip=0.0.0.0 \
  --port=8888 \
  --no-browser \
  --NotebookApp.token=data228 \
  --NotebookApp.password="" \
  --allow-root
'
```

Open the notebook server:

```text
http://127.0.0.1:8888/?token=data228
```

Then open:

```text
notebooks/taxi_demand_mllib_demo.ipynb
```

Use `INPUT_MODE = "iceberg"` when the local Docker/HDFS/Iceberg stack is running. To share the demo with a teammate who does not have the Iceberg table, send `output/taxi_demand_ml_parquet.zip`; after unzipping it into the project root, they can use `INPUT_MODE = "parquet"`.

### 6. Check the results

Open the HDFS web UI at **http://localhost:9871** and browse to `/user/data/nyc_taxi/cleaned/` to see the dataset folders.

Or query with PySpark:

```bash
docker compose exec -e PYSPARK_PYTHON=python3 -e PYSPARK_DRIVER_PYTHON=python3 spark-master \
  /opt/spark/bin/pyspark --master 'local[1]'
```

```python
df = spark.read.parquet("hdfs://namenode:9000/user/data/nyc_taxi/cleaned/yellow_taxi/yellow_tripdata_*")
df.printSchema()
df.show(5)
df.count()
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
