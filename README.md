# NYC TLC Taxi Group Project

This repository contains two separate Spark pipelines for a DATA 228 group project:
- NYC TLC HVFHV 2023 trip records
- NYC TLC Yellow Taxi 2023 trip records

The intended stack is:
- HDFS for distributed storage
- Spark for data ingestion and transformation
- MySQL for final structured output tables

The first milestone is repository setup only. Docker, data ingestion, and Spark jobs will be added after the repo structure is in place.

## Planned Structure

- `jobs/` for PySpark batch scripts
- `notebooks/` for local EDA notebooks reading exported Spark outputs
- `sql/` for MySQL schema files and analysis queries
- `docs/` for project notes and report material
- `config/` for Hadoop and MySQL environment files used by Docker Compose

## Notes

- Raw TLC data files are not tracked in Git.
- Generated parquet outputs and local temporary artifacts are not tracked in Git.
- DuckDB is intentionally out of scope for this project.

## Docker Step 2

This repo now includes a minimal local cluster in [`docker-compose.yml`](/Users/jimhe/Documents/sjsu/DATA228/Data228GroupProject/docker-compose.yml).

Services included:
- HDFS: `namenode`, `datanode`
- YARN: `resourcemanager`, `nodemanager`, `historyserver`
- Spark: `spark-master`, `spark-worker`
- MySQL: `mysql`

The repository is mounted into the Spark master at `/workspace`, which is where future `spark-submit` commands should run from. HDFS will hold raw and intermediate data, and MySQL will hold final output tables written by Spark through JDBC in a later step.

### Start the cluster

```bash
docker compose up -d
```

### Check the main UIs

- HDFS NameNode: `http://localhost:9871`
- YARN ResourceManager: `http://localhost:8088`
- Spark Master: `http://localhost:8080`
- Spark Worker: `http://localhost:8082`
- MySQL: `localhost:3306`

### Open a shell in Spark

```bash
docker compose exec spark-master bash
```

Inside the container, the project files will be available at `/workspace`.

## Spark Workflow

This repo now keeps the two datasets as separate code paths.

### HVFHV pipeline

- [`jobs/ingest_hvfhv.py`](/Users/jimhe/Documents/sjsu/DATA228/Data228GroupProject/jobs/ingest_hvfhv.py)
- [`jobs/aggregate_hvfhv.py`](/Users/jimhe/Documents/sjsu/DATA228/Data228GroupProject/jobs/aggregate_hvfhv.py)
- [`notebooks/hvfhv_eda.ipynb`](/Users/jimhe/Documents/sjsu/DATA228/Data228GroupProject/notebooks/hvfhv_eda.ipynb)

### Yellow Taxi pipeline

- [`jobs/ingest_yellow_taxi.py`](/Users/jimhe/Documents/sjsu/DATA228/Data228GroupProject/jobs/ingest_yellow_taxi.py)
- [`jobs/aggregate_yellow_taxi.py`](/Users/jimhe/Documents/sjsu/DATA228/Data228GroupProject/jobs/aggregate_yellow_taxi.py)
- [`notebooks/yellow_taxi_eda.ipynb`](/Users/jimhe/Documents/sjsu/DATA228/Data228GroupProject/notebooks/yellow_taxi_eda.ipynb)

For the first runnable pass, year/month selection and HDFS paths are defined directly at the top of each Spark script. No separate pipeline config is required.

Required reference file:
- [`data/taxi_zone_lookup.csv`](/Users/jimhe/Documents/sjsu/DATA228/Data228GroupProject/data/taxi_zone_lookup.csv)

Run the jobs from the Spark container.

HVFHV:

```bash
docker compose exec spark-master bash -lc 'PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 /spark/bin/spark-submit /workspace/jobs/ingest_hvfhv.py'
docker compose exec spark-master bash -lc 'PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 /spark/bin/spark-submit /workspace/jobs/aggregate_hvfhv.py'
```

Yellow Taxi:

```bash
docker compose exec spark-master bash -lc 'PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 /spark/bin/spark-submit /workspace/jobs/ingest_yellow_taxi.py'
docker compose exec spark-master bash -lc 'PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 /spark/bin/spark-submit /workspace/jobs/aggregate_yellow_taxi.py'
```

Each EDA notebook reads the exported outputs from its own local folder:
- HVFHV: `output/eda/hvfhv/`
- Yellow Taxi: `output/eda/yellow/`

## Local Notebook Setup

The notebook is intended to run locally, not inside the Spark container.

Create a local virtual environment and install the notebook dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements-notebook.txt
```

Then open the notebook you want:

```bash
python3 -m jupyter notebook notebooks/hvfhv_eda.ipynb
python3 -m jupyter notebook notebooks/yellow_taxi_eda.ipynb
```

If you prefer VS Code, you can open either notebook directly after selecting the `.venv` interpreter.

### MySQL connection settings

- Host: `127.0.0.1`
- Port: `3306`
- Database: `data228`
- User: `data228user`

The local development credentials are stored in [`config/mysql.env`](/Users/jimhe/Documents/sjsu/DATA228/Data228GroupProject/config/mysql.env).

### Apple Silicon note

The Hadoop and Spark images in this stack are configured with `platform: linux/amd64` because these images currently run through Docker's amd64 emulation on Apple Silicon Macs.
