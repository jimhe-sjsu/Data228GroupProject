# NYC TLC HVFHV Group Project

This repository is the clean starting point for a DATA 228 group project based on the NYC TLC High Volume For-Hire Vehicle (HVFHV) 2023 dataset.

The intended stack is:
- HDFS for distributed storage
- Spark for data ingestion and transformation
- Hive for warehouse tables and SQL analysis

The first milestone is repository setup only. Docker, data ingestion, and Spark jobs will be added after the repo structure is in place.

## Planned Structure

- `jobs/` for PySpark batch scripts
- `sql/` for Hive DDL and analysis queries
- `docs/` for project notes and report material
- `config/` for Hadoop and Hive environment files used by Docker Compose

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
- Hive: `hive-metastore-postgresql`, `hive-metastore`, `hive-server`

The repository is mounted into the Spark master at `/workspace`, which is where future `spark-submit` commands should run from.

### Start the cluster

```bash
docker compose up -d
```

### Check the main UIs

- HDFS NameNode: `http://localhost:9870`
- YARN ResourceManager: `http://localhost:8088`
- Spark Master: `http://localhost:8080`
- Spark Worker: `http://localhost:8081`

### Open a shell in Spark

```bash
docker compose exec spark-master bash
```

Inside the container, the project files will be available at `/workspace`.
