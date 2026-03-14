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

## Notes

- Raw TLC data files are not tracked in Git.
- Generated parquet outputs and local temporary artifacts are not tracked in Git.
- DuckDB is intentionally out of scope for this project.
