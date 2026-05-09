"""Build the DuckDB serving table from the Iceberg ML parquet.

Computes baseline predictions (historical average trip_count per
source × zone × month × day_of_week × hour bucket). Designed so a
trained model can replace _compute_predictions without changing
the downstream schema.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from config import (
    DEMAND_LEVELS,
    DEMAND_QUANTILES,
    DUCKDB_PATH,
    ML_PARQUET_DIR,
    ZONE_LOOKUP_CSV,
)

logger = logging.getLogger(__name__)

SERVING_TABLE = "current_taxi_demand_predictions"
RAW_ML_TABLE = "taxi_demand_ml_raw"
MODEL_VERSION = "baseline-historical-avg-v1"


def _ensure_parquet_available() -> Path:
    """Verify the extracted Iceberg ML parquet directory exists."""
    if not ML_PARQUET_DIR.exists():
        raise FileNotFoundError(
            f"Iceberg ML parquet not found at {ML_PARQUET_DIR}. "
            "Extract output/taxi_demand_ml_parquet.zip into .tmp/parquet_extract/ first."
        )
    return ML_PARQUET_DIR


def _connect(db_path: Path = DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def _compute_predictions(con: duckdb.DuckDBPyConnection, parquet_dir: Path) -> None:
    """Aggregate historical trip_count into baseline predictions per bucket."""
    parquet_glob = f"{parquet_dir}/*.parquet"

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {RAW_ML_TABLE} AS
        SELECT
            CAST(source_id AS INTEGER)            AS source_id,
            source_name,
            CAST(PULocationID AS BIGINT)          AS PULocationID,
            CAST(pickup_year AS INTEGER)          AS pickup_year,
            CAST(pickup_month AS INTEGER)         AS pickup_month,
            CAST(pickup_day AS INTEGER)           AS pickup_day,
            CAST(pickup_day_of_week AS INTEGER)   AS pickup_day_of_week,
            CAST(pickup_hour AS INTEGER)          AS pickup_hour,
            CAST(is_weekend AS BOOLEAN)           AS is_weekend,
            CAST(trip_count AS BIGINT)            AS trip_count
        FROM read_parquet('{parquet_glob}')
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE zone_lookup AS
        SELECT
            CAST(LocationID AS BIGINT) AS PULocationID,
            Borough                    AS borough,
            Zone                       AS zone
        FROM read_csv_auto('{ZONE_LOOKUP_CSV}', header=true)
        """
    )

    quantile_args = ", ".join(str(q) for q in DEMAND_QUANTILES)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _agg AS
        SELECT
            source_id,
            source_name,
            PULocationID,
            pickup_month,
            pickup_day_of_week,
            pickup_hour,
            ANY_VALUE(is_weekend)        AS is_weekend,
            AVG(CAST(trip_count AS DOUBLE))  AS predicted_trip_count
        FROM {RAW_ML_TABLE}
        GROUP BY source_id, source_name, PULocationID,
                 pickup_month, pickup_day_of_week, pickup_hour
        """
    )

    quantile_row = con.execute(
        f"SELECT QUANTILE_CONT(predicted_trip_count, [{quantile_args}]) FROM _agg"
    ).fetchone()
    q1, q2, q3 = quantile_row[0]

    now_iso = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {SERVING_TABLE} AS
        SELECT
            TIMESTAMP '{now_iso}'               AS prediction_timestamp,
            '{MODEL_VERSION}'                   AS model_version,
            a.source_id,
            a.source_name,
            a.PULocationID,
            COALESCE(z.borough, 'Unknown')      AS borough,
            COALESCE(z.zone, 'Unknown')         AS zone,
            a.pickup_month,
            a.pickup_day_of_week,
            a.pickup_hour,
            a.is_weekend,
            ROUND(a.predicted_trip_count, 2)    AS predicted_trip_count,
            CASE
                WHEN a.predicted_trip_count <= {q1} THEN '{DEMAND_LEVELS[0]}'
                WHEN a.predicted_trip_count <= {q2} THEN '{DEMAND_LEVELS[1]}'
                WHEN a.predicted_trip_count <= {q3} THEN '{DEMAND_LEVELS[2]}'
                ELSE '{DEMAND_LEVELS[3]}'
            END                                 AS demand_level
        FROM _agg a
        LEFT JOIN zone_lookup z USING (PULocationID)
        """
    )
    con.execute("DROP TABLE _agg")

    # Indexes on filter columns for fast sidebar queries
    con.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SERVING_TABLE}_filter "
        f"ON {SERVING_TABLE}(source_name, pickup_month, pickup_day_of_week, pickup_hour)"
    )
    con.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SERVING_TABLE}_zone "
        f"ON {SERVING_TABLE}(PULocationID)"
    )


def build_predictions(db_path: Path = DUCKDB_PATH) -> int:
    """Build the demand-prediction serving DuckDB table."""
    parquet_dir = _ensure_parquet_available()
    logger.info("Building DuckDB serving table at %s", db_path)
    with _connect(db_path) as con:
        _compute_predictions(con, parquet_dir)
        demand_rows = con.execute(f"SELECT COUNT(*) FROM {SERVING_TABLE}").fetchone()[0]
    logger.info("Serving: %s demand rows", demand_rows)
    return demand_rows


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rows = build_predictions()
    print(f"Built {rows:,} prediction rows -> {DUCKDB_PATH}")
