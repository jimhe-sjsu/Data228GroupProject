"""Build the DuckDB serving table from the historical Iceberg ML parquet.

Strategy: this module computes the *baseline* prediction defined in
`memory/ml_architecture_plan.md` — average historical trip_count for each
(source, zone, month, day_of_week, hour) bucket. When the trained XGBoost
model from step 8 is delivered, swap `_compute_predictions` to score the
model instead. Everything downstream (DuckDB schema, Streamlit, LLM agent)
stays identical.
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

# Reliability serving table — used by the "Reliable Income Recommender" view.
# Holds one row per (source × zone × month × dow × hour) with the four factors
# (mean demand, demand CV, multi-year trend, yellow market share). The map
# colors zones by a composite reliability_score that the UI recomputes
# whenever the driver moves the weight sliders.
RELIABILITY_TABLE = "current_zone_reliability"

# Bumped manually when the prediction logic changes. The trained-model swap
# should set this to e.g. "xgboost-v1" so downstream consumers can tell
# baseline averages apart from real ML output.
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
    """Aggregate historical trip_count → baseline predicted_trip_count per bucket.

    Each row in the source parquet is one (source, zone, year, month, day,
    hour) observation. We average across years and days to produce a
    "what's the typical demand for this month × day_of_week × hour"
    prediction that can then be filtered by the Streamlit selectors.
    """
    parquet_glob = f"{parquet_dir}/*.parquet"
    # Keep pickup_year + pickup_day in the raw table — we need them later for
    # multi-year trend (factor 3) and per-observation stddev (factor 2).
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

    # Indexes on filter columns. Streamlit hits the same WHERE pattern over
    # and over (source_name + month + dow + hour); without indexes every
    # filter change scans 895K rows. With them, sub-millisecond.
    con.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SERVING_TABLE}_filter "
        f"ON {SERVING_TABLE}(source_name, pickup_month, pickup_day_of_week, pickup_hour)"
    )
    con.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SERVING_TABLE}_zone "
        f"ON {SERVING_TABLE}(PULocationID)"
    )


def _compute_reliability(con: duckdb.DuckDBPyConnection) -> None:
    """Build the 4-factor reliability serving table.

    Factor 1 (demand)        : AVG(trip_count) across years/days for same bucket
    Factor 2 (reliability)   : 1 - (STDDEV/AVG) — high when demand is steady
    Factor 3 (trend)         : REGR_SLOPE(trip_count, pickup_year) — change/year
    Factor 4 (yellow_share)  : yellow_count / (yellow + hvfhv) per zone × time

    Each factor is also normalized to [0, 1] *within its time bucket* — that
    way zones at 3 AM aren't unfairly penalized just because absolute demand
    is low compared to 6 PM.
    """
    # ── Per-source aggregation: factors 1, 2, 3 ─────────────────────────────
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _per_source AS
        SELECT
            source_id, source_name, PULocationID,
            pickup_month, pickup_day_of_week, pickup_hour,
            ANY_VALUE(is_weekend)                                AS is_weekend,
            CAST(AVG(trip_count) AS DOUBLE)                      AS mean_demand,
            CAST(COALESCE(STDDEV(trip_count), 0) AS DOUBLE)      AS demand_stddev,
            CAST(
                COALESCE(STDDEV(trip_count), 0) /
                NULLIF(AVG(trip_count), 0)
                AS DOUBLE
            )                                                    AS demand_cv,
            -- REGR_SLOPE can return NULL (one observation) OR NaN (degenerate
            -- input, e.g. all years identical). COALESCE catches NULL but not
            -- NaN — the explicit ISNAN check below handles the NaN case so
            -- the downstream min/max normalization isn't poisoned.
            CAST(
                CASE
                    WHEN REGR_SLOPE(trip_count, pickup_year) IS NULL THEN 0.0
                    WHEN ISNAN(REGR_SLOPE(trip_count, pickup_year)) THEN 0.0
                    ELSE REGR_SLOPE(trip_count, pickup_year)
                END
                AS DOUBLE
            )                                                    AS trend_slope,
            COUNT(*)                                             AS observation_count
        FROM {RAW_ML_TABLE}
        GROUP BY source_id, source_name, PULocationID,
                 pickup_month, pickup_day_of_week, pickup_hour
        """
    )

    # ── Cross-source split: factor 4 ────────────────────────────────────────
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _split AS
        SELECT
            PULocationID, pickup_month, pickup_day_of_week, pickup_hour,
            SUM(CASE WHEN source_name = 'yellow' THEN trip_count ELSE 0 END) AS yellow_total,
            SUM(CASE WHEN source_name = 'hvfhv'  THEN trip_count ELSE 0 END) AS hvfhv_total
        FROM {RAW_ML_TABLE}
        GROUP BY PULocationID, pickup_month, pickup_day_of_week, pickup_hour
        """
    )

    # ── Bucket-wise normalization windows for the four factors ─────────────
    # We normalize within each (source, month, dow, hour) bucket so the score
    # compares zones against their peers at the same time, not globally.
    now_iso = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {RELIABILITY_TABLE} AS
        WITH joined AS (
            SELECT
                p.*,
                CAST(s.yellow_total AS BIGINT) AS yellow_total,
                CAST(s.hvfhv_total  AS BIGINT) AS hvfhv_total,
                CAST(
                    s.yellow_total::DOUBLE
                    / NULLIF(s.yellow_total + s.hvfhv_total, 0)
                    AS DOUBLE
                ) AS yellow_share
            FROM _per_source p
            JOIN _split s USING (PULocationID, pickup_month, pickup_day_of_week, pickup_hour)
        ),
        bucket_stats AS (
            SELECT
                source_name, pickup_month, pickup_day_of_week, pickup_hour,
                MIN(mean_demand)   AS dmn, MAX(mean_demand)   AS dmx,
                MIN(demand_cv)     AS cmn, MAX(demand_cv)     AS cmx,
                MIN(trend_slope)   AS tmn, MAX(trend_slope)   AS tmx
            FROM joined
            GROUP BY source_name, pickup_month, pickup_day_of_week, pickup_hour
        )
        SELECT
            TIMESTAMP '{now_iso}'                                AS prediction_timestamp,
            '{MODEL_VERSION}'                                    AS model_version,
            j.source_id, j.source_name, j.PULocationID,
            COALESCE(z.borough, 'Unknown')                       AS borough,
            COALESCE(z.zone, 'Unknown')                          AS zone,
            j.pickup_month, j.pickup_day_of_week, j.pickup_hour,
            j.is_weekend,

            -- Raw factor values (for tooltips, the radar chart, the LLM agent)
            ROUND(j.mean_demand, 2)                              AS mean_demand,
            ROUND(j.demand_stddev, 2)                            AS demand_stddev,
            ROUND(j.demand_cv, 4)                                AS demand_cv,
            ROUND(j.trend_slope, 3)                              AS trend_slope,
            ROUND(j.yellow_share, 4)                             AS yellow_share,
            j.yellow_total, j.hvfhv_total, j.observation_count,

            -- Normalized [0,1] sub-scores. CV is INVERTED so high = reliable.
            CASE WHEN b.dmx > b.dmn
                 THEN ROUND((j.mean_demand - b.dmn) / (b.dmx - b.dmn), 4)
                 ELSE 0.0 END                                    AS demand_score,
            CASE WHEN b.cmx > b.cmn
                 THEN ROUND(1.0 - (j.demand_cv - b.cmn) / (b.cmx - b.cmn), 4)
                 ELSE 0.5 END                                    AS reliability_score,
            CASE WHEN b.tmx > b.tmn
                 THEN ROUND((j.trend_slope - b.tmn) / (b.tmx - b.tmn), 4)
                 ELSE 0.5 END                                    AS trend_score,
            -- yellow_share is already in [0,1]; keep raw — it's only meaningful
            -- when the user filters by source_name = 'yellow'
            ROUND(COALESCE(j.yellow_share, 0.0), 4)              AS yellow_share_score
        FROM joined j
        JOIN bucket_stats b
          ON j.source_name = b.source_name
         AND j.pickup_month = b.pickup_month
         AND j.pickup_day_of_week = b.pickup_day_of_week
         AND j.pickup_hour = b.pickup_hour
        LEFT JOIN zone_lookup z USING (PULocationID)
        """
    )

    con.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{RELIABILITY_TABLE}_filter "
        f"ON {RELIABILITY_TABLE}(source_name, pickup_month, pickup_day_of_week, pickup_hour)"
    )
    con.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{RELIABILITY_TABLE}_zone "
        f"ON {RELIABILITY_TABLE}(PULocationID)"
    )

    con.execute("DROP TABLE _per_source")
    con.execute("DROP TABLE _split")


def build_predictions(db_path: Path = DUCKDB_PATH) -> int:
    """Build the serving DuckDB tables. Returns total prediction rows.

    Builds two tables:
      * `current_taxi_demand_predictions` — simple demand baseline (legacy view)
      * `current_zone_reliability` — 4-factor reliability recommender
    """
    parquet_dir = _ensure_parquet_available()
    logger.info("Building DuckDB serving tables at %s", db_path)
    with _connect(db_path) as con:
        _compute_predictions(con, parquet_dir)
        _compute_reliability(con)
        demand_rows = con.execute(f"SELECT COUNT(*) FROM {SERVING_TABLE}").fetchone()[0]
        rel_rows = con.execute(f"SELECT COUNT(*) FROM {RELIABILITY_TABLE}").fetchone()[0]
    logger.info("Serving: %s demand rows, %s reliability rows", demand_rows, rel_rows)
    return demand_rows


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rows = build_predictions()
    print(f"Built {rows:,} prediction rows -> {DUCKDB_PATH}")
