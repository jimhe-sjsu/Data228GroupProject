"""Cached loaders for static reference data and the DuckDB serving table."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from config import DUCKDB_PATH, ZONE_GEOJSON
from predictions import RELIABILITY_TABLE, SERVING_TABLE


# ── Connection (cached resource, single shared handle) ───────────────────────
@st.cache_resource(show_spinner=False)
def get_duckdb_connection(db_path: str = str(DUCKDB_PATH)) -> duckdb.DuckDBPyConnection:
    """Open a read-only DuckDB connection. Cached across Streamlit reruns.

    Raises FileNotFoundError if the serving file is missing — callers
    (the Streamlit app) catch this and render a friendly setup screen.
    """
    if not Path(db_path).exists():
        raise FileNotFoundError(
            f"DuckDB serving file not found at {db_path}. "
            "Run `python streamlit_app/predictions.py` first."
        )
    return duckdb.connect(db_path, read_only=True)


# ── Static reference data (cached forever per process) ──────────────────────
@st.cache_data(show_spinner=False)
def load_zone_geojson() -> dict:
    """Load the NYC taxi zone polygons (WGS84)."""
    with ZONE_GEOJSON.open("r") as f:
        return json.load(f)


@st.cache_data(show_spinner=False)
def list_filter_options() -> dict[str, list]:
    """Return sorted unique values for the sidebar dropdowns."""
    con = get_duckdb_connection()
    sources = [r[0] for r in con.execute(
        f"SELECT DISTINCT source_name FROM {SERVING_TABLE} ORDER BY source_name"
    ).fetchall()]
    months = [r[0] for r in con.execute(
        f"SELECT DISTINCT pickup_month FROM {SERVING_TABLE} ORDER BY pickup_month"
    ).fetchall()]
    days = [r[0] for r in con.execute(
        f"SELECT DISTINCT pickup_day_of_week FROM {SERVING_TABLE} ORDER BY pickup_day_of_week"
    ).fetchall()]
    hours = [r[0] for r in con.execute(
        f"SELECT DISTINCT pickup_hour FROM {SERVING_TABLE} ORDER BY pickup_hour"
    ).fetchall()]
    return {"source": sources, "month": months, "day_of_week": days, "hour": hours}


@st.cache_data(show_spinner=False)
def get_data_freshness() -> dict | None:
    """Return prediction_timestamp + model_version for the freshness banner."""
    con = get_duckdb_connection()
    cols = [r[0] for r in con.execute(f"DESCRIBE {SERVING_TABLE}").fetchall()]
    has_model_version = "model_version" in cols
    select_cols = "MAX(prediction_timestamp), COUNT(*)"
    if has_model_version:
        select_cols = "MAX(prediction_timestamp), MAX(model_version), COUNT(*)"
    row = con.execute(f"SELECT {select_cols} FROM {SERVING_TABLE}").fetchone()
    if row is None or row[0] is None:
        return None
    if has_model_version:
        ts, version, count = row
    else:
        ts, count = row
        version = "unknown"
    return {
        "prediction_timestamp": ts,
        "model_version": version,
        "row_count": int(count),
    }


# ── Filtered queries — short TTL cache so repeat filter clicks are instant ──
@st.cache_data(show_spinner=False, ttl=300)
def fetch_predictions(
    source_name: str | None,
    month: int,
    day_of_week: int,
    hour: int,
) -> pd.DataFrame:
    """Pull predicted demand for the user-selected slice.

    `source_name == None` means "combine yellow + hvfhv" — sums predicted_trip_count
    for the same zone × time bucket. Cached for 5 minutes per filter combo.
    """
    con = get_duckdb_connection()
    if source_name is None:
        # Combined view: sum yellow + hvfhv per zone, then re-bucket demand_level
        # against this slice's quantiles so the legend on the map matches what
        # the user sees. Without this CTE the dataframe would be missing the
        # demand_level column and downstream charts would silently fall back.
        query = f"""
            WITH agg AS (
                SELECT
                    PULocationID,
                    ANY_VALUE(borough) AS borough,
                    ANY_VALUE(zone)    AS zone,
                    pickup_month,
                    pickup_day_of_week,
                    pickup_hour,
                    SUM(predicted_trip_count) AS predicted_trip_count
                FROM {SERVING_TABLE}
                WHERE pickup_month = ?
                  AND pickup_day_of_week = ?
                  AND pickup_hour = ?
                GROUP BY PULocationID, pickup_month, pickup_day_of_week, pickup_hour
            ),
            quant AS (
                SELECT
                    QUANTILE_CONT(predicted_trip_count, 0.25) AS q1,
                    QUANTILE_CONT(predicted_trip_count, 0.50) AS q2,
                    QUANTILE_CONT(predicted_trip_count, 0.75) AS q3
                FROM agg
            )
            SELECT
                a.PULocationID, a.borough, a.zone, a.pickup_month,
                a.pickup_day_of_week, a.pickup_hour, a.predicted_trip_count,
                CASE
                    WHEN a.predicted_trip_count <= q.q1 THEN 'low'
                    WHEN a.predicted_trip_count <= q.q2 THEN 'medium'
                    WHEN a.predicted_trip_count <= q.q3 THEN 'high'
                    ELSE 'very_high'
                END AS demand_level
            FROM agg a CROSS JOIN quant q
            ORDER BY a.predicted_trip_count DESC
        """
        params = [month, day_of_week, hour]
    else:
        query = f"""
            SELECT
                PULocationID, borough, zone, pickup_month,
                pickup_day_of_week, pickup_hour,
                predicted_trip_count, demand_level
            FROM {SERVING_TABLE}
            WHERE source_name = ?
              AND pickup_month = ?
              AND pickup_day_of_week = ?
              AND pickup_hour = ?
            ORDER BY predicted_trip_count DESC
        """
        params = [source_name, month, day_of_week, hour]
    return con.execute(query, params).fetchdf()


@st.cache_data(show_spinner=False, ttl=300)
def fetch_zone_24h_profile(
    location_id: int,
    source_name: str | None,
    month: int,
    day_of_week: int,
) -> pd.DataFrame:
    """Return the 24-hour demand curve for one zone — used by map drill-down.

    The Streamlit app calls this when a zone is clicked on the map. Bypasses
    the hour filter so the user can see how that zone behaves across the day
    for the selected month + day_of_week.
    """
    con = get_duckdb_connection()
    if source_name is None:
        query = f"""
            SELECT
                pickup_hour,
                SUM(predicted_trip_count) AS predicted_trip_count
            FROM {SERVING_TABLE}
            WHERE PULocationID = ?
              AND pickup_month = ?
              AND pickup_day_of_week = ?
            GROUP BY pickup_hour
            ORDER BY pickup_hour
        """
        params = [location_id, month, day_of_week]
    else:
        query = f"""
            SELECT pickup_hour, predicted_trip_count
            FROM {SERVING_TABLE}
            WHERE PULocationID = ?
              AND source_name = ?
              AND pickup_month = ?
              AND pickup_day_of_week = ?
            ORDER BY pickup_hour
        """
        params = [location_id, source_name, month, day_of_week]
    return con.execute(query, params).fetchdf()


@st.cache_data(show_spinner=False, ttl=300)
def fetch_zone_metadata(location_id: int) -> dict | None:
    """Return zone + borough name for a single PULocationID."""
    con = get_duckdb_connection()
    row = con.execute(
        f"SELECT ANY_VALUE(zone), ANY_VALUE(borough) FROM {SERVING_TABLE} "
        "WHERE PULocationID = ?",
        [location_id],
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return {"zone": row[0], "borough": row[1]}


# ── Label helpers (cheap, lru_cache is plenty) ──────────────────────────────
@st.cache_data(show_spinner=False, ttl=300)
def fetch_reliability(
    source_name: str | None,
    month: int,
    day_of_week: int,
    hour: int,
) -> pd.DataFrame:
    """Pull the 4-factor reliability rows for the user-selected slice.

    `source_name == None` (Combined) averages the per-source sub-scores
    weighted by yellow vs hvfhv volume so the result still has all 4 factors
    aligned to the same zone × time bucket.
    """
    con = get_duckdb_connection()
    if source_name is None:
        query = f"""
            WITH sliced AS (
                SELECT * FROM {RELIABILITY_TABLE}
                WHERE pickup_month = ?
                  AND pickup_day_of_week = ?
                  AND pickup_hour = ?
            )
            SELECT
                PULocationID,
                ANY_VALUE(borough)               AS borough,
                ANY_VALUE(zone)                  AS zone,
                pickup_month, pickup_day_of_week, pickup_hour,
                SUM(mean_demand)                 AS mean_demand,
                AVG(demand_cv)                   AS demand_cv,
                SUM(trend_slope)                 AS trend_slope,
                MAX(yellow_share)                AS yellow_share,
                AVG(demand_score)                AS demand_score,
                AVG(reliability_score)           AS reliability_score,
                AVG(trend_score)                 AS trend_score,
                MAX(yellow_share_score)          AS yellow_share_score,
                SUM(yellow_total)                AS yellow_total,
                SUM(hvfhv_total)                 AS hvfhv_total
            FROM sliced
            GROUP BY PULocationID, pickup_month, pickup_day_of_week, pickup_hour
        """
        params = [month, day_of_week, hour]
    else:
        query = f"""
            SELECT
                PULocationID, borough, zone,
                pickup_month, pickup_day_of_week, pickup_hour,
                mean_demand, demand_cv, trend_slope, yellow_share,
                demand_score, reliability_score, trend_score, yellow_share_score,
                yellow_total, hvfhv_total
            FROM {RELIABILITY_TABLE}
            WHERE source_name = ?
              AND pickup_month = ?
              AND pickup_day_of_week = ?
              AND pickup_hour = ?
        """
        params = [source_name, month, day_of_week, hour]
    return con.execute(query, params).fetchdf()


@lru_cache(maxsize=1)
def day_of_week_labels() -> dict[int, str]:
    """Map Spark's `dayofweek` output (1=Sun, 7=Sat) to readable names."""
    return {1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday",
            5: "Thursday", 6: "Friday", 7: "Saturday"}


@lru_cache(maxsize=1)
def month_labels() -> dict[int, str]:
    return {i: pd.Timestamp(2024, i, 1).strftime("%B") for i in range(1, 13)}


def format_freshness(freshness: dict | None) -> str:
    """Human-readable freshness for the header banner."""
    if not freshness or freshness.get("prediction_timestamp") is None:
        return "Data: not initialized"
    ts = freshness["prediction_timestamp"]
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts)
        except ValueError:
            return f"Data as of {ts}"
    # `ts` is naive (DuckDB TIMESTAMP without tz); compare against a naive UTC now.
    delta = datetime.now(timezone.utc).replace(tzinfo=None) - ts
    seconds = int(delta.total_seconds())
    if seconds < 60:
        rel = f"{seconds}s ago"
    elif seconds < 3600:
        rel = f"{seconds // 60} min ago"
    elif seconds < 86400:
        rel = f"{seconds // 3600} hr ago"
    else:
        rel = f"{seconds // 86400} d ago"
    version = freshness.get("model_version", "unknown")
    return f"Refreshed {rel} · model: {version}"
