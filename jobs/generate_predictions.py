"""Airflow entrypoint: rebuild the DuckDB serving table from the ML parquet.

This is a thin wrapper around `streamlit_app/predictions.py` so the same
code path runs from both:
  - manual CLI: `python streamlit_app/predictions.py`
  - scheduled Airflow run: SparkSubmitOperator -> /workspace/jobs/generate_predictions.py
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

# Allow direct module import when run inside the spark-master container,
# where /workspace is the project root.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "streamlit_app"))

from predictions import build_predictions  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s - %(message)s",
)
logger = logging.getLogger("generate_predictions")


def main() -> None:
    rows = build_predictions()
    logger.info("Predictions serving table refreshed: %s rows", rows)


if __name__ == "__main__":
    main()
