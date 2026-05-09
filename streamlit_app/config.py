"""Paths and constants shared across the streamlit_app modules."""
from pathlib import Path

# ── Filesystem layout ────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"

ZONE_LOOKUP_CSV = DATA_DIR / "taxi_zone_lookup.csv"
ZONE_GEOJSON = DATA_DIR / "taxi_zones.geojson"
ML_PARQUET_DIR = PROJECT_ROOT / ".tmp" / "parquet_extract" / "taxi_demand_ml_parquet"
DUCKDB_PATH = OUTPUT_DIR / "predictions.duckdb"

# ── Demand level thresholds ──────────────────────────────────────────────────
# trip_count quantiles approximated from the ML table; categorical bands
# are recomputed at serve time so they adapt to the actual data distribution.
DEMAND_LEVELS = ("low", "medium", "high", "very_high")
DEMAND_QUANTILES = (0.25, 0.50, 0.75)

# ── LLM agent ────────────────────────────────────────────────────────────────
import os

# Provider selection. "anthropic" uses the Claude SDK + native tool calling.
# "ollama" uses a local Ollama server with a Gemma-friendly prompt pattern
# (Gemma has no native tool-call support, so we drive it via SQL extraction).
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "ollama").lower()

# Anthropic settings
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")

# Ollama settings
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "gemma3:1b")

# Shared
DEFAULT_LLM_MODEL = ANTHROPIC_MODEL  # backwards-compatible alias
DEFAULT_MAX_TOKENS = 1024
