# NYC Taxi Demand — Streamlit Demo + LLM Agent

Steps 9-12 of the data flow: precomputed predictions → DuckDB serving table → Streamlit map + LLM Q&A.

```
Iceberg ML parquet (teammates' step 6 output)
    └─ predictions.py ──► output/predictions.duckdb (current_taxi_demand_predictions)
                                  │
                                  ├─► app.py            (NYC choropleth map + top zones)
                                  └─► llm_agent.py      (Claude tool-calling, same DuckDB)
```

## One-time setup

```bash
# 1. Extract the ML parquet your teammate produced
mkdir -p .tmp/parquet_extract
unzip -o output/taxi_demand_ml_parquet.zip -d .tmp/parquet_extract/

# 2. Create venv + install runtime deps
python3 -m venv streamlit_app/.venv
source streamlit_app/.venv/bin/activate
pip install -r streamlit_app/requirements.txt

# 3. Build the DuckDB serving table (~30 seconds, runs once)
python streamlit_app/predictions.py
# → Built ~700,000 prediction rows -> output/predictions.duckdb

# 4. Pick an LLM backend (one of the two below)

# Option A — Anthropic Claude (uses native tool calling, most reliable)
export LLM_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-ant-...

# Option B — Local Ollama with Gemma (free, offline, no API key)
#   Pull a model first. Recommended sizes:
#     gemma2:9b   (5.5 GB) — best SQL accuracy, the default
#     gemma3:4b   (3.3 GB) — lighter, decent quality
#     gemma3:1b   (815 MB) — only works for simple queries, useful for smoke tests
ollama pull gemma2:9b
export LLM_PROVIDER=ollama
export OLLAMA_MODEL=gemma2:9b
# Optional override if Ollama runs on a non-default host:
# export OLLAMA_HOST=http://localhost:11434
```

> Gemma has no native tool-calling support in Ollama, so the Ollama backend
> uses a 2-step pattern instead: model writes SQL → we execute it → model
> writes the natural-language answer. The DuckDB safety guard (only SELECT,
> no multi-statement) is enforced before any query runs.

## Run the app

```bash
source streamlit_app/.venv/bin/activate
streamlit run streamlit_app/app.py
```

Open http://localhost:8501.

## What it does

- **Sidebar:** pick source (Yellow / HVFHV / Combined), month, day of week, hour
- **Map:** every NYC pickup zone is colored by predicted demand band (low / medium / high / very_high)
- **Top zones table:** top 10 highest-predicted zones for the chosen slice
- **LLM panel:** ask natural-language questions; the agent issues read-only SQL against `current_taxi_demand_predictions` and answers from the result

## Swapping in the trained ML model

The current `predictions.py` uses a **historical-average baseline** (per architecture plan).
When the trained XGBoost/RandomForest model is delivered, replace `_compute_predictions` to
call `model.predict(features_df)` instead of `AVG(trip_count)`. Schema and downstream
code stay identical.

## File layout

| File | Role |
|------|------|
| `config.py` | Paths, constants, LLM model id |
| `predictions.py` | Builds the DuckDB serving table from the Iceberg ML parquet |
| `data_loader.py` | Cached DuckDB connection + GeoJSON loader for Streamlit |
| `app.py` | Streamlit UI: sidebar + folium map + top-zones table + LLM chat |
| `llm_agent.py` | Pluggable LLM backends (Anthropic with tool-calling / Ollama with prompt-based SQL extraction) |
| `requirements.txt` | Python deps |
