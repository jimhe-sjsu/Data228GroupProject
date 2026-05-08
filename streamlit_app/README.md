# NYC Taxi Demand — Streamlit Demo + LLM Agent

Steps 9-12 of the data flow: precomputed predictions → DuckDB serving table → Streamlit map + LLM Q&A.

```text
Notebook trained-model predictions
    └─ predictions.py ──► output/predictions.duckdb (current_taxi_demand_predictions)
                                  │
                                  ├─► app.py            (NYC choropleth map + top zones)
                                  └─► llm_agent.py      (Claude tool-calling, same DuckDB)
```

## One-time setup

```bash
# 1. Create venv + install runtime deps
python3 -m venv streamlit_app/.venv
source streamlit_app/.venv/bin/activate
pip install -r streamlit_app/requirements.txt

# 2. Build the DuckDB serving table from notebook predictions
python streamlit_app/predictions.py
# → Built prediction rows -> output/predictions.duckdb

# 3. Pick an LLM backend (one of the two below)

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

- **Sidebar:** pick the available prediction profile, month, day of week, and hour
- **Map:** every NYC pickup zone is colored by predicted demand band (low / medium / high / very_high)
- **Top zones table:** top 10 highest-predicted zones for the chosen slice
- **LLM panel:** ask natural-language questions; the agent issues read-only SQL against `current_taxi_demand_predictions` and answers from the result

## Prediction Source

`predictions.py` first looks for the trained-model CSV exported by the notebook:

```text
notebooks/output/streamlit_predictions_csv/*.csv
```

If that file exists, it writes those trained predictions to DuckDB with
`model_version = 'trained-notebook-v1'`. The current notebook export is
combined-only, so the app does not split trained predictions into Yellow Taxi
and HVFHV views. If the notebook CSV is missing, the script falls back to the
historical-average baseline from the ML parquet export. For that fallback,
extract the portable parquet first:

```bash
mkdir -p .tmp/parquet_extract
unzip -o output/taxi_demand_ml_parquet.zip -d .tmp/parquet_extract/
python streamlit_app/predictions.py
```

## File layout

| File | Role |
|------|------|
| `config.py` | Paths, constants, LLM model id |
| `predictions.py` | Builds the DuckDB serving table from notebook predictions, with ML parquet baseline fallback |
| `data_loader.py` | Cached DuckDB connection + GeoJSON loader for Streamlit |
| `app.py` | Streamlit UI: sidebar + folium map + top-zones table + LLM chat |
| `llm_agent.py` | Pluggable LLM backends (Anthropic with tool-calling / Ollama with prompt-based SQL extraction) |
| `requirements.txt` | Python deps |
