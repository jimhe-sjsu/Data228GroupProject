"""LLM agent that answers natural-language questions about NYC taxi demand.

Two backends are supported:

  * AnthropicBackend  — Claude with native tool-calling (one tool: query_taxi_demand)
  * OllamaBackend     — local Ollama server, prompt-based SQL extraction (Gemma-friendly)

Both expose the same `ask(user_message, history) -> (answer, trace)` contract
so the Streamlit UI doesn't care which one is wired in. Pick via LLM_PROVIDER.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Protocol

import duckdb
import pandas as pd

from config import (
    ANTHROPIC_MODEL,
    DEFAULT_MAX_TOKENS,
    DUCKDB_PATH,
    LLM_PROVIDER,
    OLLAMA_HOST,
    OLLAMA_MODEL,
)
from predictions import SERVING_TABLE


# ── Schema doc shared by both backends ───────────────────────────────────────
SCHEMA_DOC = f"""You are an analytics assistant for a NYC taxi *driver
opportunity recommender*. You answer questions by querying TWO DuckDB tables.

TABLE 1 — `{SERVING_TABLE}` — simple demand predictions (legacy view):

  prediction_timestamp  TIMESTAMP    -- when the table was built (NOT a query field)
  model_version         VARCHAR      -- traceability (NOT a query field)
  source_id             INTEGER      -- 0=yellow, 1=hvfhv
  source_name           VARCHAR      -- 'yellow' or 'hvfhv'
  PULocationID          BIGINT       -- NYC taxi zone id (1-265)
  borough               VARCHAR      -- 'Manhattan' | 'Queens' | 'Brooklyn' | 'Bronx' | 'Staten Island' | 'EWR' | 'Unknown'
  zone                  VARCHAR      -- human-readable zone name (e.g. 'Midtown Center')
  pickup_month          INTEGER      -- 1=Jan, 12=Dec
  pickup_day_of_week    INTEGER      -- Spark convention: 1=Sun, 2=Mon, 3=Tue, 4=Wed, 5=Thu, 6=Fri, 7=Sat
  pickup_hour           INTEGER      -- 0-23 (24-hour clock)
  is_weekend            BOOLEAN      -- TRUE only when pickup_day_of_week IN (1, 7)
  predicted_trip_count  DOUBLE       -- the demand forecast
  demand_level          VARCHAR      -- 'low' | 'medium' | 'high' | 'very_high'

TABLE 2 — `current_zone_reliability` — risk-aware 4-factor recommender.
Use this when the user asks about RELIABILITY, RISK, GROWTH, VOLATILITY,
TREND, or YELLOW vs HVFHV market share:

  source_id, source_name, PULocationID, borough, zone,
  pickup_month, pickup_day_of_week, pickup_hour, is_weekend,

  -- Raw factor values (use these for explanations)
  mean_demand            DOUBLE   -- avg trip_count for this zone × time bucket
  demand_cv              DOUBLE   -- coefficient of variation (lower = more reliable)
  trend_slope            DOUBLE   -- multi-year demand slope (trips/year, signed)
  yellow_share           DOUBLE   -- yellow / (yellow + hvfhv) in [0, 1]
  yellow_total           BIGINT   -- yellow trip counts across observations
  hvfhv_total            BIGINT   -- hvfhv trip counts across observations

  -- Normalized [0,1] sub-scores per bucket (use these for ranking)
  demand_score           DOUBLE   -- higher = busier
  reliability_score      DOUBLE   -- higher = more consistent (CV inverted)
  trend_score            DOUBLE   -- higher = growing zone
  yellow_share_score     DOUBLE   -- higher = stronger yellow position

CRITICAL hints to avoid silent wrong answers:
- DO NOT filter by prediction_timestamp — it is metadata, not query input.
- "Friday" → pickup_day_of_week = 6   (NOT is_weekend; Friday is a weekday)
- "Weekend" → is_weekend = TRUE       (covers Sat + Sun, both)
- "6 pm" → pickup_hour = 18           (24-hour clock)
- "Manhattan", "Brooklyn" etc. → borough = 'Manhattan'  (always quoted, exact case)
- Boolean AND must be `AND`, not `&&`. SQL is standard SQL, not C/JS.
- Always end with LIMIT (10 unless asked otherwise).

Rules:
- Read-only: only SELECT (or WITH ... SELECT) is allowed.
- If user is vague about time, default to weekday commuter slots
  (pickup_hour in (8, 18), pickup_day_of_week IN (2,3,4,5,6)) and tell them.
- Be concise: 2-4 sentences in the final answer.
"""


# ── Few-shot examples (small models lean on these heavily) ───────────────────
FEW_SHOT_EXAMPLES = """Examples:

Q: Top 3 busiest zones in Manhattan on Friday at 6pm.
```sql
SELECT zone, predicted_trip_count
FROM current_taxi_demand_predictions
WHERE borough = 'Manhattan'
  AND pickup_day_of_week = 6
  AND pickup_hour = 18
ORDER BY predicted_trip_count DESC
LIMIT 3
```

Q: Where should I drive for the most RELIABLE income on a weekday at 6pm?
```sql
SELECT zone, borough, mean_demand, demand_cv, reliability_score
FROM current_zone_reliability
WHERE source_name = 'yellow'
  AND pickup_day_of_week IN (2,3,4,5,6)
  AND pickup_hour = 18
  AND mean_demand > 30
ORDER BY reliability_score DESC
LIMIT 5
```

Q: Which zones are GROWING fastest year-over-year for yellow taxis?
```sql
SELECT zone, borough, trend_slope, mean_demand, trend_score
FROM current_zone_reliability
WHERE source_name = 'yellow'
  AND pickup_day_of_week = 6
  AND pickup_hour = 18
ORDER BY trend_slope DESC
LIMIT 10
```

Q: Where does yellow taxi still dominate over Uber/Lyft (HVFHV)?
```sql
SELECT zone, borough, yellow_total, hvfhv_total, yellow_share
FROM current_zone_reliability
WHERE source_name = 'yellow'
  AND pickup_hour = 18
  AND mean_demand > 50
ORDER BY yellow_share DESC
LIMIT 10
```

Q: Show me high-demand but VOLATILE zones (avoid these — risky bets).
```sql
SELECT zone, borough, mean_demand, demand_cv
FROM current_zone_reliability
WHERE source_name = 'yellow'
  AND pickup_day_of_week = 6
  AND pickup_hour = 18
  AND mean_demand > 100
ORDER BY demand_cv DESC
LIMIT 10
```

Q: How does demand change across hours for JFK Airport on a Monday?
```sql
SELECT pickup_hour, SUM(predicted_trip_count) AS total_trips
FROM current_taxi_demand_predictions
WHERE zone = 'JFK Airport'
  AND pickup_day_of_week = 2
GROUP BY pickup_hour
ORDER BY pickup_hour
LIMIT 24
```
"""


# ── Tool implementation ──────────────────────────────────────────────────────
def _is_safe_select(sql: str) -> bool:
    cleaned = sql.strip().rstrip(";").strip()
    if not cleaned:
        return False
    head = cleaned.lower().split(None, 1)
    if head[0] not in ("select", "with"):
        return False
    forbidden = (";", "drop ", "delete ", "insert ", "update ", "alter ",
                 "create ", "attach ", "copy ", "pragma ", "set ")
    lower = cleaned.lower()
    return not any(bad in lower for bad in forbidden)


def _default_connection() -> duckdb.DuckDBPyConnection:
    """Open a read-only DuckDB connection without depending on Streamlit caching."""
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


# Result-row safety cap. DuckDB has no `statement_timeout` parameter (unlike
# Postgres), and per-query timeout via threading would complicate the cached
# connection lifecycle. We protect against runaway results in two cheaper
# ways instead: the safety guard rejects DDL/DML, and we slice the dataframe
# to MAX_ROWS_RETURNED before returning.
MAX_ROWS_RETURNED = 200


def query_taxi_demand(sql: str, con: duckdb.DuckDBPyConnection | None = None) -> dict[str, Any]:
    """Execute a vetted SELECT and return rows as JSON-friendly records."""
    if not _is_safe_select(sql):
        return {"error": "Only single SELECT statements are allowed."}
    cleaned_sql = sql.strip().rstrip(";").strip()
    if con is None:
        con = _default_connection()
    try:
        df: pd.DataFrame = con.execute(cleaned_sql).fetchdf()
    except Exception as exc:
        return {"error": str(exc)}
    if len(df) > MAX_ROWS_RETURNED:
        df = df.head(MAX_ROWS_RETURNED)
    return {"row_count": len(df), "rows": df.to_dict(orient="records")}


# ── Backend protocol ─────────────────────────────────────────────────────────
class LLMBackend(Protocol):
    def ask(self, user_message: str, history: list[dict] | None = None) -> tuple[str, list[dict]]: ...


# ── Anthropic backend (native tool calling) ──────────────────────────────────
ANTHROPIC_TOOLS = [
    {
        "name": "query_taxi_demand",
        "description": (
            f"Run a read-only DuckDB SELECT against {SERVING_TABLE}. "
            "Returns up to 200 rows as JSON records."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "A single SELECT statement."}
            },
            "required": ["sql"],
        },
    }
]


def _build_history_messages(history: list[dict] | None, max_turns: int = 4) -> list[dict]:
    """Take last N user/assistant pairs from chat history for follow-up context."""
    if not history:
        return []
    cleaned = [h for h in history if h.get("role") in ("user", "assistant") and h.get("content")]
    return cleaned[-max_turns * 2:]


class AnthropicBackend:
    def __init__(self, model: str = ANTHROPIC_MODEL, max_tokens: int = DEFAULT_MAX_TOKENS):
        try:
            from anthropic import Anthropic
        except ImportError as exc:
            raise RuntimeError(
                "anthropic SDK not installed. Run `pip install anthropic`."
            ) from exc
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is not set.")
        self.client = Anthropic(api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens

    def ask(self, user_message: str, history: list[dict] | None = None) -> tuple[str, list[dict]]:
        prior = _build_history_messages(history)
        messages: list[dict] = [
            *[{"role": h["role"], "content": h["content"]} for h in prior],
            {"role": "user", "content": user_message},
        ]
        trace: list[dict] = [{"role": "user", "content": user_message}]

        while True:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=SCHEMA_DOC + "\n" + FEW_SHOT_EXAMPLES,
                tools=ANTHROPIC_TOOLS,
                messages=messages,
            )
            trace.append({"role": "assistant", "content": [
                {"type": b.type, "text": getattr(b, "text", None),
                 "name": getattr(b, "name", None),
                 "input": getattr(b, "input", None)}
                for b in response.content
            ]})

            if response.stop_reason != "tool_use":
                text_blocks = [b.text for b in response.content if b.type == "text"]
                return ("\n".join(text_blocks).strip() or "(no answer)", trace)

            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                sql = block.input.get("sql", "")
                result = query_taxi_demand(sql)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": str(result),
                })
                trace.append({"role": "tool", "content": {"sql": sql, "result": result}})

            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})


# ── Ollama backend (prompt-based SQL extraction for Gemma) ───────────────────
OLLAMA_GENERATE_PROMPT = SCHEMA_DOC + "\n" + FEW_SHOT_EXAMPLES + """

Now produce ONE SQL SELECT that answers the user's question, wrapped in a
```sql ... ``` code block. Output nothing else — no explanation, no preamble.
"""

OLLAMA_RETRY_PROMPT = SCHEMA_DOC + "\n" + FEW_SHOT_EXAMPLES + """

Your previous SQL failed. Read the error message carefully and rewrite ONE
corrected SQL SELECT, wrapped in a ```sql ... ``` code block. Common fixes:
- Use AND, not && or &
- Don't filter by prediction_timestamp
- Friday is pickup_day_of_week = 6, not is_weekend = TRUE
- Borough names must be exact: 'Manhattan', 'Queens', 'Brooklyn', 'Bronx', 'Staten Island'
"""

OLLAMA_ANSWER_PROMPT = """You are answering a follow-up turn in a chat. You will receive:
1. The user's original question
2. The SQL that was run
3. The result rows (JSON, up to 200 rows)
4. (Optional) Earlier conversation history for context

Answer the user's question in 2-4 sentences. Be specific with numbers. Do not
preamble. If a default-time assumption was applied, mention it briefly.
"""

_SQL_CODEBLOCK_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


def _extract_sql(text: str) -> str | None:
    m = _SQL_CODEBLOCK_RE.search(text)
    if m:
        return m.group(1).strip()
    stripped = text.strip()
    if stripped.lower().startswith(("select", "with")):
        return stripped
    return None


def _format_history_for_gemma(history: list[dict] | None, max_turns: int = 3) -> str:
    """Render recent chat history as plaintext for non-tool-calling models."""
    prior = _build_history_messages(history, max_turns=max_turns)
    if not prior:
        return ""
    lines = ["Recent conversation:"]
    for h in prior:
        role = "User" if h["role"] == "user" else "Assistant"
        lines.append(f"{role}: {h['content']}")
    return "\n".join(lines) + "\n\n"


class OllamaBackend:
    def __init__(self, host: str = OLLAMA_HOST, model: str = OLLAMA_MODEL):
        try:
            import ollama  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "ollama SDK not installed. Run `pip install ollama`."
            ) from exc
        self.model = model
        self.host = host
        from ollama import Client
        self.client = Client(host=host)

    def _chat(self, system: str, user: str) -> str:
        response = self.client.chat(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            options={"temperature": 0.1},
        )
        return response["message"]["content"]

    def _try_sql(self, prompt: str, user_input: str) -> tuple[str | None, str]:
        """Return (extracted_sql, raw_response). extracted_sql is None if missing."""
        raw = self._chat(prompt, user_input)
        return _extract_sql(raw), raw

    def ask(self, user_message: str, history: list[dict] | None = None) -> tuple[str, list[dict]]:
        trace: list[dict] = [{"role": "user", "content": user_message}]
        history_block = _format_history_for_gemma(history)
        composed_q = (history_block + f"Current question: {user_message}").strip()

        # Step 1: ask Gemma for SQL
        sql, raw = self._try_sql(OLLAMA_GENERATE_PROMPT, composed_q)
        trace.append({"role": "assistant", "content": [{"type": "text", "text": raw}]})

        if not sql:
            return (
                "I couldn't generate a valid SQL query for that. "
                "Try rephrasing — e.g. 'top 5 zones in Manhattan at 6pm'.",
                trace,
            )

        # Step 2: execute
        result = query_taxi_demand(sql)
        trace.append({"role": "tool", "content": {"sql": sql, "result": result}})

        # Step 2b: one retry on SQL error — feed the error back to the model
        if "error" in result:
            retry_input = (
                f"{composed_q}\n\n"
                f"Your previous SQL was:\n```sql\n{sql}\n```\n\n"
                f"It failed with: {result['error']}\n\n"
                "Rewrite a corrected SELECT."
            )
            sql2, raw2 = self._try_sql(OLLAMA_RETRY_PROMPT, retry_input)
            trace.append({"role": "assistant", "content": [{"type": "text", "text": raw2}]})
            if sql2:
                result2 = query_taxi_demand(sql2)
                trace.append({"role": "tool", "content": {"sql": sql2, "result": result2}})
                if "error" not in result2:
                    sql, result = sql2, result2

        if "error" in result:
            return (f"⚠️ Query failed twice: {result['error']}", trace)

        # Step 3: synthesize the natural-language answer
        answer_input = (
            f"{history_block}"
            f"Current question: {user_message}\n\n"
            f"SQL run:\n```sql\n{sql}\n```\n\n"
            f"Results ({result['row_count']} rows):\n"
            f"{json.dumps(result['rows'], default=str, indent=2)}"
        )
        final = self._chat(OLLAMA_ANSWER_PROMPT, answer_input)
        trace.append({"role": "assistant", "content": [{"type": "text", "text": final}]})
        return (final.strip() or "(no answer)", trace)


# ── Public entrypoint ────────────────────────────────────────────────────────
def _get_backend() -> LLMBackend:
    if LLM_PROVIDER == "ollama":
        return OllamaBackend()
    if LLM_PROVIDER == "anthropic":
        return AnthropicBackend()
    raise RuntimeError(f"Unknown LLM_PROVIDER='{LLM_PROVIDER}'. Use 'anthropic' or 'ollama'.")


def ask(user_message: str, history: list[dict] | None = None,
        **_legacy_kwargs) -> tuple[str, list[dict]]:
    """Run a single agent turn. Provider chosen by LLM_PROVIDER env var.

    `history` is the chat transcript so far — list of {role, content} dicts —
    so follow-up questions like "now Brooklyn" make sense. Pass the last
    several turns; the backend picks how many to actually send.
    """
    return _get_backend().ask(user_message, history=history)
