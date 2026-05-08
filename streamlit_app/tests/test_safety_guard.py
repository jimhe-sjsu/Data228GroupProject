"""Adversarial tests for the LLM agent's SQL safety guard.

The guard is the only thing standing between an LLM's hallucinated SQL
and a writable connection. These tests lock down its behavior so a
careless edit doesn't accidentally let DML through.

Run with:
    pytest streamlit_app/tests/
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from llm_agent import _is_safe_select  # noqa: E402


# ── Things that MUST be allowed ──────────────────────────────────────────────
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM x",
        "select * from x",
        "  SELECT 1  ",
        "SELECT * FROM x;",                       # trailing ; is tolerated
        "SELECT * FROM x;   ",                     # ; + trailing whitespace
        "WITH y AS (SELECT 1) SELECT * FROM y",    # CTE
        "SELECT a, b FROM t WHERE a = 'x'",
        "SELECT COUNT(*) FROM t",
        "SELECT * FROM t ORDER BY a DESC LIMIT 5",
    ],
)
def test_legitimate_selects_are_allowed(sql: str) -> None:
    assert _is_safe_select(sql) is True


# ── Things that MUST be blocked ──────────────────────────────────────────────
@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE x",
        "delete from x",
        "INSERT INTO x VALUES (1)",
        "UPDATE x SET a = 1",
        "ALTER TABLE x ADD COLUMN c INT",
        "CREATE TABLE x (id INT)",
        "PRAGMA database_size",
        "SET statement_timeout = '5s'",
        "ATTACH 'evil.db' AS evil",
        "COPY x TO 'out.csv'",
    ],
)
def test_dml_and_ddl_blocked(sql: str) -> None:
    assert _is_safe_select(sql) is False


# ── Multi-statement injection attempts ───────────────────────────────────────
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; DROP TABLE x;",
        "SELECT 1; SELECT 2",
        "SELECT * FROM x; PRAGMA database_size",
        "WITH y AS (SELECT 1) SELECT * FROM y; DROP TABLE x",
    ],
)
def test_multi_statement_injection_blocked(sql: str) -> None:
    assert _is_safe_select(sql) is False


# ── Empty / malformed input ──────────────────────────────────────────────────
@pytest.mark.parametrize("sql", ["", "   ", ";", "\n\n", "garbage", "explain select 1"])
def test_non_select_blocked(sql: str) -> None:
    assert _is_safe_select(sql) is False


# ── Forbidden keywords inside legitimate-looking SELECTs ─────────────────────
def test_forbidden_keywords_anywhere_blocked() -> None:
    """Defense in depth: if a model produces 'SELECT ... INSERT ...' we still block."""
    cases = [
        "SELECT 1 UNION ALL INSERT INTO x VALUES (1)",
        "SELECT * FROM (DELETE FROM x)",
    ]
    for sql in cases:
        assert _is_safe_select(sql) is False, f"should block: {sql}"
