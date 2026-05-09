"""Tests for the composite opportunity-score function."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scoring import (  # noqa: E402
    DEFAULT_WEIGHTS,
    Weights,
    annotate_recommendations,
    assign_band,
    compute_opportunity_score,
)


def _df(**cols) -> pd.DataFrame:
    return pd.DataFrame(cols)


def test_weights_normalize_to_sum_one() -> None:
    w = Weights(demand=2.0, reliability=2.0, trend=2.0, yellow_share=2.0).normalized()
    total = w.demand + w.reliability + w.trend + w.yellow_share
    assert pytest.approx(total) == 1.0


def test_zero_weights_falls_back_to_defaults() -> None:
    w = Weights(demand=0, reliability=0, trend=0, yellow_share=0).normalized()
    assert w == DEFAULT_WEIGHTS


def test_score_with_all_perfect_subscores_is_one() -> None:
    df = _df(
        demand_score=[1.0],
        reliability_score=[1.0],
        trend_score=[1.0],
        yellow_share_score=[1.0],
    )
    s = compute_opportunity_score(df, DEFAULT_WEIGHTS)
    assert pytest.approx(s.iloc[0]) == 1.0


def test_score_with_all_zero_subscores_is_zero() -> None:
    df = _df(
        demand_score=[0.0],
        reliability_score=[0.0],
        trend_score=[0.0],
        yellow_share_score=[0.0],
    )
    s = compute_opportunity_score(df, DEFAULT_WEIGHTS)
    assert s.iloc[0] == 0.0


def test_score_respects_weights() -> None:
    """Heavy demand weight should dominate the score."""
    df = _df(
        demand_score=[1.0, 0.0],
        reliability_score=[0.0, 1.0],
        trend_score=[0.5, 0.5],
        yellow_share_score=[0.5, 0.5],
    )
    demand_heavy = Weights(demand=1.0, reliability=0.0, trend=0.0, yellow_share=0.0)
    s = compute_opportunity_score(df, demand_heavy)
    # Row 0 has perfect demand → should outscore row 1
    assert s.iloc[0] > s.iloc[1]


def test_score_clipped_to_unit_interval() -> None:
    """Even with weird weights, scores stay in [0, 1]."""
    df = _df(
        demand_score=[1.5, -0.5],   # invalid sub-scores
        reliability_score=[1.0, 1.0],
        trend_score=[1.0, 1.0],
        yellow_share_score=[1.0, 1.0],
    )
    s = compute_opportunity_score(df, DEFAULT_WEIGHTS)
    assert (s >= 0.0).all() and (s <= 1.0).all()


def test_missing_columns_treated_as_neutral() -> None:
    df = _df(demand_score=[1.0])  # only one of four sub-scores present
    s = compute_opportunity_score(df, DEFAULT_WEIGHTS)
    # Should not crash; result should be between 0 and 1
    assert 0.0 <= s.iloc[0] <= 1.0


@pytest.mark.parametrize("score, expected", [
    (0.0,  "avoid"),
    (0.20, "avoid"),
    (0.30, "okay"),
    (0.45, "okay"),
    (0.55, "good"),
    (0.65, "good"),
    (0.75, "top_pick"),
    (1.00, "top_pick"),
])
def test_band_assignment(score: float, expected: str) -> None:
    assert assign_band(score) == expected


def test_annotate_recommendations_adds_columns() -> None:
    df = _df(
        demand_score=[0.9, 0.1],
        reliability_score=[0.8, 0.1],
        trend_score=[0.7, 0.1],
        yellow_share_score=[0.6, 0.1],
    )
    out = annotate_recommendations(df, DEFAULT_WEIGHTS)
    assert "opportunity_score" in out.columns
    assert "recommendation_band" in out.columns
    # Row 0 should rank above row 1
    assert out["opportunity_score"].iloc[0] > out["opportunity_score"].iloc[1]
