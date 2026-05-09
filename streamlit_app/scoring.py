"""Composite scoring for the Reliable Income Recommender.

The DuckDB serving table holds normalized per-factor sub-scores in [0, 1]:
    demand_score, reliability_score, trend_score, yellow_share_score

The driver sets weights via sliders. This module combines the sub-scores
into a final `opportunity_score` and assigns a recommendation band.

All math here is intentionally simple and pure — easy to test, easy to swap
out for a learned ranker later.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class Weights:
    """Driver preference weights. Should sum to 1.0 (we normalize to be safe).

    All weights are positive — the sub-scores are pre-aligned so higher = better
    for every factor:

      * demand_score:        higher = busier
      * reliability_score:   higher = less volatile demand
      * trend_score:         higher = growing market
      * yellow_share_score:  higher = stronger yellow position (0 if HVFHV view)
    """
    demand: float = 0.40
    reliability: float = 0.30
    trend: float = 0.20
    yellow_share: float = 0.10

    def normalized(self) -> "Weights":
        total = self.demand + self.reliability + self.trend + self.yellow_share
        if total <= 0:
            return DEFAULT_WEIGHTS
        return Weights(
            demand=self.demand / total,
            reliability=self.reliability / total,
            trend=self.trend / total,
            yellow_share=self.yellow_share / total,
        )

    def as_dict(self) -> dict[str, float]:
        return {
            "demand": self.demand,
            "reliability": self.reliability,
            "trend": self.trend,
            "yellow_share": self.yellow_share,
        }


DEFAULT_WEIGHTS = Weights()

# Bands are applied to the final opportunity_score.
# Tuned for visual usefulness on the choropleth — adjust if the distribution
# shifts a lot when a real ML model lands.
RECOMMENDATION_BANDS = (
    ("avoid",      0.00),  # bottom band; included for symmetry
    ("okay",       0.30),
    ("good",       0.55),
    ("top_pick",   0.75),
)


def compute_opportunity_score(df: pd.DataFrame, weights: Weights) -> pd.Series:
    """Return a Series of opportunity scores aligned to df.index.

    Expects the four sub-score columns to exist on df. Missing columns are
    treated as 0.5 (neutral) so a partial result still ranks reasonably.
    """
    w = weights.normalized()
    cols = {
        "demand_score":       w.demand,
        "reliability_score":  w.reliability,
        "trend_score":        w.trend,
        "yellow_share_score": w.yellow_share,
    }
    score = pd.Series(0.0, index=df.index)
    for col, weight in cols.items():
        if col in df.columns:
            # Defense in depth: any NaN in a sub-score would poison the
            # composite via NaN-propagation, leaving every zone in the
            # "avoid" band (because NaN >= threshold is always False).
            # Treat missing/NaN as the neutral 0.5 sub-score.
            sub = df[col].fillna(0.5)
        else:
            sub = 0.5
        score = score + weight * sub
    return score.clip(lower=0.0, upper=1.0).round(4)


def assign_band(score: float) -> str:
    """Map a numeric opportunity_score to a categorical band label."""
    last_label = RECOMMENDATION_BANDS[0][0]
    for label, threshold in RECOMMENDATION_BANDS:
        if score >= threshold:
            last_label = label
    return last_label


def assign_bands(scores: Iterable[float]) -> list[str]:
    return [assign_band(float(s)) for s in scores]


def annotate_recommendations(df: pd.DataFrame, weights: Weights) -> pd.DataFrame:
    """Add `opportunity_score` + `recommendation_band` to the dataframe."""
    out = df.copy()
    out["opportunity_score"] = compute_opportunity_score(out, weights)
    out["recommendation_band"] = assign_bands(out["opportunity_score"])
    return out
