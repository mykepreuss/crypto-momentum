from __future__ import annotations

from app.scoring import ScoreComponents, compute_score, dv_term_from_dvz, percentile_ranks


def test_percentile_ranks_basic_ordering() -> None:
    ranks = percentile_ranks({"a": 1.0, "b": 2.0, "c": 3.0})
    assert ranks["a"] == 0.0
    assert ranks["b"] == 0.5
    assert ranks["c"] == 1.0


def test_percentile_ranks_ties_use_average_rank() -> None:
    ranks = percentile_ranks({"a": 1.0, "b": 1.0, "c": 3.0})
    # a and b share ranks 0 and 1 => avg_rank=0.5 => pct=0.5/(3-1)=0.25
    assert ranks["a"] == 0.25
    assert ranks["b"] == 0.25
    assert ranks["c"] == 1.0


def test_compute_score_matches_spec_formula() -> None:
    components = ScoreComponents(rank_rel_r15=1.0, rank_accel=0.0, dv_term=dv_term_from_dvz(0.0), breakout=1)
    score = compute_score(components)
    assert score == (0.45 * 1.0) + (0.35 * 0.0) + (0.20 * 0.0) + (0.10 * 1.0)

