from __future__ import annotations

from app.config import Settings
from app.state_machine import determine_exit_reason


def test_determine_exit_reason_score_threshold_wins() -> None:
    settings = Settings(exit_score_threshold=0.55, stall_minutes=10, stall_dvz_max=1.0)
    reason = determine_exit_reason(
        score=0.50,
        trend_ok=False,
        dv_z=0.0,
        t0=1_000_000,
        peak_ts=0,
        settings=settings,
    )
    assert reason == "score_below_exit_threshold"


def test_determine_exit_reason_trend_break() -> None:
    settings = Settings(exit_score_threshold=0.55, stall_minutes=10, stall_dvz_max=1.0)
    reason = determine_exit_reason(
        score=0.60,
        trend_ok=False,
        dv_z=0.0,
        t0=1_000_000,
        peak_ts=0,
        settings=settings,
    )
    assert reason == "trend_break"


def test_determine_exit_reason_stall() -> None:
    settings = Settings(exit_score_threshold=0.55, stall_minutes=10, stall_dvz_max=1.0)
    reason = determine_exit_reason(
        score=0.60,
        trend_ok=True,
        dv_z=0.5,
        t0=10 * 60_000,
        peak_ts=0,
        settings=settings,
    )
    assert reason == "stall"

