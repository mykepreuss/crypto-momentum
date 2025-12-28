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
        price=1.0,
        peak_price=1.0,
        peak_high=1.0,
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
        price=1.0,
        peak_price=1.0,
        peak_high=1.0,
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
        price=1.0,
        peak_price=1.0,
        peak_high=1.0,
        peak_ts=0,
        settings=settings,
    )
    assert reason == "stall"


def test_determine_exit_reason_trend_trailing_ignores_score_threshold() -> None:
    settings = Settings(
        exit_score_threshold=0.55,
        exit_mode="trend_trailing",
        trailing_stop_pct=0.10,
        stall_minutes=10,
        stall_dvz_max=1.0,
    )
    # Score below exit threshold would normally exit, but trend_trailing should not.
    reason = determine_exit_reason(
        score=0.10,
        trend_ok=True,
        dv_z=0.0,
        t0=1_000_000,
        price=99.0,
        peak_price=100.0,
        peak_high=100.0,
        peak_ts=0,
        settings=settings,
    )
    assert reason is None


def test_determine_exit_reason_trend_trailing_trailing_stop() -> None:
    settings = Settings(
        exit_score_threshold=0.55,
        exit_mode="trend_trailing",
        trailing_stop_pct=0.10,
        stall_minutes=10,
        stall_dvz_max=1.0,
    )
    reason = determine_exit_reason(
        score=0.99,
        trend_ok=True,
        dv_z=0.0,
        t0=1_000_000,
        price=90.0,
        peak_price=100.0,
        peak_high=100.0,
        peak_ts=0,
        settings=settings,
    )
    assert reason == "trailing_stop"

