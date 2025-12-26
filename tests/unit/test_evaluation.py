from __future__ import annotations

from decimal import Decimal

import pytest

from app.candles import Candle
from app.evaluation import MINUTE_MS, compute_entry_evaluation


def _candle(t: int, *, c: float, h: float | None = None, l: float | None = None) -> Candle:
    hh = c if h is None else h
    ll = c if l is None else l
    return Candle(
        t=t,
        o=Decimal(str(c)),
        h=Decimal(str(hh)),
        l=Decimal(str(ll)),
        c=Decimal(str(c)),
        v=Decimal("1"),
    )


def test_compute_entry_evaluation_returns_and_mae_mfe() -> None:
    t0 = 1_700_000_000_000

    candles = []
    # Entry candle at t0: close 100
    candles.append(_candle(t0, c=100.0))

    # Build 60-minute forward window (t0+1m .. t0+60m).
    for i in range(1, 61):
        t = t0 + i * MINUTE_MS
        # Default: flat at 100
        candles.append(_candle(t, c=100.0))

    # Inject closes for forward returns.
    candles = [c for c in candles if c.t not in {t0 + 5 * MINUTE_MS, t0 + 15 * MINUTE_MS, t0 + 60 * MINUTE_MS}]
    candles.append(_candle(t0 + 5 * MINUTE_MS, c=110.0))
    candles.append(_candle(t0 + 15 * MINUTE_MS, c=105.0))
    candles.append(_candle(t0 + 60 * MINUTE_MS, c=120.0, h=120.0, l=120.0))

    # Inject extremes for MAE/MFE over the next 60 minutes.
    candles = [c for c in candles if c.t != t0 + 7 * MINUTE_MS]
    candles.append(_candle(t0 + 7 * MINUTE_MS, c=100.0, h=130.0, l=95.0))

    res = compute_entry_evaluation(entry_t0=t0, candles_1m=candles)
    assert res is not None
    assert res.r_5m == pytest.approx(0.10)
    assert res.r_15m == pytest.approx(0.05)
    assert res.r_60m == pytest.approx(0.20)
    assert res.mae_60m == pytest.approx(-0.05)
    assert res.mfe_60m == pytest.approx(0.30)


def test_compute_entry_evaluation_requires_full_60m_window() -> None:
    t0 = 1_700_000_000_000
    candles = [
        _candle(t0, c=100.0),
        _candle(t0 + 5 * MINUTE_MS, c=110.0),
        _candle(t0 + 15 * MINUTE_MS, c=105.0),
        # Missing +60m candle
    ]
    assert compute_entry_evaluation(entry_t0=t0, candles_1m=candles) is None

