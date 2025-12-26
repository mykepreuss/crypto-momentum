from __future__ import annotations

from decimal import Decimal

from app.candles import Candle
from app.features import compute_features


def _constant_candles(symbol_close: Decimal, start_t: int, minutes: int) -> list[Candle]:
    candles: list[Candle] = []
    for i in range(minutes):
        t = start_t + i * 60_000
        candles.append(
            Candle(
                t=t,
                o=symbol_close,
                h=symbol_close,
                l=symbol_close,
                c=symbol_close,
                v=Decimal("1"),
            )
        )
    return candles


def test_compute_features_constant_series() -> None:
    # 120 candles ending at minute index 119 (119 % 5 == 4) so the last 5m bucket is fully closed.
    symbol = _constant_candles(Decimal("10"), start_t=0, minutes=120)
    baseline = _constant_candles(Decimal("100"), start_t=0, minutes=120)

    fs = compute_features(symbol, baseline)
    assert fs is not None
    assert fs.r1 == 0.0
    assert fs.r5 == 0.0
    assert fs.r15 == 0.0
    assert fs.rel_r5 == 0.0
    assert fs.rel_r15 == 0.0
    assert fs.accel == 0.0
    assert fs.dv_z == 0.0
    assert fs.extension == 0.0
    assert fs.breakout == 0
    # Constant 5m series => EMA9 == EMA21 => trend_ok should be False.
    assert fs.trend_ok is False

