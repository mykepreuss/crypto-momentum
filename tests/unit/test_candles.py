from __future__ import annotations

from decimal import Decimal

import pytest

from app.candles import Candle, CandleBuffer, filter_closed_candles, parse_candles


def test_parse_and_filter_closed_candles() -> None:
    raw = [
        {"t": 60_000, "o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10"},
        {"t": 120_000, "o": "1.5", "h": "1.6", "l": "1.4", "c": "1.55", "v": "5"},
    ]
    candles = parse_candles(raw)
    assert [c.t for c in candles] == [60_000, 120_000]

    # current minute start is 120_000, so the 120_000 candle is in-progress and must be ignored
    closed = filter_closed_candles(candles, current_minute_start_ms=120_000)
    assert [c.t for c in closed] == [60_000]


def test_candle_buffer_upsert_and_trim() -> None:
    buf = CandleBuffer(maxlen=3)

    def c(t: int, close: str) -> Candle:
        d = Decimal(close)
        return Candle(t=t, o=d, h=d, l=d, c=d, v=Decimal("1"))

    buf.upsert_many([c(1, "1"), c(2, "2"), c(3, "3")])
    assert [x.t for x in buf.as_list()] == [1, 2, 3]

    # Replace existing candle
    buf.upsert(c(2, "2.2"))
    assert [x.c for x in buf.as_list() if x.t == 2] == [Decimal("2.2")]

    # Append and trim oldest
    buf.upsert(c(4, "4"))
    assert [x.t for x in buf.as_list()] == [2, 3, 4]

    # Out-of-order insert should still keep sorted + trimmed
    buf.upsert(c(0, "0"))
    assert [x.t for x in buf.as_list()] == [2, 3, 4]


def test_candle_buffer_requires_positive_maxlen() -> None:
    with pytest.raises(ValueError):
        CandleBuffer(maxlen=0)

