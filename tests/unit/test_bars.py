from __future__ import annotations

from decimal import Decimal

import pytest

from app.bars import BarAggregator, iter_bars_from_1m_rows


def _d(x: float) -> Decimal:
    return Decimal(str(x))


def _minute_rows(*, start_t: int, minutes: int) -> list[tuple[int, Decimal, Decimal, Decimal, Decimal, Decimal]]:
    rows: list[tuple[int, Decimal, Decimal, Decimal, Decimal, Decimal]] = []
    for i in range(minutes):
        t = int(start_t) + (i * 60_000)
        o = _d(100.0 + i)
        h = _d(101.0 + i)
        l = _d(99.0 + i)
        c = _d(100.5 + i)
        v = _d(1.0)
        rows.append((t, o, h, l, c, v))
    return rows


def test_iter_bars_from_1m_rows_one_bar_complete() -> None:
    rows = _minute_rows(start_t=0, minutes=60)
    bars = list(iter_bars_from_1m_rows(rows, bar_minutes=60))
    assert len(bars) == 1
    b = bars[0]
    assert b.t == 0
    assert b.o == _d(100.0)
    assert b.h == _d(101.0 + 59)
    assert b.l == _d(99.0)
    assert b.c == _d(100.5 + 59)
    assert b.v == _d(60.0)


def test_iter_bars_from_1m_rows_missing_minute_skips_bar() -> None:
    rows = _minute_rows(start_t=0, minutes=60)
    rows.pop(5)  # remove t=5m
    bars = list(iter_bars_from_1m_rows(rows, bar_minutes=60))
    assert bars == []


def test_bar_aggregator_yields_multiple_bars() -> None:
    rows = _minute_rows(start_t=0, minutes=120)
    agg = BarAggregator(bar_minutes=60)
    out = []
    for t, o, h, l, c, v in rows:
        b = agg.ingest_1m(t=t, o=o, h=h, l=l, c=c, v=v)
        if b is not None:
            out.append(b)
    last = agg.flush()
    if last is not None:
        out.append(last)
    assert [b.t for b in out] == [0, 3_600_000]


def test_bar_aggregator_skips_partial_first_bar_but_keeps_next() -> None:
    # Start at t=1m into the hour (partial bar), then provide a full next hour.
    partial = _minute_rows(start_t=60_000, minutes=59)  # missing t=0
    full_next = _minute_rows(start_t=3_600_000, minutes=60)
    rows = partial + full_next

    bars = list(iter_bars_from_1m_rows(rows, bar_minutes=60))
    assert len(bars) == 1
    assert bars[0].t == 3_600_000


def test_bar_minutes_must_be_positive() -> None:
    with pytest.raises(ValueError):
        _ = list(iter_bars_from_1m_rows([], bar_minutes=0))

