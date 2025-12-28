from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from decimal import Decimal

MINUTE_MS = 60_000


@dataclass(frozen=True)
class Bar:
    t: int  # bar start (ms)
    o: Decimal
    h: Decimal
    l: Decimal
    c: Decimal
    v: Decimal


def bar_ms(bar_minutes: int) -> int:
    if int(bar_minutes) <= 0:
        raise ValueError("bar_minutes must be > 0")
    return int(bar_minutes) * MINUTE_MS


def ceil_to_bar_start(t_ms: int, *, bar_minutes: int) -> int:
    """
    Returns the smallest bar start >= t_ms.

    bar start is aligned to epoch multiples of `bar_minutes`.
    """
    bm = bar_ms(bar_minutes)
    t = int(t_ms)
    if t % bm == 0:
        return t
    return ((t // bm) + 1) * bm


class BarAggregator:
    def __init__(self, *, bar_minutes: int) -> None:
        if int(bar_minutes) <= 0:
            raise ValueError("bar_minutes must be > 0")
        self.bar_minutes = int(bar_minutes)
        self.bar_ms = bar_ms(self.bar_minutes)
        self.reset()

    def reset(self) -> None:
        self._cur_start: int | None = None
        self._cur_count = 0
        self._cur_valid = True
        self._cur_expected_t: int | None = None
        self._cur_o: Decimal | None = None
        self._cur_h: Decimal | None = None
        self._cur_l: Decimal | None = None
        self._cur_c: Decimal | None = None
        self._cur_v: Decimal = Decimal("0")

    def ingest_1m(
        self,
        *,
        t: int,
        o: Decimal,
        h: Decimal,
        l: Decimal,
        c: Decimal,
        v: Decimal,
    ) -> Bar | None:
        """
        Ingest a single 1m candle.

        Returns a completed Bar when we observe the first candle of a new bar.
        """
        t_i = int(t)
        start = (t_i // self.bar_ms) * self.bar_ms

        if self._cur_start is None:
            self._cur_start = int(start)
            self._cur_expected_t = int(self._cur_start)

        if int(start) != int(self._cur_start):
            out = self.flush()
            self._cur_start = int(start)
            self._cur_expected_t = int(self._cur_start)
        else:
            out = None

        if self._cur_expected_t is None:
            self._cur_expected_t = int(self._cur_start)

        if t_i != int(self._cur_expected_t):
            self._cur_valid = False
        self._cur_expected_t = int(self._cur_expected_t) + MINUTE_MS

        if self._cur_count == 0:
            self._cur_o = o
            self._cur_h = h
            self._cur_l = l
        else:
            if self._cur_h is not None:
                self._cur_h = max(self._cur_h, h)
            if self._cur_l is not None:
                self._cur_l = min(self._cur_l, l)

        self._cur_c = c
        self._cur_v += v
        self._cur_count += 1
        if self._cur_count > self.bar_minutes:
            self._cur_valid = False
        return out

    def flush(self) -> Bar | None:
        out: Bar | None = None
        if (
            self._cur_start is not None
            and self._cur_valid
            and self._cur_count == self.bar_minutes
            and self._cur_o is not None
            and self._cur_h is not None
            and self._cur_l is not None
            and self._cur_c is not None
        ):
            out = Bar(
                t=int(self._cur_start),
                o=self._cur_o,
                h=self._cur_h,
                l=self._cur_l,
                c=self._cur_c,
                v=self._cur_v,
            )
        self.reset()
        return out


def iter_bars_from_1m_rows(
    rows: Iterable[tuple[int, Decimal, Decimal, Decimal, Decimal, Decimal]],
    *,
    bar_minutes: int,
) -> Iterator[Bar]:
    """
    Deterministically aggregate 1m candles into fixed-size bars.

    Requirements (strict, to avoid silently fabricating bars):
    - Input rows MUST be sorted by t ascending for a single symbol.
    - Bars are emitted only when the bar contains exactly `bar_minutes` contiguous 1m candles.
    - If any minute is missing inside a bar, the entire bar is skipped.
    """
    agg = BarAggregator(bar_minutes=int(bar_minutes))
    for t, o, h, l, c, v in rows:
        out = agg.ingest_1m(t=int(t), o=o, h=h, l=l, c=c, v=v)
        if out is not None:
            yield out
    out = agg.flush()
    if out is not None:
        yield out
