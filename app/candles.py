from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Candle1m

logger = logging.getLogger(__name__)


def _insert_for_dialect(dialect_name: str):
    if dialect_name == "postgresql":
        return pg_insert
    if dialect_name == "sqlite":
        return sqlite_insert
    return sa.insert


def _get_first(d: dict[str, Any], keys: list[str]) -> object:
    for k in keys:
        if k in d:
            return d[k]
    return None


def _to_decimal(v: object) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None


@dataclass(frozen=True)
class Candle:
    t: int
    o: Decimal
    h: Decimal
    l: Decimal
    c: Decimal
    v: Decimal


def parse_candles(raw: list[dict[str, Any]]) -> list[Candle]:
    candles: list[Candle] = []
    for item in raw:
        t_raw = _get_first(item, ["t", "time", "timestamp"])
        try:
            t = int(t_raw)  # ms candle start
        except (ValueError, TypeError):
            continue

        o = _to_decimal(_get_first(item, ["o", "open"]))
        h = _to_decimal(_get_first(item, ["h", "high"]))
        l = _to_decimal(_get_first(item, ["l", "low"]))
        c = _to_decimal(_get_first(item, ["c", "close"]))
        v = _to_decimal(_get_first(item, ["v", "volume"]))
        if o is None or h is None or l is None or c is None or v is None:
            continue

        candles.append(Candle(t=t, o=o, h=h, l=l, c=c, v=v))
    candles.sort(key=lambda x: x.t)
    return candles


def filter_closed_candles(candles: list[Candle], current_minute_start_ms: int) -> list[Candle]:
    # Candle start times are on minute boundaries. Any candle whose start time is >= current minute
    # start is in-progress (not fully closed).
    return [c for c in candles if c.t < current_minute_start_ms]


class CandleBuffer:
    def __init__(self, maxlen: int) -> None:
        if maxlen <= 0:
            raise ValueError("maxlen must be > 0")
        self._maxlen = maxlen
        self._candles: list[Candle] = []

    def upsert_many(self, candles: list[Candle]) -> None:
        for c in candles:
            self.upsert(c)

    def upsert(self, candle: Candle) -> None:
        if not self._candles:
            self._candles.append(candle)
            return

        last = self._candles[-1]
        if candle.t > last.t:
            self._candles.append(candle)
        elif candle.t == last.t:
            self._candles[-1] = candle
        else:
            # Rare path (backfills/out-of-order): insert/replace into sorted list.
            ts = [c.t for c in self._candles]
            lo, hi = 0, len(ts)
            while lo < hi:
                mid = (lo + hi) // 2
                if ts[mid] < candle.t:
                    lo = mid + 1
                else:
                    hi = mid
            idx = lo
            if idx < len(self._candles) and self._candles[idx].t == candle.t:
                self._candles[idx] = candle
            else:
                self._candles.insert(idx, candle)

        if len(self._candles) > self._maxlen:
            # keep newest maxlen
            self._candles = self._candles[-self._maxlen :]

    def latest(self) -> Optional[Candle]:
        return self._candles[-1] if self._candles else None

    def as_list(self) -> list[Candle]:
        return list(self._candles)


class CandleStore:
    def __init__(self, buffer_size: int) -> None:
        self._buffer_size = buffer_size
        self._buffers: dict[str, CandleBuffer] = {}

    def buffer_for(self, symbol: str) -> CandleBuffer:
        buf = self._buffers.get(symbol)
        if buf is None:
            buf = CandleBuffer(maxlen=self._buffer_size)
            self._buffers[symbol] = buf
        return buf

    def upsert_many(self, symbol: str, candles: list[Candle]) -> None:
        if not candles:
            return
        self.buffer_for(symbol).upsert_many(candles)

    def latest(self, symbol: str) -> Optional[Candle]:
        buf = self._buffers.get(symbol)
        if buf is None:
            return None
        return buf.latest()

    def candles(self, symbol: str) -> list[Candle]:
        buf = self._buffers.get(symbol)
        if buf is None:
            return []
        return buf.as_list()


def candle_rows(symbol: str, candles: list[Candle]) -> list[dict[str, object]]:
    return [
        {
            "symbol": symbol,
            "t": c.t,
            "o": c.o,
            "h": c.h,
            "l": c.l,
            "c": c.c,
            "v": c.v,
        }
        for c in candles
    ]


async def upsert_candle_rows(session: AsyncSession, rows: list[dict[str, object]]) -> None:
    if not rows:
        return

    bind = session.get_bind()
    insert_fn = _insert_for_dialect(bind.dialect.name)

    stmt = insert_fn(Candle1m.__table__).values(rows)
    if hasattr(stmt, "on_conflict_do_update"):
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "t"],
            set_={
                "o": stmt.excluded.o,
                "h": stmt.excluded.h,
                "l": stmt.excluded.l,
                "c": stmt.excluded.c,
                "v": stmt.excluded.v,
            },
        )
    await session.execute(stmt)


async def prune_old_candles(session: AsyncSession, cutoff_ts_ms: int) -> int:
    result = await session.execute(sa.delete(Candle1m).where(Candle1m.t < cutoff_ts_ms))
    deleted = int(result.rowcount or 0)
    return deleted
