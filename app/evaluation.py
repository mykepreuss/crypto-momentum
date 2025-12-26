from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.candles import Candle

MINUTE_MS = 60_000


@dataclass(frozen=True)
class EntryEvaluation:
    r_5m: Optional[float]
    r_15m: Optional[float]
    r_60m: Optional[float]
    mae_60m: Optional[float]
    mfe_60m: Optional[float]


def _ret(entry_close: float, later_close: float) -> Optional[float]:
    if entry_close <= 0.0:
        return None
    return (later_close / entry_close) - 1.0


def compute_entry_evaluation(
    *,
    entry_t0: int,
    candles_1m: list[Candle],
) -> Optional[EntryEvaluation]:
    """
    Compute forward returns (+5m/+15m/+60m) and MAE/MFE over the next 60 minutes.

    Candle timestamps are candle *start* times in ms. For consistency with our
    backward returns in the signal engine, forward offsets use start times too:
      r_5m uses close at (t0 + 5m) vs close at t0, etc.
    """
    if not candles_1m:
        return None

    by_t = {c.t: c for c in candles_1m}
    entry = by_t.get(entry_t0)
    if entry is None:
        return None

    end_t = entry_t0 + 60 * MINUTE_MS
    if end_t not in by_t:
        # Not enough future data yet.
        return None

    entry_close = float(entry.c)

    t5 = entry_t0 + 5 * MINUTE_MS
    t15 = entry_t0 + 15 * MINUTE_MS
    r_5m = _ret(entry_close, float(by_t[t5].c)) if t5 in by_t else None
    r_15m = _ret(entry_close, float(by_t[t15].c)) if t15 in by_t else None
    r_60m = _ret(entry_close, float(by_t[end_t].c))

    future = [c for c in candles_1m if entry_t0 < c.t <= end_t]
    if not future:
        return EntryEvaluation(r_5m=r_5m, r_15m=r_15m, r_60m=r_60m, mae_60m=None, mfe_60m=None)

    min_low = min(float(c.l) for c in future)
    max_high = max(float(c.h) for c in future)
    mae_60m = _ret(entry_close, min_low)
    mfe_60m = _ret(entry_close, max_high)

    return EntryEvaluation(r_5m=r_5m, r_15m=r_15m, r_60m=r_60m, mae_60m=mae_60m, mfe_60m=mfe_60m)
