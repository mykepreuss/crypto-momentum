from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from app.candles import Candle

MINUTE_MS = 60_000
FIVE_MIN_MS = 5 * MINUTE_MS


@dataclass(frozen=True)
class FeatureSet:
    t0: int
    price: float
    high: float
    r1: float
    r5: float
    r15: float
    rel_r5: float
    rel_r15: float
    accel: float
    dv_z: float
    avg_dv_1m: float
    breakout: int
    trend_ok: bool
    extension: float


def _to_f(v) -> float:
    return float(v)


def _ret(c0: float, ck: float) -> Optional[float]:
    if ck <= 0.0:
        return None
    return (c0 / ck) - 1.0


def _ema_last(values: list[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    alpha = 2.0 / (period + 1.0)
    ema = values[0]
    for v in values[1:]:
        ema = (alpha * v) + ((1.0 - alpha) * ema)
    return ema


def _rollup_5m_closed(candles_1m: list[Candle]) -> list[float]:
    # Return closes for fully closed 5m buckets only (exactly 5 1m candles in the bucket).
    buckets: dict[int, list[Candle]] = {}
    for c in candles_1m:
        bucket = (c.t // FIVE_MIN_MS) * FIVE_MIN_MS
        buckets.setdefault(bucket, []).append(c)

    closes: list[tuple[int, float]] = []
    for bucket, cs in buckets.items():
        if len(cs) != 5:
            continue
        cs_sorted = sorted(cs, key=lambda x: x.t)
        closes.append((bucket, _to_f(cs_sorted[-1].c)))

    closes.sort(key=lambda x: x[0])
    return [c for (_t, c) in closes]


def _last_common_t(symbol: list[Candle], baseline: dict[int, Candle]) -> Optional[int]:
    # Find the most recent 1m candle start time that exists in both series.
    for c in reversed(symbol):
        if c.t in baseline:
            return c.t
    return None


def compute_features_with_reason(
    symbol_candles: list[Candle],
    baseline_candles: list[Candle],
    *,
    t0: Optional[int] = None,
) -> tuple[Optional[FeatureSet], Optional[str]]:
    if not symbol_candles or not baseline_candles:
        return None, "missing_series"

    baseline_by_t = {c.t: c for c in baseline_candles}
    symbol_by_t = {c.t: c for c in symbol_candles}

    if t0 is None:
        t0 = _last_common_t(symbol_candles, baseline_by_t)
        if t0 is None:
            return None, "no_common_t0"
    else:
        if t0 not in symbol_by_t or t0 not in baseline_by_t:
            return None, "missing_t0"

    # Required 1m offsets for returns/acceleration.
    required_offsets = [0, 1, 5, 10, 15]
    for k in required_offsets:
        t = t0 - (k * MINUTE_MS)
        if t not in symbol_by_t or t not in baseline_by_t:
            return None, "missing_offsets"

    c0 = _to_f(symbol_by_t[t0].c)
    h0 = _to_f(symbol_by_t[t0].h)
    c1 = _to_f(symbol_by_t[t0 - MINUTE_MS].c)
    c5 = _to_f(symbol_by_t[t0 - 5 * MINUTE_MS].c)
    c10 = _to_f(symbol_by_t[t0 - 10 * MINUTE_MS].c)
    c15 = _to_f(symbol_by_t[t0 - 15 * MINUTE_MS].c)

    b0 = _to_f(baseline_by_t[t0].c)
    b5 = _to_f(baseline_by_t[t0 - 5 * MINUTE_MS].c)
    b10 = _to_f(baseline_by_t[t0 - 10 * MINUTE_MS].c)
    b15 = _to_f(baseline_by_t[t0 - 15 * MINUTE_MS].c)

    r1 = _ret(c0, c1)
    r5 = _ret(c0, c5)
    r15 = _ret(c0, c15)
    if r1 is None or r5 is None or r15 is None:
        return None, "bad_returns"

    br5 = _ret(b0, b5)
    br15 = _ret(b0, b15)
    if br5 is None or br15 is None:
        return None, "bad_baseline_returns"

    rel_r5 = r5 - br5
    rel_r15 = r15 - br15

    r5_prev = _ret(c5, c10)
    br5_prev = _ret(b5, b10)
    if r5_prev is None or br5_prev is None:
        return None, "bad_accel"

    rel_r5_prev = r5_prev - br5_prev
    accel = rel_r5 - rel_r5_prev

    # dv_z + avg dv + vwap extension over last 60 closed 1m candles ending at t0.
    dv: list[float] = []
    vwap_num = 0.0
    vwap_den = 0.0
    for i in range(60):
        t = t0 - ((59 - i) * MINUTE_MS)
        c = symbol_by_t.get(t)
        if c is None:
            return None, "missing_dv_window"
        close = _to_f(c.c)
        vol = _to_f(c.v)
        dv.append(close * vol)

        typical = (_to_f(c.h) + _to_f(c.l) + close) / 3.0
        vwap_num += typical * vol
        vwap_den += vol

    dv_mean = sum(dv) / len(dv)
    dv_var = sum((x - dv_mean) ** 2 for x in dv) / len(dv)
    dv_std = math.sqrt(dv_var)
    eps = 1e-9
    dv_z = (dv[-1] - dv_mean) / max(dv_std, eps)
    avg_dv_1m = dv_mean

    if vwap_den <= 0.0:
        return None, "zero_volume_window"
    vwap60 = vwap_num / vwap_den
    if vwap60 <= 0.0:
        return None, "bad_vwap"
    extension = (c0 - vwap60) / vwap60

    # Breakout: C0 > max(high over prior 20 closed 1m candles)
    prior_highs: list[float] = []
    for i in range(1, 21):
        t = t0 - (i * MINUTE_MS)
        c = symbol_by_t.get(t)
        if c is None:
            return None, "missing_breakout_window"
        prior_highs.append(_to_f(c.h))
    breakout = 1 if c0 > max(prior_highs) else 0

    # Trend filter using fully closed 5m candles.
    candles_up_to_t0 = [c for c in symbol_candles if c.t <= t0]
    closes_5m = _rollup_5m_closed(candles_up_to_t0)
    ema9 = _ema_last(closes_5m, 9)
    ema21 = _ema_last(closes_5m, 21)
    if ema9 is None or ema21 is None:
        return None, "insufficient_5m_history"
    trend_ok = ema9 > ema21

    return (
        FeatureSet(
            t0=t0,
            price=c0,
            high=h0,
            r1=r1,
            r5=r5,
            r15=r15,
            rel_r5=rel_r5,
            rel_r15=rel_r15,
            accel=accel,
            dv_z=dv_z,
            avg_dv_1m=avg_dv_1m,
            breakout=breakout,
            trend_ok=trend_ok,
            extension=extension,
        ),
        None,
    )


def compute_features(symbol_candles: list[Candle], baseline_candles: list[Candle]) -> Optional[FeatureSet]:
    fs, _reason = compute_features_with_reason(symbol_candles, baseline_candles)
    return fs
