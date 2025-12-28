from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Optional

from app.features import FeatureSet

MINUTE_MS = 60_000


@dataclass(frozen=True)
class CandlePoint:
    t: int
    o: float
    h: float
    l: float
    c: float
    v: float


def _ret(c0: float, ck: float) -> Optional[float]:
    if ck <= 0.0:
        return None
    return (c0 / ck) - 1.0


class _RollingMaxByTime:
    def __init__(self, *, window_steps: int, step_ms: int) -> None:
        if int(window_steps) <= 0:
            raise ValueError("window_steps must be > 0")
        if int(step_ms) <= 0:
            raise ValueError("step_ms must be > 0")
        self._window_ms = int(window_steps) * int(step_ms)
        self._dq: deque[tuple[int, float]] = deque()

    def reset(self) -> None:
        self._dq.clear()

    def add(self, t: int, value: float) -> None:
        cutoff = int(t) - self._window_ms
        # Keep values at the boundary (t - window_ms) so a 20m window includes exactly the prior
        # 20 candles when timestamps are on minute boundaries.
        while self._dq and self._dq[0][0] < cutoff:
            self._dq.popleft()
        while self._dq and self._dq[-1][1] <= value:
            self._dq.pop()
        self._dq.append((int(t), float(value)))

    def max(self) -> Optional[float]:
        return self._dq[0][1] if self._dq else None


class RollingFeatureState:
    """
    Incremental, deterministic feature computation for replay/backtests.

    This is intended to be logically equivalent to `compute_features_with_reason(...)` but
    avoids rescanning full candle buffers for each minute.
    """

    def __init__(
        self,
        *,
        step_ms: int = MINUTE_MS,
        dv_vwap_window_steps: int = 60,
        breakout_window_steps: int = 20,
        trend_bucket_steps: int = 5,
        ema9_period: int = 9,
        ema21_period: int = 21,
    ) -> None:
        if int(step_ms) <= 0:
            raise ValueError("step_ms must be > 0")
        if int(dv_vwap_window_steps) <= 0:
            raise ValueError("dv_vwap_window_steps must be > 0")
        if int(breakout_window_steps) <= 0:
            raise ValueError("breakout_window_steps must be > 0")
        if int(trend_bucket_steps) <= 0:
            raise ValueError("trend_bucket_steps must be > 0")
        if int(ema9_period) <= 0 or int(ema21_period) <= 0:
            raise ValueError("ema periods must be > 0")

        self.step_ms = int(step_ms)
        self._dv_vwap_window_steps = int(dv_vwap_window_steps)
        self._breakout_window_steps = int(breakout_window_steps)
        self._trend_bucket_steps = int(trend_bucket_steps)
        self._ema9_period = int(ema9_period)
        self._ema21_period = int(ema21_period)
        self._alpha9 = 2.0 / (float(self._ema9_period) + 1.0)
        self._alpha21 = 2.0 / (float(self._ema21_period) + 1.0)
        self._bucket_ms = int(self._trend_bucket_steps) * int(self.step_ms)

        self.last_t: Optional[int] = None

        # Close history for required offsets (t0, t0-1, t0-5, t0-10, t0-15).
        self._closes: deque[float] = deque(maxlen=16)
        self._last_high: Optional[float] = None

        # Rolling dv/vwap window (default 60 candles including current).
        self._dv_vwap_window: deque[tuple[float, float, float]] = deque()
        self._dv_sum = 0.0
        self._dv_sum_sq = 0.0
        self._vwap_vol_sum = 0.0
        self._vwap_typical_vol_sum = 0.0

        # Rolling breakout max(high) over prior N candles (tracked as a time-based rolling max).
        self._prior_high_max = _RollingMaxByTime(window_steps=self._breakout_window_steps, step_ms=self.step_ms)
        self._prior_20_max_high_at_t: Optional[float] = None

        # Bucket aggregation to compute EMA9/EMA21 on fully closed buckets.
        self._bucket_start: Optional[int] = None
        self._bucket_count = 0
        self._ema9: Optional[float] = None
        self._ema21: Optional[float] = None
        self._closed_5m_count = 0

    def reset(self) -> None:
        self.last_t = None
        self._closes.clear()
        self._last_high = None

        self._dv_vwap_window.clear()
        self._dv_sum = 0.0
        self._dv_sum_sq = 0.0
        self._vwap_vol_sum = 0.0
        self._vwap_typical_vol_sum = 0.0

        self._prior_high_max.reset()
        self._prior_20_max_high_at_t = None

        self._bucket_start = None
        self._bucket_count = 0
        self._ema9 = None
        self._ema21 = None
        self._closed_5m_count = 0

    def ingest(self, candle: CandlePoint) -> None:
        if self.last_t is not None and int(candle.t) != int(self.last_t) + int(self.step_ms):
            # We require strict 1m contiguity for rolling windows to match the live "missing_*" behaviors.
            self.reset()

        # Breakout uses prior 20 highs excluding current.
        self._prior_20_max_high_at_t = self._prior_high_max.max()

        # Close history
        self._closes.append(float(candle.c))
        self._last_high = float(candle.h)

        # dv + vwap rolling window
        dv = float(candle.c) * float(candle.v)
        typical = (float(candle.h) + float(candle.l) + float(candle.c)) / 3.0
        typical_vol = typical * float(candle.v)

        if len(self._dv_vwap_window) == int(self._dv_vwap_window_steps):
            old_dv, old_vol, old_typical_vol = self._dv_vwap_window.popleft()
            self._dv_sum -= old_dv
            self._dv_sum_sq -= old_dv * old_dv
            self._vwap_vol_sum -= old_vol
            self._vwap_typical_vol_sum -= old_typical_vol

        self._dv_vwap_window.append((dv, float(candle.v), typical_vol))
        self._dv_sum += dv
        self._dv_sum_sq += dv * dv
        self._vwap_vol_sum += float(candle.v)
        self._vwap_typical_vol_sum += typical_vol

        self._ingest_5m_bucket(candle)

        # Update breakout rolling max AFTER computing prior max for this t.
        self._prior_high_max.add(candle.t, float(candle.h))

        self.last_t = int(candle.t)

    def _ingest_5m_bucket(self, candle: CandlePoint) -> None:
        bucket_start = (int(candle.t) // int(self._bucket_ms)) * int(self._bucket_ms)
        if self._bucket_start is None or bucket_start != self._bucket_start:
            self._bucket_start = bucket_start
            self._bucket_count = 0

        self._bucket_count += 1
        # Only treat a bucket as closed if we have exactly N candles and the last candle is the final
        # step of the bucket.
        if self._bucket_count == int(self._trend_bucket_steps) and int(candle.t) == (
            bucket_start + ((int(self._trend_bucket_steps) - 1) * int(self.step_ms))
        ):
            close_bucket = float(candle.c)
            if self._ema9 is None:
                self._ema9 = close_bucket
            else:
                self._ema9 = (self._alpha9 * close_bucket) + ((1.0 - self._alpha9) * self._ema9)

            if self._ema21 is None:
                self._ema21 = close_bucket
            else:
                self._ema21 = (self._alpha21 * close_bucket) + ((1.0 - self._alpha21) * self._ema21)

            self._closed_5m_count += 1

    def baseline_usable_reason(self) -> Optional[str]:
        """
        Mirror the live baseline sanity check (signals.py): we require enough baseline history to
        compute the full feature set at the current t0. When this returns a non-None reason,
        the whole snapshot should be treated as unreliable and skipped.
        """
        if self.last_t is None:
            return "missing_series"
        if len(self._closes) < 16:
            return "missing_offsets"
        if len(self._dv_vwap_window) < int(self._dv_vwap_window_steps):
            return "missing_dv_window"
        if self._vwap_vol_sum <= 0.0:
            return "zero_volume_window"
        if self._prior_20_max_high_at_t is None:
            return "missing_breakout_window"
        if self._closed_5m_count < int(self._ema21_period) or self._ema9 is None or self._ema21 is None:
            return "insufficient_5m_history"
        return None

    def compute_features_with_reason(
        self,
        *,
        baseline: "RollingFeatureState",
        t0: int,
    ) -> tuple[Optional[FeatureSet], Optional[str]]:
        if self.last_t is None or baseline.last_t is None:
            return None, "missing_series"

        if int(t0) != int(self.last_t) or int(t0) != int(baseline.last_t):
            return None, "missing_t0"

        if len(self._closes) < 16 or len(baseline._closes) < 16:
            return None, "missing_offsets"

        c0 = float(self._closes[-1])
        c1 = float(self._closes[-2])
        c5 = float(self._closes[-6])
        c10 = float(self._closes[-11])
        c15 = float(self._closes[-16])

        b0 = float(baseline._closes[-1])
        b5 = float(baseline._closes[-6])
        b10 = float(baseline._closes[-11])
        b15 = float(baseline._closes[-16])

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

        if len(self._dv_vwap_window) < int(self._dv_vwap_window_steps):
            return None, "missing_dv_window"

        n = float(self._dv_vwap_window_steps)
        dv_mean = self._dv_sum / n
        dv_var = (self._dv_sum_sq / n) - (dv_mean * dv_mean)
        dv_std = math.sqrt(max(0.0, dv_var))
        eps = 1e-9
        dv_last = float(self._dv_vwap_window[-1][0])
        dv_z = (dv_last - dv_mean) / max(dv_std, eps)
        avg_dv_1m = dv_mean

        if self._vwap_vol_sum <= 0.0:
            return None, "zero_volume_window"
        vwap60 = self._vwap_typical_vol_sum / self._vwap_vol_sum
        if vwap60 <= 0.0:
            return None, "bad_vwap"
        extension = (c0 - vwap60) / vwap60

        if self._prior_20_max_high_at_t is None:
            return None, "missing_breakout_window"
        breakout = 1 if c0 > float(self._prior_20_max_high_at_t) else 0

        if self._closed_5m_count < int(self._ema21_period) or self._ema9 is None or self._ema21 is None:
            return None, "insufficient_5m_history"
        trend_ok = float(self._ema9) > float(self._ema21)

        high = float(self._last_high) if self._last_high is not None else c0

        return (
            FeatureSet(
                t0=int(t0),
                price=float(c0),
                high=float(high),
                r1=float(r1),
                r5=float(r5),
                r15=float(r15),
                rel_r5=float(rel_r5),
                rel_r15=float(rel_r15),
                accel=float(accel),
                dv_z=float(dv_z),
                avg_dv_1m=float(avg_dv_1m),
                breakout=int(breakout),
                trend_ok=bool(trend_ok),
                extension=float(extension),
            ),
            None,
        )
