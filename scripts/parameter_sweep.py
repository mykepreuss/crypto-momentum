#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import itertools
import logging
import math
import random
import sys
from array import array
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import func, select

from app.config import Settings, get_settings
from app.db import create_engine, create_sessionmaker, session_scope
from app.lookback_eval import CostModel, buy_with_costs, sell_with_costs
from app.models import Candle1mHist, CandleHistBar, UniverseMembership
from app.replay import CandlePoint, MINUTE_MS, RollingFeatureState
from app.scoring import ScoreComponents, compute_score, dv_term_from_dvz, percentile_ranks
from app.state_machine import determine_exit_reason
from app.time_utils import floor_minute_ms, now_ms


DAY_MS = 24 * 60 * MINUTE_MS
SENTINEL_U16 = 65_535

logger = logging.getLogger("parameter_sweep")


def _decimal_equal(a: object, b: object) -> bool:
    try:
        return Decimal(str(a)) == Decimal(str(b))
    except (InvalidOperation, ValueError, TypeError):
        return False


async def _assert_hist_bars_close_timestamps(*, session, timeframe_min: int, symbol: str) -> None:
    """
    Guardrail: `candles_hist_bars.t` must represent BAR CLOSE time.

    Earlier versions stored bars at bar start while also using bar close prices, which introduced
    a lookahead in offline sweeps/evals. This check detects that condition and tells the operator
    to rebuild with `scripts/build_hist_bars.py --replace`.
    """
    row = (
        await session.execute(
            select(CandleHistBar.t, CandleHistBar.c)
            .where(CandleHistBar.timeframe_min == int(timeframe_min), CandleHistBar.symbol == str(symbol))
            .order_by(CandleHistBar.t.desc())
            .limit(1)
        )
    ).first()
    if row is None:
        return
    bar_t, bar_c = row

    bar_ms = int(timeframe_min) * MINUTE_MS
    close_semantics_last_1m_t = int(bar_t) - MINUTE_MS
    start_semantics_last_1m_t = int(bar_t) + int(bar_ms) - MINUTE_MS

    c_close = (
        await session.execute(
            select(Candle1mHist.c).where(Candle1mHist.symbol == str(symbol), Candle1mHist.t == int(close_semantics_last_1m_t))
        )
    ).scalar_one_or_none()
    if c_close is not None and _decimal_equal(c_close, bar_c):
        return

    c_start = (
        await session.execute(
            select(Candle1mHist.c).where(Candle1mHist.symbol == str(symbol), Candle1mHist.t == int(start_semantics_last_1m_t))
        )
    ).scalar_one_or_none()
    if c_start is not None and _decimal_equal(c_start, bar_c):
        raise SystemExit(
            "candles_hist_bars appears to use BAR START timestamps (old behavior). "
            "Rebuild with: python scripts/build_hist_bars.py ... --replace (stores BAR CLOSE timestamps)."
        )

    logger.warning(
        "hist_bars_timestamp_semantics_inconclusive",
        extra={"symbol": str(symbol), "timeframe_min": int(timeframe_min), "bar_t": int(bar_t)},
    )


@dataclass(frozen=True)
class AlertEvent:
    symbol: str
    t0: int
    price: float
    score: float
    alert_type: str  # MOMENTUM_UP|MOMENTUM_SLOWING


@dataclass(frozen=True)
class TradeRecord:
    symbol: str
    entry_t0: int
    exit_t0: int
    exit_reason: str
    entry_price: float
    exit_price: float
    duration_min: int
    gross_ret: float
    net_ret: float


@dataclass(frozen=True)
class SweepResult:
    config_id: str
    params: dict[str, object]

    entry_alerts: int
    exit_alerts: int
    trades: int

    final_equity: float
    final_return: float
    final_equity_after_do: float
    final_return_after_do: float

    baseline_equity: float
    baseline_return: float
    equity_delta: float
    equity_delta_after_do: float

    total_fee_paid: float
    total_slippage_paid: float
    win_rate_net: float


class _WindowData:
    """
    Precomputed per-step data (scores + raw prices) for a fixed window.

    Storage strategy:
    - Raw OHLC (close + high) is stored for *all* required symbols (candidates + BTC + ETH + baseline).
    - Features used for gates/scoring (dv_z, avg_dv_1m, extension, trend_ok, score) are stored for candidates.
    - For each minute, we store candidate indices sorted by score (descending) in a compact u16 array.
    """

    def __init__(
        self,
        *,
        step_ms: int,
        ingest_start_t0: int,
        start_t0: int,
        end_t0: int,
        candidate_symbols: list[str],
        baseline_symbol: str,
        btc_symbol: str,
        eth_symbol: str,
        all_symbols: list[str],
        raw_close_by_slot: list[array],
        raw_high_by_slot: list[array],
        cand_slot_by_idx: list[int],
        score_by_idx: list[array],
        rank_rel_r15_by_idx: list[array],
        dvz_by_idx: list[array],
        avg_dv_by_idx: list[array],
        extension_by_idx: list[array],
        trend_ok_by_idx: list[array],
        baseline_trend_ok_by_time: array,
        order_flat: array,
        order_len: array,
    ) -> None:
        self.step_ms = int(step_ms)
        self.ingest_start_t0 = int(ingest_start_t0)
        self.start_t0 = int(start_t0)
        self.end_t0 = int(end_t0)
        self.candidate_symbols = candidate_symbols
        self.baseline_symbol = str(baseline_symbol)
        self.btc_symbol = str(btc_symbol)
        self.eth_symbol = str(eth_symbol)

        self.all_symbols = all_symbols
        self._slot_by_symbol = {s: i for i, s in enumerate(all_symbols)}

        self.raw_close_by_slot = raw_close_by_slot
        self.raw_high_by_slot = raw_high_by_slot

        self.cand_slot_by_idx = cand_slot_by_idx
        self.score_by_idx = score_by_idx
        self.rank_rel_r15_by_idx = rank_rel_r15_by_idx
        self.dvz_by_idx = dvz_by_idx
        self.avg_dv_by_idx = avg_dv_by_idx
        self.extension_by_idx = extension_by_idx
        self.trend_ok_by_idx = trend_ok_by_idx
        self.baseline_trend_ok_by_time = baseline_trend_ok_by_time

        self.order_flat = order_flat
        self.order_len = order_len

        self.n_times = len(order_len)
        self.n_candidates = len(candidate_symbols)

    def t0_at(self, time_idx: int) -> int:
        return self.ingest_start_t0 + (int(time_idx) * int(self.step_ms))

    def idx_for_t0(self, t0: int) -> int:
        return int((int(t0) - int(self.ingest_start_t0)) / int(self.step_ms))

    def slot_of(self, symbol: str) -> Optional[int]:
        return self._slot_by_symbol.get(symbol)

    def close_at_or_before(self, *, slot: int, time_idx: int) -> Optional[float]:
        i = int(time_idx)
        arr = self.raw_close_by_slot[slot]
        while i >= 0:
            px = float(arr[i])
            if not math.isnan(px):
                return px
            i -= 1
        return None


def _parse_symbols_csv(s: str) -> list[str]:
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _ms(minutes: int) -> int:
    return int(minutes * 60_000)


def _expire(times: list[int], *, cutoff: int) -> int:
    keep = [t for t in times if t >= cutoff]
    times[:] = keep
    return len(times)


def _parse_grid_kv(s: str) -> tuple[str, str]:
    if "=" not in s:
        raise ValueError(f"grid entry must be like key=v1,v2,...; got {s!r}")
    k, v = s.split("=", 1)
    k = k.strip()
    v = v.strip()
    if not k:
        raise ValueError(f"grid key is empty in {s!r}")
    if not v:
        raise ValueError(f"grid values are empty for key {k!r}")
    return k, v


_SWEEP_TYPES: dict[str, type] = {
    "dvz_min": float,
    "min_dv_1m_usd": float,
    "extension_max": float,
    "entry_score_threshold": float,
    "exit_score_threshold": float,
    "stall_minutes": int,
    "stall_dvz_max": float,
    "global_entry_cooldown_min": int,
    "symbol_entry_cooldown_min": int,
    "symbol_exit_cooldown_min": int,
    "max_entry_alerts_per_scan": int,
}


def _default_grid() -> dict[str, list[object]]:
    # Tuned for Crypto.com "top N" universes where many markets have low 1m $ volume.
    return {
        "min_dv_1m_usd": [0.0, 100.0, 500.0, 2_000.0],
        "dvz_min": [-0.5, 0.0, 0.5, 1.5],
        "entry_score_threshold": [0.70, 0.75, 0.80],
        "extension_max": [0.08, 0.12],
    }


def _parse_grid_args(grid_args: list[str]) -> dict[str, list[object]]:
    if not grid_args:
        return _default_grid()

    grid: dict[str, list[object]] = {}
    for raw in grid_args:
        k, v = _parse_grid_kv(raw)
        if k not in _SWEEP_TYPES:
            allowed = ", ".join(sorted(_SWEEP_TYPES.keys()))
            raise ValueError(f"Unsupported sweep param {k!r}. Allowed: {allowed}")
        cast = _SWEEP_TYPES[k]
        values: list[object] = []
        for part in v.split(","):
            p = part.strip()
            if not p:
                continue
            try:
                values.append(cast(p))
            except Exception as e:
                raise ValueError(f"Bad value {p!r} for {k!r} (expected {cast.__name__})") from e
        if not values:
            raise ValueError(f"No values parsed for grid key {k!r}")
        grid[k] = values
    return grid


def _grid_size(grid: dict[str, list[object]]) -> int:
    size = 1
    for v in grid.values():
        size *= len(v)
    return size


def _sampled_param_sets(
    *,
    keys: list[str],
    values: list[list[object]],
    sample: int,
    seed: int,
) -> list[dict[str, object]]:
    if sample <= 0:
        return []
    rnd = random.Random(seed)

    # Sample unique index tuples to avoid duplicates (best-effort).
    dims = [len(v) for v in values]
    seen: set[tuple[int, ...]] = set()
    out: list[dict[str, object]] = []

    max_tries = sample * 50
    tries = 0
    while len(out) < sample and tries < max_tries:
        idxs = tuple(rnd.randrange(d) for d in dims)
        tries += 1
        if idxs in seen:
            continue
        seen.add(idxs)
        params = {keys[i]: values[i][idxs[i]] for i in range(len(keys))}
        out.append(params)
    return out


async def _load_universe_symbols(
    *,
    session,
    quote_ccy: str,
    max_symbols: Optional[int],
) -> tuple[list[str], Optional[str]]:
    rows = (
        await session.execute(
            select(UniverseMembership.symbol, UniverseMembership.is_baseline)
            .where(UniverseMembership.quote_ccy == quote_ccy, UniverseMembership.is_active.is_(True))
            .order_by(UniverseMembership.liquidity_rank.asc())
        )
    ).all()
    symbols = [str(r[0]) for r in rows]
    baseline = next((str(r[0]) for r in rows if bool(r[1])), None)
    if max_symbols is not None:
        symbols = symbols[: max(0, int(max_symbols))]
        if baseline is not None and baseline not in symbols:
            symbols = [baseline] + symbols
    return symbols, baseline


async def _symbol_min_max(
    session,
    *,
    symbol: str,
    candles_model,
    timeframe_min: Optional[int],
) -> tuple[Optional[int], Optional[int]]:
    stmt = select(func.min(candles_model.t), func.max(candles_model.t)).where(candles_model.symbol == symbol)
    if candles_model is CandleHistBar:
        if timeframe_min is None:
            raise ValueError("timeframe_min is required for CandleHistBar")
        stmt = stmt.where(CandleHistBar.timeframe_min == int(timeframe_min))
    row = (await session.execute(stmt)).one()
    min_t, max_t = row
    return (int(min_t) if min_t is not None else None, int(max_t) if max_t is not None else None)


async def _symbol_min_in_range(
    session,
    *,
    symbol: str,
    start_t0: int,
    end_t0: int,
    candles_model,
    timeframe_min: Optional[int],
) -> Optional[int]:
    stmt = select(func.min(candles_model.t)).where(
        candles_model.symbol == symbol,
        candles_model.t >= int(start_t0),
        candles_model.t <= int(end_t0),
    )
    if candles_model is CandleHistBar:
        if timeframe_min is None:
            raise ValueError("timeframe_min is required for CandleHistBar")
        stmt = stmt.where(CandleHistBar.timeframe_min == int(timeframe_min))

    row = (await session.execute(stmt)).scalar_one_or_none()
    return int(row) if row is not None else None


async def _build_window_data(
    *,
    session_factory,
    base_settings: Settings,
    candidate_symbols: list[str],
    baseline_symbol: str,
    btc_symbol: str,
    eth_symbol: str,
    days: int,
    warmup_min: int,
    chunk_minutes: int,
    bar_minutes: int,
) -> _WindowData:
    if days < 1:
        raise ValueError("days must be >= 1")
    if warmup_min < 0:
        raise ValueError("warmup_min must be >= 0")
    if chunk_minutes < 60:
        raise ValueError("chunk_minutes must be >= 60")
    if int(bar_minutes) < 1:
        raise ValueError("bar_minutes must be >= 1")

    candles_model = Candle1mHist
    timeframe_min: Optional[int] = None
    candle_step_ms = MINUTE_MS
    if int(bar_minutes) > 1:
        candles_model = CandleHistBar
        timeframe_min = int(bar_minutes)
        candle_step_ms = int(bar_minutes) * MINUTE_MS
        if int(warmup_min) % int(bar_minutes) != 0:
            raise ValueError("warmup_min must be a multiple of bar_minutes")
        if int(chunk_minutes) % int(bar_minutes) != 0:
            raise ValueError("chunk_minutes must be a multiple of bar_minutes")

    desired_end_t0 = floor_minute_ms(now_ms()) - MINUTE_MS
    if int(bar_minutes) > 1:
        desired_end_t0 = (int(desired_end_t0) // int(candle_step_ms)) * int(candle_step_ms)

    async with session_scope(session_factory) as session:
        if int(bar_minutes) > 1 and timeframe_min is not None:
            await _assert_hist_bars_close_timestamps(session=session, timeframe_min=int(timeframe_min), symbol=baseline_symbol)

        btc_min, btc_max = await _symbol_min_max(
            session,
            symbol=btc_symbol,
            candles_model=candles_model,
            timeframe_min=timeframe_min,
        )
        eth_min, eth_max = await _symbol_min_max(
            session,
            symbol=eth_symbol,
            candles_model=candles_model,
            timeframe_min=timeframe_min,
        )
        if btc_min is None or btc_max is None or eth_min is None or eth_max is None:
            raise RuntimeError("Missing BTC/ETH historical candles for requested candle source")

        end_t0 = min(int(desired_end_t0), int(btc_max), int(eth_max))
        desired_start_t0 = end_t0 - (int(days) * DAY_MS)

        btc_first = await _symbol_min_in_range(
            session,
            symbol=btc_symbol,
            start_t0=desired_start_t0,
            end_t0=end_t0,
            candles_model=candles_model,
            timeframe_min=timeframe_min,
        )
        eth_first = await _symbol_min_in_range(
            session,
            symbol=eth_symbol,
            start_t0=desired_start_t0,
            end_t0=end_t0,
            candles_model=candles_model,
            timeframe_min=timeframe_min,
        )
        if btc_first is None or eth_first is None:
            raise RuntimeError("Not enough BTC/ETH candles to cover requested window")

        start_t0 = max(int(desired_start_t0), int(btc_first), int(eth_first))
        ingest_start_t0 = start_t0 - (int(warmup_min) * MINUTE_MS)
        if int(bar_minutes) > 1:
            ingest_start_t0 = (int(ingest_start_t0) // int(candle_step_ms)) * int(candle_step_ms)

    # Also ingest baseline + BTC + ETH raw prices/highs (for evaluation).
    required_symbols_set = set(candidate_symbols) | {baseline_symbol, btc_symbol, eth_symbol}
    required_symbols: list[str] = sorted(required_symbols_set)
    slot_by_symbol = {s: i for i, s in enumerate(required_symbols)}

    n_times = int((end_t0 - ingest_start_t0) / int(candle_step_ms)) + 1

    # Safety: this is intended for tuning windows (e.g. 30–90 days). 365d grids can be enormous.
    bytes_per_float = 8
    n_all = len(required_symbols)
    n_cand = len(candidate_symbols)
    approx_bytes = 0
    # raw close + high for all symbols
    approx_bytes += n_all * 2 * n_times * bytes_per_float
    # dvz + avg_dv + extension + score for candidates
    approx_bytes += n_cand * 4 * n_times * bytes_per_float
    # trend_ok for candidates (1 byte-ish)
    approx_bytes += n_cand * n_times
    # order indices (u16)
    approx_bytes += n_times * n_cand * 2
    approx_mb = approx_bytes / (1024 * 1024)
    logger.info(
        "window_plan",
        extra={
            "days": days,
            "symbols_all": n_all,
            "symbols_candidates": n_cand,
            "minutes": n_times,
            "approx_mb": round(approx_mb, 1),
            "start_t0": start_t0,
            "end_t0": end_t0,
        },
    )

    if approx_mb > 750:
        raise RuntimeError(
            f"Requested window is too large for in-memory sweep precompute (~{approx_mb:.1f}MB). "
            "Reduce --days and/or --max-symbols."
        )

    nan = float("nan")

    raw_close_by_slot: list[array] = [array("d", [nan]) * n_times for _ in range(n_all)]
    raw_high_by_slot: list[array] = [array("d", [nan]) * n_times for _ in range(n_all)]

    cand_slot_by_idx: list[int] = [slot_by_symbol[s] for s in candidate_symbols]
    score_by_idx: list[array] = [array("d", [nan]) * n_times for _ in range(n_cand)]
    rank_rel_r15_by_idx: list[array] = [array("d", [nan]) * n_times for _ in range(n_cand)]
    dvz_by_idx: list[array] = [array("d", [nan]) * n_times for _ in range(n_cand)]
    avg_dv_by_idx: list[array] = [array("d", [nan]) * n_times for _ in range(n_cand)]
    extension_by_idx: list[array] = [array("d", [nan]) * n_times for _ in range(n_cand)]
    trend_ok_by_idx: list[array] = [array("b", [0]) * n_times for _ in range(n_cand)]
    baseline_trend_ok_by_time = array("b", [0]) * n_times

    order_flat = array("H", [SENTINEL_U16]) * (n_times * max(1, n_cand))
    order_len = array("H", [0]) * n_times

    # Rolling feature state for scoring.
    trend_bucket_steps = 1 if int(bar_minutes) > 1 else 5
    feature_state: dict[str, RollingFeatureState] = {
        s: RollingFeatureState(step_ms=int(candle_step_ms), trend_bucket_steps=int(trend_bucket_steps))
        for s in required_symbols_set
    }

    async with session_scope(session_factory) as session:
        chunk_ms = int(chunk_minutes) * MINUTE_MS

        current_t: Optional[int] = None

        def compute_snapshot(t0: int) -> None:
            if t0 < start_t0:
                return
            if t0 < ingest_start_t0 or t0 > end_t0:
                return
            idx = int((t0 - ingest_start_t0) / int(candle_step_ms))
            if idx < 0 or idx >= n_times:
                return

            baseline_state = feature_state.get(baseline_symbol)
            if baseline_state is None or baseline_state.last_t != t0:
                return
            if baseline_state.baseline_usable_reason() is not None:
                return

            baseline_fs, _baseline_reason = baseline_state.compute_features_with_reason(baseline=baseline_state, t0=t0)
            if baseline_fs is None:
                return
            baseline_trend_ok_by_time[idx] = 1 if bool(baseline_fs.trend_ok) else 0

            # Compute features (and store feature values) for each candidate.
            features_by_symbol: dict[str, Any] = {}
            for sym in candidate_symbols:
                st = feature_state.get(sym)
                if st is None:
                    continue
                fs, _reason = st.compute_features_with_reason(baseline=baseline_state, t0=t0)
                if fs is None:
                    continue
                features_by_symbol[sym] = fs

            if not features_by_symbol:
                return

            ranks_rel_r15 = percentile_ranks({sym: fs.rel_r15 for sym, fs in features_by_symbol.items()})
            ranks_accel = percentile_ranks({sym: fs.accel for sym, fs in features_by_symbol.items()})

            scores: list[tuple[float, int]] = []
            for cand_i, sym in enumerate(candidate_symbols):
                fs = features_by_symbol.get(sym)
                if fs is None:
                    continue
                components = ScoreComponents(
                    rank_rel_r15=float(ranks_rel_r15.get(sym, 0.0)),
                    rank_accel=float(ranks_accel.get(sym, 0.0)),
                    dv_term=float(dv_term_from_dvz(fs.dv_z)),
                    breakout=int(fs.breakout),
                )
                score = float(compute_score(components))
                score_by_idx[cand_i][idx] = score
                rank_rel_r15_by_idx[cand_i][idx] = float(components.rank_rel_r15)
                dvz_by_idx[cand_i][idx] = float(fs.dv_z)
                avg_dv_by_idx[cand_i][idx] = float(fs.avg_dv_1m)
                extension_by_idx[cand_i][idx] = float(fs.extension)
                trend_ok_by_idx[cand_i][idx] = 1 if bool(fs.trend_ok) else 0
                scores.append((score, cand_i))

            # Precompute candidate ordering by score (descending).
            scores.sort(key=lambda x: x[0], reverse=True)
            order_len[idx] = min(len(scores), SENTINEL_U16)
            base = idx * max(1, n_cand)
            for j in range(max(1, n_cand)):
                order_flat[base + j] = SENTINEL_U16
            for j, (_score, cand_i) in enumerate(scores[:n_cand]):
                order_flat[base + j] = int(cand_i)

        for chunk_start in range(ingest_start_t0, end_t0 + 1, chunk_ms):
            chunk_end = min(end_t0, chunk_start + chunk_ms - int(candle_step_ms))
            if chunk_end < chunk_start:
                chunk_end = chunk_start

            if candles_model is Candle1mHist:
                stmt = (
                    select(
                        Candle1mHist.symbol,
                        Candle1mHist.t,
                        Candle1mHist.o,
                        Candle1mHist.h,
                        Candle1mHist.l,
                        Candle1mHist.c,
                        Candle1mHist.v,
                    )
                    .where(
                        Candle1mHist.symbol.in_(required_symbols),
                        Candle1mHist.t >= chunk_start,
                        Candle1mHist.t <= chunk_end,
                    )
                    .order_by(Candle1mHist.t.asc(), Candle1mHist.symbol.asc())
                )
            else:
                stmt = (
                    select(
                        CandleHistBar.symbol,
                        CandleHistBar.t,
                        CandleHistBar.o,
                        CandleHistBar.h,
                        CandleHistBar.l,
                        CandleHistBar.c,
                        CandleHistBar.v,
                    )
                    .where(
                        CandleHistBar.timeframe_min == int(timeframe_min),
                        CandleHistBar.symbol.in_(required_symbols),
                        CandleHistBar.t >= chunk_start,
                        CandleHistBar.t <= chunk_end,
                    )
                    .order_by(CandleHistBar.t.asc(), CandleHistBar.symbol.asc())
                )
            rows = (await session.execute(stmt)).all()
            if not rows:
                continue

            for symbol, t, o, h, l, c, v in rows:
                t_i = int(t)
                if current_t is None:
                    current_t = t_i
                if t_i != current_t:
                    compute_snapshot(current_t)
                    current_t = t_i

                sym = str(symbol)
                st = feature_state.get(sym)
                if st is None:
                    continue
                slot = slot_by_symbol.get(sym)
                if slot is not None:
                    idx = int((t_i - ingest_start_t0) / int(candle_step_ms))
                    if 0 <= idx < n_times:
                        raw_close_by_slot[slot][idx] = float(c)
                        raw_high_by_slot[slot][idx] = float(h)

                cp = CandlePoint(t=t_i, o=float(o), h=float(h), l=float(l), c=float(c), v=float(v))
                st.ingest(cp)

            if current_t is not None:
                compute_snapshot(current_t)

            logger.info(
                "precompute_chunk_done",
                extra={"chunk_start": chunk_start, "chunk_end": chunk_end},
            )

    return _WindowData(
        step_ms=int(candle_step_ms),
        ingest_start_t0=ingest_start_t0,
        start_t0=start_t0,
        end_t0=end_t0,
        candidate_symbols=candidate_symbols,
        baseline_symbol=baseline_symbol,
        btc_symbol=btc_symbol,
        eth_symbol=eth_symbol,
        all_symbols=required_symbols,
        raw_close_by_slot=raw_close_by_slot,
        raw_high_by_slot=raw_high_by_slot,
        cand_slot_by_idx=cand_slot_by_idx,
        score_by_idx=score_by_idx,
        rank_rel_r15_by_idx=rank_rel_r15_by_idx,
        dvz_by_idx=dvz_by_idx,
        avg_dv_by_idx=avg_dv_by_idx,
        extension_by_idx=extension_by_idx,
        trend_ok_by_idx=trend_ok_by_idx,
        baseline_trend_ok_by_time=baseline_trend_ok_by_time,
        order_flat=order_flat,
        order_len=order_len,
    )


def _simulate_alerts(window: _WindowData, *, settings: Settings) -> tuple[list[AlertEvent], dict[str, list[int]]]:
    budget_window_ms = int(settings.alert_lookback_hours * 3600 * 1000)
    exit_entry_lookback_ms = int(settings.exit_entry_lookback_hours * 3600 * 1000)

    # OUT/IN per symbol.
    class _Pos:
        __slots__ = (
            "state",
            "last_state_change_ts",
            "last_entry_alert_ts",
            "last_exit_alert_ts",
            "peak_price_since_entry",
            "peak_ts_since_entry",
            "peak_high_since_entry",
            "peak_high_ts_since_entry",
        )

        def __init__(self, now: int) -> None:
            self.state = "OUT"
            self.last_state_change_ts = now
            self.last_entry_alert_ts: Optional[int] = None
            self.last_exit_alert_ts: Optional[int] = None
            self.peak_price_since_entry: Optional[float] = None
            self.peak_ts_since_entry: Optional[int] = None
            self.peak_high_since_entry: Optional[float] = None
            self.peak_high_ts_since_entry: Optional[int] = None

    start_idx = window.idx_for_t0(window.start_t0)
    end_idx = window.idx_for_t0(window.end_t0)

    positions: list[_Pos] = [_Pos(window.start_t0) for _ in window.candidate_symbols]
    entries_out: list[AlertEvent] = []
    exits_by_symbol: dict[str, list[int]] = {s: [] for s in window.candidate_symbols}

    entry_times: list[int] = []
    exit_times: list[int] = []
    total_times: list[int] = []
    last_global_entry_ts: Optional[int] = None

    for idx in range(start_idx, end_idx + 1):
        t0 = window.t0_at(idx)

        # Rolling budgets.
        window_start = t0 - budget_window_ms
        entry_count = _expire(entry_times, cutoff=window_start)
        exit_count = _expire(exit_times, cutoff=window_start)
        total_count = _expire(total_times, cutoff=window_start)

        # EXITS first.
        for cand_i, sym in enumerate(window.candidate_symbols):
            st = positions[cand_i]
            if st.state != "IN":
                continue

            score = float(window.score_by_idx[cand_i][idx])
            if math.isnan(score):
                continue

            dv_z = float(window.dvz_by_idx[cand_i][idx])
            if math.isnan(dv_z):
                continue

            trend_ok = bool(int(window.trend_ok_by_idx[cand_i][idx]))
            slot = window.cand_slot_by_idx[cand_i]
            price = window.raw_close_by_slot[slot][idx]
            high = window.raw_high_by_slot[slot][idx]
            if math.isnan(price) or math.isnan(high):
                continue

            # Update peaks.
            if st.peak_price_since_entry is None or float(price) > float(st.peak_price_since_entry):
                st.peak_price_since_entry = float(price)
                st.peak_ts_since_entry = int(t0)
            if st.peak_high_since_entry is None or float(high) > float(st.peak_high_since_entry):
                st.peak_high_since_entry = float(high)
                st.peak_high_ts_since_entry = int(t0)

            exit_reason = determine_exit_reason(
                score=float(score),
                trend_ok=bool(trend_ok),
                dv_z=float(dv_z),
                t0=int(t0),
                price=float(price),
                peak_price=st.peak_price_since_entry,
                peak_high=st.peak_high_since_entry,
                peak_ts=st.peak_ts_since_entry,
                settings=settings,
            )
            if exit_reason is None:
                continue

            should_alert = True
            if st.last_entry_alert_ts is None or (t0 - int(st.last_entry_alert_ts)) > exit_entry_lookback_ms:
                should_alert = False
            if st.last_exit_alert_ts is not None and (t0 - int(st.last_exit_alert_ts)) < _ms(
                settings.symbol_exit_cooldown_min
            ):
                should_alert = False
            if exit_count >= settings.max_exit_alerts_24h or total_count >= settings.max_total_alerts_24h:
                should_alert = False

            if should_alert:
                exits_by_symbol[sym].append(int(t0))
                exit_times.append(int(t0))
                total_times.append(int(t0))
                exit_count += 1
                total_count += 1
                st.last_exit_alert_ts = int(t0)

            # Always transition OUT on exit conditions.
            st.state = "OUT"
            st.last_state_change_ts = int(t0)
            st.peak_price_since_entry = None
            st.peak_ts_since_entry = None
            st.peak_high_since_entry = None
            st.peak_high_ts_since_entry = None

        # ENTRIES.
        if entry_count >= settings.max_entry_alerts_24h or total_count >= settings.max_total_alerts_24h:
            continue

        global_entry_ok = last_global_entry_ts is None or (t0 - int(last_global_entry_ts)) >= _ms(
            settings.global_entry_cooldown_min
        )
        if not global_entry_ok:
            continue
        if settings.require_btc_trend_ok_for_entries:
            if not bool(int(window.baseline_trend_ok_by_time[idx])):
                continue

        entries_fired = 0
        base = idx * max(1, window.n_candidates)
        n_order = int(window.order_len[idx])
        for j in range(min(n_order, window.n_candidates)):
            if entry_count >= settings.max_entry_alerts_24h or total_count >= settings.max_total_alerts_24h:
                break
            if entries_fired >= settings.max_entry_alerts_per_scan:
                break

            cand_i = int(window.order_flat[base + j])
            if cand_i == SENTINEL_U16:
                break

            score = float(window.score_by_idx[cand_i][idx])
            if math.isnan(score):
                continue
            if score < settings.entry_score_threshold:
                break

            st = positions[cand_i]
            if st.state != "OUT":
                continue
            if st.last_entry_alert_ts is not None and (t0 - int(st.last_entry_alert_ts)) < _ms(
                settings.symbol_entry_cooldown_min
            ):
                continue

            if settings.min_rank_rel_r15 > 0.0:
                rank_rel_r15 = float(window.rank_rel_r15_by_idx[cand_i][idx])
                if math.isnan(rank_rel_r15) or rank_rel_r15 < float(settings.min_rank_rel_r15):
                    continue

            dv_z = float(window.dvz_by_idx[cand_i][idx])
            avg_dv = float(window.avg_dv_by_idx[cand_i][idx])
            ext = float(window.extension_by_idx[cand_i][idx])
            trend_ok = bool(int(window.trend_ok_by_idx[cand_i][idx]))
            if any(math.isnan(x) for x in [dv_z, avg_dv, ext]):
                continue

            if not trend_ok:
                continue
            if dv_z < settings.dvz_min:
                continue
            if ext > settings.extension_max:
                continue
            if avg_dv < settings.min_dv_1m_usd:
                continue

            slot = window.cand_slot_by_idx[cand_i]
            price = float(window.raw_close_by_slot[slot][idx])
            high = float(window.raw_high_by_slot[slot][idx])
            if math.isnan(price) or math.isnan(high):
                continue

            entries_out.append(
                AlertEvent(
                    symbol=window.candidate_symbols[cand_i],
                    t0=int(t0),
                    price=float(price),
                    score=float(score),
                    alert_type="MOMENTUM_UP",
                )
            )
            entry_times.append(int(t0))
            total_times.append(int(t0))
            entry_count += 1
            total_count += 1

            st.state = "IN"
            st.last_state_change_ts = int(t0)
            st.last_entry_alert_ts = int(t0)
            st.peak_price_since_entry = float(price)
            st.peak_ts_since_entry = int(t0)
            st.peak_high_since_entry = float(high)
            st.peak_high_ts_since_entry = int(t0)

            last_global_entry_ts = int(t0)
            entries_fired += 1
            if settings.global_entry_cooldown_min > 0:
                break

    # Sort exit lists (already chronological, but keep explicit).
    for sym in exits_by_symbol.keys():
        exits_by_symbol[sym].sort()

    entries_out.sort(key=lambda e: e.t0)
    return entries_out, exits_by_symbol


def _evaluate_one_position(
    *,
    window: _WindowData,
    entries: list[AlertEvent],
    exits_by_symbol: dict[str, list[int]],
    initial_usd: float,
    fee_bps: float,
    slippage_bps: float,
    do_cost_per_month: float,
    max_hold_min: int,
    include_btc_eth_trades: bool,
    trade_mode: str,
    sleeve_usdt: float,
) -> tuple[SweepResult, list[TradeRecord]]:
    if initial_usd <= 0.0:
        raise ValueError("initial_usd must be > 0")
    if max_hold_min < 1:
        raise ValueError("max_hold_min must be >= 1")
    if trade_mode not in ("rebalance", "usdt-sleeve"):
        raise ValueError("trade_mode must be one of: rebalance, usdt-sleeve")
    if trade_mode == "usdt-sleeve":
        if sleeve_usdt < 0.0:
            raise ValueError("sleeve_usdt must be >= 0")
        if sleeve_usdt > initial_usd:
            raise ValueError("sleeve_usdt must be <= initial_usd")

    start_idx = window.idx_for_t0(window.start_t0)
    end_idx = window.idx_for_t0(window.end_t0)

    btc_slot = window.slot_of(window.btc_symbol)
    eth_slot = window.slot_of(window.eth_symbol)
    if btc_slot is None or eth_slot is None:
        raise RuntimeError("BTC/ETH symbols missing from precompute window")

    btc_px_start = window.close_at_or_before(slot=btc_slot, time_idx=start_idx)
    eth_px_start = window.close_at_or_before(slot=eth_slot, time_idx=start_idx)
    btc_px_end = window.close_at_or_before(slot=btc_slot, time_idx=end_idx)
    eth_px_end = window.close_at_or_before(slot=eth_slot, time_idx=end_idx)
    if btc_px_start is None or eth_px_start is None or btc_px_end is None or eth_px_end is None:
        raise RuntimeError("Missing BTC/ETH prices in window")

    baseline_usdt = 0.0
    baseline_core_usd = float(initial_usd)
    if trade_mode == "usdt-sleeve":
        baseline_usdt = float(sleeve_usdt)
        baseline_core_usd = float(initial_usd) - float(sleeve_usdt)

    # Baseline: hold 50/50 BTC+ETH core + (optional) USDT sleeve.
    baseline_btc_qty = (baseline_core_usd * 0.5) / btc_px_start if baseline_core_usd > 0.0 else 0.0
    baseline_eth_qty = (baseline_core_usd * 0.5) / eth_px_start if baseline_core_usd > 0.0 else 0.0
    baseline_equity_end = baseline_usdt + (baseline_btc_qty * btc_px_end) + (baseline_eth_qty * eth_px_end)
    baseline_return = (baseline_equity_end / initial_usd) - 1.0

    excluded_syms = set()
    if not include_btc_eth_trades:
        excluded_syms.update([window.btc_symbol, window.eth_symbol])

    cost = CostModel(fee_bps=fee_bps, slippage_bps=slippage_bps)
    max_hold_ms = int(max_hold_min) * MINUTE_MS

    active_btc_qty = baseline_btc_qty
    active_eth_qty = baseline_eth_qty
    active_sleeve_usdt = float(baseline_usdt) if trade_mode == "usdt-sleeve" else 0.0

    total_fee_paid = 0.0
    total_slippage_paid = 0.0

    trades: list[TradeRecord] = []

    current_position_end_t0: Optional[int] = None

    for entry in entries:
        if entry.symbol in excluded_syms:
            continue
        if entry.t0 < window.start_t0 or entry.t0 > window.end_t0:
            continue

        if current_position_end_t0 is not None and entry.t0 < current_position_end_t0:
            continue

        exit_list = exits_by_symbol.get(entry.symbol) or []

        exit_limit_t0 = min(entry.t0 + max_hold_ms, window.end_t0)
        idx = 0
        while idx < len(exit_list) and exit_list[idx] <= entry.t0:
            idx += 1

        if idx < len(exit_list) and exit_list[idx] <= exit_limit_t0:
            exit_t0 = int(exit_list[idx])
            exit_reason = "exit_alert"
        else:
            exit_t0 = int(exit_limit_t0)
            exit_reason = "time_stop" if exit_limit_t0 < window.end_t0 else "end_of_window"

        entry_idx = window.idx_for_t0(entry.t0)
        exit_idx = window.idx_for_t0(exit_t0)

        sym_slot = window.slot_of(entry.symbol)
        if sym_slot is None:
            continue

        entry_px = entry.price
        exit_px = window.close_at_or_before(slot=sym_slot, time_idx=exit_idx)
        if exit_px is None or entry_px <= 0.0:
            continue

        btc_px_entry = window.close_at_or_before(slot=btc_slot, time_idx=entry_idx)
        eth_px_entry = window.close_at_or_before(slot=eth_slot, time_idx=entry_idx)
        btc_px_exit = window.close_at_or_before(slot=btc_slot, time_idx=exit_idx)
        eth_px_exit = window.close_at_or_before(slot=eth_slot, time_idx=exit_idx)
        if btc_px_entry is None or eth_px_entry is None or btc_px_exit is None or eth_px_exit is None:
            continue

        equity_before = (active_btc_qty * btc_px_entry) + (active_eth_qty * eth_px_entry) + float(active_sleeve_usdt)

        if trade_mode == "rebalance":
            # Entry: sell BTC + ETH -> USDT, then buy ALT.
            usdt_from_btc, cost_btc_sell = sell_with_costs(active_btc_qty, price=btc_px_entry, cost=cost)
            usdt_from_eth, cost_eth_sell = sell_with_costs(active_eth_qty, price=eth_px_entry, cost=cost)
            total_fee_paid += cost_btc_sell.fee_paid + cost_eth_sell.fee_paid
            total_slippage_paid += cost_btc_sell.slippage_paid + cost_eth_sell.slippage_paid

            active_btc_qty = 0.0
            active_eth_qty = 0.0
            usdt_total = usdt_from_btc + usdt_from_eth

            alt_qty, cost_alt_buy = buy_with_costs(usdt_total, price=entry_px, cost=cost)
            total_fee_paid += cost_alt_buy.fee_paid
            total_slippage_paid += cost_alt_buy.slippage_paid

            # Exit: sell ALT -> USDT, then buy BTC + ETH (rebalance 50/50).
            usdt_after_sell, cost_alt_sell = sell_with_costs(alt_qty, price=exit_px, cost=cost)
            total_fee_paid += cost_alt_sell.fee_paid
            total_slippage_paid += cost_alt_sell.slippage_paid

            usdt_half = usdt_after_sell * 0.5
            new_btc_qty, cost_btc_buy = buy_with_costs(usdt_half, price=btc_px_exit, cost=cost)
            new_eth_qty, cost_eth_buy = buy_with_costs(usdt_half, price=eth_px_exit, cost=cost)
            total_fee_paid += cost_btc_buy.fee_paid + cost_eth_buy.fee_paid
            total_slippage_paid += cost_btc_buy.slippage_paid + cost_eth_buy.slippage_paid

            active_btc_qty = new_btc_qty
            active_eth_qty = new_eth_qty

            equity_after = (active_btc_qty * btc_px_exit) + (active_eth_qty * eth_px_exit)
            net_ret = (equity_after / equity_before) - 1.0 if equity_before > 0.0 else 0.0
        else:
            # USDT sleeve mode: buy/sell only with the sleeve; core BTC/ETH stays held.
            if active_sleeve_usdt <= 0.0:
                continue

            alt_qty, cost_alt_buy = buy_with_costs(active_sleeve_usdt, price=entry_px, cost=cost)
            total_fee_paid += cost_alt_buy.fee_paid
            total_slippage_paid += cost_alt_buy.slippage_paid
            active_sleeve_usdt = 0.0

            usdt_after_sell, cost_alt_sell = sell_with_costs(alt_qty, price=exit_px, cost=cost)
            total_fee_paid += cost_alt_sell.fee_paid
            total_slippage_paid += cost_alt_sell.slippage_paid
            active_sleeve_usdt = float(usdt_after_sell)

            equity_after = (active_btc_qty * btc_px_exit) + (active_eth_qty * eth_px_exit) + float(active_sleeve_usdt)
            net_ret = (equity_after / equity_before) - 1.0 if equity_before > 0.0 else 0.0

        duration_min = int((exit_t0 - entry.t0) / MINUTE_MS)
        gross_ret = (exit_px / entry_px) - 1.0

        trades.append(
            TradeRecord(
                symbol=entry.symbol,
                entry_t0=int(entry.t0),
                exit_t0=int(exit_t0),
                exit_reason=exit_reason,
                entry_price=float(entry_px),
                exit_price=float(exit_px),
                duration_min=duration_min,
                gross_ret=float(gross_ret),
                net_ret=float(net_ret),
            )
        )
        current_position_end_t0 = int(exit_t0)

    active_equity_end = (active_btc_qty * btc_px_end) + (active_eth_qty * eth_px_end)
    if trade_mode == "usdt-sleeve":
        active_equity_end += float(active_sleeve_usdt)
    active_return = (active_equity_end / initial_usd) - 1.0

    window_days = (window.end_t0 - window.start_t0) / float(DAY_MS)
    do_cost = float(do_cost_per_month) * (window_days / 30.0)
    active_equity_after_do = active_equity_end - do_cost
    active_return_after_do = (active_equity_after_do / initial_usd) - 1.0

    trade_net_rets = [t.net_ret for t in trades]
    win_rate = (sum(1 for r in trade_net_rets if r > 0.0) / len(trade_net_rets)) if trade_net_rets else 0.0

    # Placeholder config_id; caller fills.
    dummy = SweepResult(
        config_id="",
        params={},
        entry_alerts=len(entries),
        exit_alerts=sum(len(v) for v in exits_by_symbol.values()),
        trades=len(trades),
        final_equity=float(active_equity_end),
        final_return=float(active_return),
        final_equity_after_do=float(active_equity_after_do),
        final_return_after_do=float(active_return_after_do),
        baseline_equity=float(baseline_equity_end),
        baseline_return=float(baseline_return),
        equity_delta=float(active_equity_end - baseline_equity_end),
        equity_delta_after_do=float(active_equity_after_do - baseline_equity_end),
        total_fee_paid=float(total_fee_paid),
        total_slippage_paid=float(total_slippage_paid),
        win_rate_net=float(win_rate),
    )
    return dummy, trades


async def _main_async(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="scripts/parameter_sweep.py",
        description="Offline grid search over signal thresholds using candles_1m_hist / candles_hist_bars (replay mode).",
    )
    parser.add_argument("--days", type=int, default=30, help="Lookback window in days (default: 30)")
    parser.add_argument("--warmup-min", type=int, default=120, help="Warmup minutes before window start (default: 120)")
    parser.add_argument("--chunk-minutes", type=int, default=1440, help="DB fetch chunk size in minutes (default: 1440)")
    parser.add_argument(
        "--bar-minutes",
        type=int,
        default=1,
        help=(
            "Use aggregated historical bars from candles_hist_bars instead of 1m candles. "
            "Example: --bar-minutes 60 (requires running scripts/build_hist_bars.py first)."
        ),
    )
    parser.add_argument("--baseline-symbol", type=str, default="BTC_USDT", help="Baseline symbol (default: BTC_USDT)")
    parser.add_argument("--btc-symbol", type=str, default="BTC_USDT", help="BTC symbol for hold baseline (default: BTC_USDT)")
    parser.add_argument("--eth-symbol", type=str, default="ETH_USDT", help="ETH symbol for hold baseline (default: ETH_USDT)")
    parser.add_argument("--from-universe", action="store_true", help="Use current universe_membership table")
    parser.add_argument("--max-symbols", type=int, default=50, help="Max symbols when using --from-universe (default: 50)")
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated symbols (if not using --from-universe)")
    parser.add_argument("--initial-usd", type=float, default=500.0, help="Initial USD for eval (default: 500)")
    parser.add_argument("--fee-bps", type=float, default=10.0, help="Fee per side in bps (default: 10)")
    parser.add_argument("--slippage-bps", type=float, default=5.0, help="Slippage per side in bps (default: 5)")
    parser.add_argument("--do-cost-per-month", type=float, default=15.0, help="DO cost/mo in USD (default: 15)")
    parser.add_argument("--max-hold-min", type=int, default=360, help="Time stop if no exit alert (default: 360)")
    parser.add_argument(
        "--trade-mode",
        type=str,
        choices=["rebalance", "usdt-sleeve"],
        default="rebalance",
        help=(
            "Trading simulation mode: "
            "rebalance=sell BTC+ETH into ALT and rebalance back (6 legs), "
            "usdt-sleeve=keep BTC/ETH hold and trade a USDT sleeve only (2 legs)."
        ),
    )
    parser.add_argument(
        "--sleeve-usdt",
        type=float,
        default=200.0,
        help="USDT sleeve size (USD notionals) for --trade-mode usdt-sleeve (default: 200)",
    )
    parser.add_argument(
        "--include-btc-eth-trades",
        action="store_true",
        help="Allow BTC/ETH trades (default: excluded)",
    )
    parser.add_argument(
        "--grid",
        action="append",
        default=[],
        help="Grid entry like key=v1,v2,... (repeatable). If omitted, uses a safe default grid.",
    )
    parser.add_argument("--sample", type=int, default=None, help="Randomly sample N configs from the grid")
    parser.add_argument("--seed", type=int, default=1, help="RNG seed for --sample (default: 1)")
    parser.add_argument("--top", type=int, default=10, help="Print top N configs (default: 10)")
    parser.add_argument("--out-csv", type=str, default="sweep_results.csv", help="Output CSV path (default: sweep_results.csv)")
    parser.add_argument(
        "--yes-large-grid",
        action="store_true",
        help="Allow running large grids (>500 configs).",
    )
    args = parser.parse_args(argv)

    if args.days < 1:
        raise SystemExit("--days must be >= 1")
    if args.warmup_min < 0:
        raise SystemExit("--warmup-min must be >= 0")
    if args.chunk_minutes < 60:
        raise SystemExit("--chunk-minutes must be >= 60")
    if args.bar_minutes < 1:
        raise SystemExit("--bar-minutes must be >= 1")
    if int(args.bar_minutes) > 1 and int(args.warmup_min) == 120:
        # Convenience default for bar sweeps: ensure enough history for dv/vwap (60 bars).
        args.warmup_min = 60 * int(args.bar_minutes)
    if args.initial_usd <= 0:
        raise SystemExit("--initial-usd must be > 0")
    if args.max_hold_min < 1:
        raise SystemExit("--max-hold-min must be >= 1")
    if args.trade_mode == "usdt-sleeve":
        if args.sleeve_usdt < 0:
            raise SystemExit("--sleeve-usdt must be >= 0")
        if args.sleeve_usdt > args.initial_usd:
            raise SystemExit("--sleeve-usdt must be <= --initial-usd")

    base_settings = get_settings()
    # Offline sweep cannot use historical order books; disable spread gating.
    base_settings = base_settings.model_copy(update={"book_check_top_n": 0})

    engine = create_engine(base_settings)
    session_factory = create_sessionmaker(engine)

    try:
        # 1) Choose symbols (static universe).
        async with session_scope(session_factory) as session:
            if args.from_universe:
                symbols, baseline_from_db = await _load_universe_symbols(
                    session=session,
                    quote_ccy=base_settings.quote_ccy,
                    max_symbols=args.max_symbols,
                )
                baseline_symbol = baseline_from_db or args.baseline_symbol
            else:
                symbols = _parse_symbols_csv(args.symbols)
                baseline_symbol = args.baseline_symbol

        if not baseline_symbol:
            raise SystemExit("Baseline symbol not set; use --baseline-symbol or set it in universe_membership.")
        if baseline_symbol not in symbols:
            symbols = [baseline_symbol] + symbols

        candidate_symbols = [s for s in symbols if s != baseline_symbol]
        if not candidate_symbols:
            raise SystemExit("No candidate symbols (universe is empty after excluding baseline).")

        # 2) Grid configs.
        try:
            grid = _parse_grid_args(args.grid)
        except ValueError as e:
            raise SystemExit(str(e)) from e

        keys = list(grid.keys())
        values = [grid[k] for k in keys]
        grid_count = _grid_size(grid)
        if grid_count > 500 and not args.yes_large_grid and args.sample is None:
            raise SystemExit(
                f"Grid has {grid_count} configs. Re-run with --yes-large-grid, or use --sample N to sample."
            )

        if args.sample is not None:
            param_sets = _sampled_param_sets(keys=keys, values=values, sample=min(args.sample, grid_count), seed=args.seed)
        else:
            param_sets = [{keys[i]: combo[i] for i in range(len(keys))} for combo in itertools.product(*values)]

        logger.info(
            "sweep_plan",
            extra={
                "days": args.days,
                "candidates": len(candidate_symbols),
                "baseline": baseline_symbol,
                "grid_count": grid_count,
                "run_count": len(param_sets),
                "grid": grid,
            },
        )

        # 3) Precompute window once.
        window = await _build_window_data(
            session_factory=session_factory,
            base_settings=base_settings,
            candidate_symbols=candidate_symbols,
            baseline_symbol=baseline_symbol,
            btc_symbol=args.btc_symbol,
            eth_symbol=args.eth_symbol,
            days=args.days,
            warmup_min=args.warmup_min,
            chunk_minutes=args.chunk_minutes,
            bar_minutes=args.bar_minutes,
        )

        # 4) Sweep.
        results: list[SweepResult] = []

        for i, params in enumerate(param_sets):
            config_id = f"cfg{i:04d}"
            settings = base_settings.model_copy(update={**params})

            entries, exits_by_symbol = _simulate_alerts(window, settings=settings)
            dummy, trades = _evaluate_one_position(
                window=window,
                entries=entries,
                exits_by_symbol=exits_by_symbol,
                initial_usd=float(args.initial_usd),
                fee_bps=float(args.fee_bps),
                slippage_bps=float(args.slippage_bps),
                do_cost_per_month=float(args.do_cost_per_month),
                max_hold_min=int(args.max_hold_min),
                include_btc_eth_trades=bool(args.include_btc_eth_trades),
                trade_mode=str(args.trade_mode),
                sleeve_usdt=float(args.sleeve_usdt),
            )

            results.append(
                SweepResult(
                    config_id=config_id,
                    params=dict(params),
                    entry_alerts=dummy.entry_alerts,
                    exit_alerts=dummy.exit_alerts,
                    trades=dummy.trades,
                    final_equity=dummy.final_equity,
                    final_return=dummy.final_return,
                    final_equity_after_do=dummy.final_equity_after_do,
                    final_return_after_do=dummy.final_return_after_do,
                    baseline_equity=dummy.baseline_equity,
                    baseline_return=dummy.baseline_return,
                    equity_delta=dummy.equity_delta,
                    equity_delta_after_do=dummy.equity_delta_after_do,
                    total_fee_paid=dummy.total_fee_paid,
                    total_slippage_paid=dummy.total_slippage_paid,
                    win_rate_net=dummy.win_rate_net,
                )
            )

            if (i + 1) % 10 == 0 or i == len(param_sets) - 1:
                logger.info("sweep_progress", extra={"done": i + 1, "total": len(param_sets)})

        # Sort by after-DO equity (optimize for "covers infra + outperforms hold").
        results.sort(key=lambda r: r.final_equity_after_do, reverse=True)

        out_path = Path(args.out_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="") as f:
            fieldnames = [
                "config_id",
                "trade_mode",
                "sleeve_usdt",
                *keys,
                "entry_alerts",
                "exit_alerts",
                "trades",
                "final_equity",
                "final_return",
                "final_equity_after_do",
                "final_return_after_do",
                "baseline_equity",
                "baseline_return",
                "equity_delta",
                "equity_delta_after_do",
                "total_fee_paid",
                "total_slippage_paid",
                "win_rate_net",
            ]
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in results:
                row: dict[str, Any] = {
                    "config_id": r.config_id,
                    "trade_mode": str(args.trade_mode),
                    "sleeve_usdt": float(args.sleeve_usdt),
                    "entry_alerts": r.entry_alerts,
                    "exit_alerts": r.exit_alerts,
                    "trades": r.trades,
                    "final_equity": f"{r.final_equity:.6f}",
                    "final_return": f"{r.final_return:.6f}",
                    "final_equity_after_do": f"{r.final_equity_after_do:.6f}",
                    "final_return_after_do": f"{r.final_return_after_do:.6f}",
                    "baseline_equity": f"{r.baseline_equity:.6f}",
                    "baseline_return": f"{r.baseline_return:.6f}",
                    "equity_delta": f"{r.equity_delta:.6f}",
                    "equity_delta_after_do": f"{r.equity_delta_after_do:.6f}",
                    "total_fee_paid": f"{r.total_fee_paid:.6f}",
                    "total_slippage_paid": f"{r.total_slippage_paid:.6f}",
                    "win_rate_net": f"{r.win_rate_net:.6f}",
                }
                for k in keys:
                    row[k] = r.params.get(k)
                w.writerow(row)

        print("")
        print("Parameter sweep complete (offline replay)")
        print(f"- Window: t0 [{window.start_t0} .. {window.end_t0}] (~{(window.end_t0 - window.start_t0) / DAY_MS:.2f} days)")
        print(f"- Candidates: {len(window.candidate_symbols)} (baseline={baseline_symbol})")
        if str(args.trade_mode) == "usdt-sleeve":
            print(f"- Trade mode: usdt-sleeve (sleeve_usdt=${float(args.sleeve_usdt):.2f})")
        else:
            print("- Trade mode: rebalance (sell BTC+ETH into ALT, then rebalance back)")
        print(f"- Configs evaluated: {len(results)} (grid size={grid_count})")
        print(f"- Results CSV: {out_path}")
        print("")

        top_n = max(1, int(args.top))
        print(f"Top {min(top_n, len(results))} by final_equity_after_do:")
        for r in results[:top_n]:
            params_str = " ".join(f"{k}={r.params.get(k)}" for k in keys)
            print(
                f"- {r.config_id} final=${r.final_equity_after_do:.2f} "
                f"(delta_vs_hold=${r.equity_delta_after_do:.2f}) trades={r.trades} "
                f"alerts(up/slow)={r.entry_alerts}/{r.exit_alerts} win={r.win_rate_net:.1%} {params_str}"
            )
        print("")

        print("Notes:")
        if int(args.bar_minutes) > 1:
            print("- This sweep regenerates signals/alerts from candles_hist_bars and disables spread gating (no historical order books).")
        else:
            print("- This sweep regenerates signals/alerts from candles_1m_hist and disables spread gating (no historical order books).")
        print("- Universe is the current universe_membership set (survivorship bias) unless you pass --symbols.")
        if str(args.trade_mode) == "usdt-sleeve":
            print("- The trading sim matches scripts/lookback_eval.py: USDT sleeve mode uses 2 legs per trade (buy/sell ALT).")
        else:
            print(
                "- The trading sim matches scripts/lookback_eval.py: rebalance mode uses 6 legs per trade "
                "(sell BTC + sell ETH + buy ALT + sell ALT + buy BTC + buy ETH)."
            )
        print("- With taker fees, costs can dominate unless average per-trade moves are meaningfully larger than round-trip friction.")

        return 0
    finally:
        await engine.dispose()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        return asyncio.run(_main_async(sys.argv[1:]))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
