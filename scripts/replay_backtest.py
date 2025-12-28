#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import sys
import uuid
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import func, select

from app.alerting import (
    ALERT_TYPE_MOMENTUM_SLOWING,
    ALERT_TYPE_MOMENTUM_UP,
    format_momentum_slowing,
    format_momentum_up,
)
from app.config import Settings, get_settings
from app.db import create_engine, create_sessionmaker, session_scope
from app.models import Candle1mHist, CandleHistBar, UniverseMembership
from app.replay import CandlePoint, MINUTE_MS, RollingFeatureState
from app.scoring import ScoreComponents, compute_score, dv_term_from_dvz, percentile_ranks
from app.state_machine import determine_exit_reason
from app.time_utils import floor_minute_ms, now_ms


DAY_MS = 24 * 60 * MINUTE_MS

logger = logging.getLogger("replay_backtest")


def _parse_symbols_csv(s: str) -> list[str]:
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _decimal_equal(a: object, b: object) -> bool:
    try:
        return Decimal(str(a)) == Decimal(str(b))
    except (InvalidOperation, ValueError, TypeError):
        return False


async def _assert_hist_bars_close_timestamps(
    *,
    session,
    timeframe_min: int,
    symbol: str,
) -> None:
    """
    Guardrail: `candles_hist_bars.t` must represent BAR CLOSE time.

    Earlier versions stored bars at bar start while also using bar close prices, which introduced
    a lookahead in replay/sweep/eval. This check detects that condition and tells the operator
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


def _feature_dict(fs, *, score: float) -> dict[str, Any]:
    return {
        "t0": fs.t0,
        "price": fs.price,
        "high": fs.high,
        "score": score,
        "rel_r15": fs.rel_r15,
        "rel_r5": fs.rel_r5,
        "accel": fs.accel,
        "dv_z": fs.dv_z,
        "avg_dv_1m": fs.avg_dv_1m,
        "extension": fs.extension,
        "breakout": fs.breakout,
        "trend_ok": fs.trend_ok,
        "spread": None,
    }


@dataclass
class PositionState:
    state: str = "OUT"  # OUT|IN
    last_state_change_ts: int = 0
    last_entry_alert_ts: Optional[int] = None
    last_exit_alert_ts: Optional[int] = None
    peak_price_since_entry: Optional[float] = None
    peak_ts_since_entry: Optional[int] = None
    peak_high_since_entry: Optional[float] = None
    peak_high_ts_since_entry: Optional[int] = None


def _ms(minutes: int) -> int:
    return int(minutes * 60_000)


def _expire(times: list[int], *, cutoff: int) -> int:
    # In-place remove items < cutoff. Returns new length.
    # (Budgets are tiny, so O(n) each step is fine.)
    keep = [t for t in times if t >= cutoff]
    times[:] = keep
    return len(times)


async def _load_universe_symbols(*, session, quote_ccy: str, max_symbols: Optional[int]) -> tuple[list[str], Optional[str]]:
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


async def _baseline_range(
    *,
    session,
    candles_model,
    baseline_symbol: str,
    timeframe_min: Optional[int],
) -> tuple[Optional[int], Optional[int]]:
    stmt = select(func.min(candles_model.t), func.max(candles_model.t)).where(candles_model.symbol == baseline_symbol)
    if candles_model is CandleHistBar:
        if timeframe_min is None:
            raise ValueError("timeframe_min is required for CandleHistBar")
        stmt = stmt.where(CandleHistBar.timeframe_min == int(timeframe_min))

    row = (await session.execute(stmt)).one()
    min_t, max_t = row
    return (int(min_t) if min_t is not None else None, int(max_t) if max_t is not None else None)


async def _main_async(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="scripts/replay_backtest.py",
        description="Offline replay: regenerate signals and MOMENTUM_UP/MOMENTUM_SLOWING alerts from candles_1m_hist / candles_hist_bars.",
    )
    parser.add_argument("--days", type=int, default=365, help="Backtest window in days (default: 365)")
    parser.add_argument(
        "--warmup-min",
        type=int,
        default=120,
        help="Warmup minutes ingested before the window start (default: 120)",
    )
    parser.add_argument(
        "--baseline-symbol",
        type=str,
        default="BTC_USDT",
        help="Baseline symbol (default: BTC_USDT)",
    )
    parser.add_argument(
        "--from-universe",
        action="store_true",
        help="Use current universe_membership table (recommended) rather than --symbols.",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Limit symbols when using --from-universe (useful for quicker backtests).",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Comma-separated symbols to backtest (if not using --from-universe).",
    )
    parser.add_argument(
        "--chunk-minutes",
        type=int,
        default=1440,
        help="DB fetch chunk size in minutes (default: 1440 = 1 day)",
    )
    parser.add_argument(
        "--step-minutes",
        type=int,
        default=1,
        help="Compute signals/alerts every N minutes (default: 1). Candles are always ingested at 1m granularity.",
    )
    parser.add_argument(
        "--bar-minutes",
        type=int,
        default=1,
        help=(
            "Use aggregated historical bars from candles_hist_bars instead of 1m candles. "
            "Example: --bar-minutes 60 (requires running scripts/build_hist_bars.py first)."
        ),
    )
    parser.add_argument(
        "--out-alerts-csv",
        type=str,
        default="replay_alerts.csv",
        help="Output CSV path for regenerated alerts (default: replay_alerts.csv)",
    )
    args = parser.parse_args(argv)

    if args.days < 1:
        raise SystemExit("--days must be >= 1")
    if args.warmup_min < 0:
        raise SystemExit("--warmup-min must be >= 0")
    if args.chunk_minutes < 60:
        raise SystemExit("--chunk-minutes must be >= 60")
    if args.step_minutes < 1:
        raise SystemExit("--step-minutes must be >= 1")
    if args.bar_minutes < 1:
        raise SystemExit("--bar-minutes must be >= 1")
    if int(args.bar_minutes) > 1:
        # Convenience defaults for bar replays.
        if int(args.step_minutes) == 1:
            args.step_minutes = int(args.bar_minutes)
        if int(args.warmup_min) == 120:
            # Ensure enough history for dv/vwap (60 bars) by default.
            args.warmup_min = 60 * int(args.bar_minutes)

    base_settings = get_settings()
    # Offline replay cannot use historical order books; disable spread gating.
    settings: Settings = base_settings.model_copy(update={"book_check_top_n": 0})

    engine = create_engine(settings)
    session_factory = create_sessionmaker(engine)

    try:
        async with session_scope(session_factory) as session:
            if args.from_universe:
                symbols, baseline_from_db = await _load_universe_symbols(
                    session=session,
                    quote_ccy=settings.quote_ccy,
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

            candles_model = Candle1mHist
            candle_step_ms = MINUTE_MS
            timeframe_min: Optional[int] = None
            if int(args.bar_minutes) > 1:
                candles_model = CandleHistBar
                candle_step_ms = int(args.bar_minutes) * MINUTE_MS
                timeframe_min = int(args.bar_minutes)

                if int(args.warmup_min) % int(args.bar_minutes) != 0:
                    raise SystemExit("--warmup-min must be a multiple of --bar-minutes for bar replays.")
                if int(args.chunk_minutes) % int(args.bar_minutes) != 0:
                    raise SystemExit("--chunk-minutes must be a multiple of --bar-minutes for bar replays.")
                if int(args.step_minutes) % int(args.bar_minutes) != 0:
                    raise SystemExit("--step-minutes must be a multiple of --bar-minutes for bar replays.")

                await _assert_hist_bars_close_timestamps(
                    session=session,
                    timeframe_min=int(timeframe_min),
                    symbol=baseline_symbol,
                )

            min_t, max_t = await _baseline_range(
                session=session,
                candles_model=candles_model,
                baseline_symbol=baseline_symbol,
                timeframe_min=timeframe_min,
            )
            if min_t is None or max_t is None:
                raise SystemExit(
                    f"No historical candles found for baseline {baseline_symbol}. "
                    "Run scripts/backfill_hist_candles.py (1m) and optionally scripts/build_hist_bars.py (bars) first."
                )

        desired_end_t0 = floor_minute_ms(now_ms()) - MINUTE_MS
        if int(args.bar_minutes) > 1:
            desired_end_t0 = (int(desired_end_t0) // int(candle_step_ms)) * int(candle_step_ms)
        end_t0 = min(int(max_t), int(desired_end_t0))
        start_t0 = end_t0 - (int(args.days) * DAY_MS)
        ingest_start_t0 = max(int(min_t), start_t0 - (int(args.warmup_min) * MINUTE_MS))
        if int(args.bar_minutes) > 1:
            ingest_start_t0 = (int(ingest_start_t0) // int(candle_step_ms)) * int(candle_step_ms)

        if end_t0 <= ingest_start_t0:
            raise SystemExit("Not enough candle history for the requested window.")

        candidate_symbols = [s for s in symbols if s != baseline_symbol]
        trend_bucket_steps = 1 if int(args.bar_minutes) > 1 else 5
        feature_state: dict[str, RollingFeatureState] = {
            s: RollingFeatureState(step_ms=int(candle_step_ms), trend_bucket_steps=int(trend_bucket_steps)) for s in symbols
        }

        position_state: dict[str, PositionState] = {
            s: PositionState(state="OUT", last_state_change_ts=ingest_start_t0) for s in candidate_symbols
        }

        entry_times: list[int] = []
        exit_times: list[int] = []
        total_times: list[int] = []
        last_global_entry_ts: Optional[int] = None

        alerts_out: list[dict[str, object]] = []

        budget_window_ms = int(settings.alert_lookback_hours * 3600 * 1000)
        exit_entry_lookback_ms = int(settings.exit_entry_lookback_hours * 3600 * 1000)

        async with session_scope(session_factory) as session:
            chunk_ms = int(args.chunk_minutes) * MINUTE_MS
            compute_every_ms = int(args.step_minutes) * MINUTE_MS

            logger.info(
                "replay_start",
                extra={
                    "symbols": len(symbols),
                    "candidate_symbols": len(candidate_symbols),
                    "baseline": baseline_symbol,
                    "ingest_start_t0": ingest_start_t0,
                    "start_t0": start_t0,
                    "end_t0": end_t0,
                },
            )

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
                            Candle1mHist.symbol.in_(symbols),
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
                            CandleHistBar.symbol.in_(symbols),
                            CandleHistBar.t >= chunk_start,
                            CandleHistBar.t <= chunk_end,
                        )
                        .order_by(CandleHistBar.t.asc(), CandleHistBar.symbol.asc())
                    )

                rows = (await session.execute(stmt)).all()
                if not rows:
                    continue

                current_t: Optional[int] = None
                updated_symbols: set[str] = set()

                def process_minute(t0: int, updated: set[str]) -> None:
                    nonlocal last_global_entry_ts

                    if t0 < start_t0:
                        return

                    # Compute only on cadence (but always ingest all candles).
                    if (t0 - start_t0) % compute_every_ms != 0:
                        return

                    baseline_state = feature_state[baseline_symbol]
                    if baseline_state.last_t != t0:
                        return
                    baseline_reason = baseline_state.baseline_usable_reason()
                    if baseline_reason is not None:
                        return

                    baseline_fs, _baseline_fs_reason = baseline_state.compute_features_with_reason(
                        baseline=baseline_state,
                        t0=t0,
                    )
                    if baseline_fs is None:
                        return
                    baseline_trend_ok = bool(baseline_fs.trend_ok)

                    # Expire budgets to the rolling lookback window.
                    window_start = t0 - budget_window_ms
                    entry_count = _expire(entry_times, cutoff=window_start)
                    exit_count = _expire(exit_times, cutoff=window_start)
                    total_count = _expire(total_times, cutoff=window_start)

                    global_entry_ok = last_global_entry_ts is None or (t0 - int(last_global_entry_ts)) >= _ms(
                        settings.global_entry_cooldown_min
                    )

                    # 1) Compute features for the cross-section at this t0.
                    features: dict[str, Any] = {}
                    for sym in updated:
                        if sym == baseline_symbol:
                            continue
                        fs, _reason = feature_state[sym].compute_features_with_reason(baseline=baseline_state, t0=t0)
                        if fs is None:
                            continue
                        features[sym] = fs

                    if not features:
                        return

                    ranks_rel_r15 = percentile_ranks({sym: fs.rel_r15 for sym, fs in features.items()})
                    ranks_accel = percentile_ranks({sym: fs.accel for sym, fs in features.items()})

                    signals: list[dict[str, Any]] = []
                    for sym, fs in features.items():
                        components = ScoreComponents(
                            rank_rel_r15=ranks_rel_r15.get(sym, 0.0),
                            rank_accel=ranks_accel.get(sym, 0.0),
                            dv_term=dv_term_from_dvz(fs.dv_z),
                            breakout=fs.breakout,
                        )
                        score = compute_score(components)
                        hard_gates = {
                            "trend_ok": bool(fs.trend_ok),
                            "dv_z_min": fs.dv_z >= settings.dvz_min,
                            "extension_max": fs.extension <= settings.extension_max,
                            "min_dv_1m_usd": fs.avg_dv_1m >= settings.min_dv_1m_usd,
                        }
                        passes_hard = all(hard_gates.values())
                        signals.append(
                            {
                                "symbol": sym,
                                "score": float(score),
                                "rank_rel_r15": float(components.rank_rel_r15),
                                "passes_hard_gates": bool(passes_hard),
                                "passes_spread_gate": None,
                                "spread": None,
                                "features": _feature_dict(fs, score=float(score)),
                            }
                        )

                    signals.sort(key=lambda r: r["score"], reverse=True)
                    signals_by_symbol = {r["symbol"]: r for r in signals}

                    # 2) Exits first.
                    for sym, st in position_state.items():
                        if st.state != "IN":
                            continue
                        r = signals_by_symbol.get(sym)
                        if r is None:
                            continue
                        feats = r.get("features") or {}
                        if not isinstance(feats, dict):
                            continue
                        score = float(r.get("score", 0.0))
                        price = feats.get("price")
                        high = feats.get("high")
                        dv_z = feats.get("dv_z")
                        trend_ok = feats.get("trend_ok")
                        if not isinstance(price, (int, float)) or not isinstance(high, (int, float)):
                            continue
                        if not isinstance(dv_z, (int, float)) or not isinstance(trend_ok, bool):
                            continue

                        # Peak tracking (close + high).
                        if st.peak_price_since_entry is None or float(price) > float(st.peak_price_since_entry):
                            st.peak_price_since_entry = float(price)
                            st.peak_ts_since_entry = int(t0)
                        if st.peak_high_since_entry is None or float(high) > float(st.peak_high_since_entry):
                            st.peak_high_since_entry = float(high)
                            st.peak_high_ts_since_entry = int(t0)

                        exit_reason = determine_exit_reason(
                            score=score,
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
                            msg = format_momentum_slowing(
                                symbol=sym,
                                reason=exit_reason,
                                features=feats,
                                peak_price=st.peak_price_since_entry,
                            )
                            alert = {
                                "id": str(uuid.uuid4()),
                                "ts": int(t0),
                                "t0": int(t0),
                                "symbol": sym,
                                "alert_type": ALERT_TYPE_MOMENTUM_SLOWING,
                                "score": float(score),
                                "price": float(price),
                                "reason": exit_reason,
                                "message": msg,
                            }
                            alerts_out.append(alert)
                            exit_times.append(int(t0))
                            total_times.append(int(t0))
                            exit_count += 1
                            total_count += 1
                            st.last_exit_alert_ts = int(t0)

                        # Always transition OUT when exit conditions are met.
                        st.state = "OUT"
                        st.last_state_change_ts = int(t0)
                        st.peak_price_since_entry = None
                        st.peak_ts_since_entry = None
                        st.peak_high_since_entry = None
                        st.peak_high_ts_since_entry = None

                    # 3) Entries.
                    if not global_entry_ok:
                        return
                    if settings.require_btc_trend_ok_for_entries and not baseline_trend_ok:
                        return
                    if entry_count >= settings.max_entry_alerts_24h or total_count >= settings.max_total_alerts_24h:
                        return

                    entries_fired = 0
                    for r in signals:
                        if entry_count >= settings.max_entry_alerts_24h or total_count >= settings.max_total_alerts_24h:
                            break
                        if entries_fired >= settings.max_entry_alerts_per_scan:
                            break

                        sym = str(r.get("symbol", "")).strip()
                        if not sym:
                            continue
                        score = float(r.get("score", 0.0))
                        if score < settings.entry_score_threshold:
                            break

                        st = position_state.get(sym)
                        if st is None or st.state != "OUT":
                            continue
                        if st.last_entry_alert_ts is not None and (t0 - int(st.last_entry_alert_ts)) < _ms(
                            settings.symbol_entry_cooldown_min
                        ):
                            continue
                        if not bool(r.get("passes_hard_gates")):
                            continue
                        if settings.min_rank_rel_r15 > 0.0:
                            try:
                                rank_rel_r15 = float(r.get("rank_rel_r15", 0.0))
                            except (TypeError, ValueError):
                                rank_rel_r15 = 0.0
                            if rank_rel_r15 < float(settings.min_rank_rel_r15):
                                continue

                        feats = r.get("features") or {}
                        if not isinstance(feats, dict):
                            continue
                        price = feats.get("price")
                        high = feats.get("high")
                        if not isinstance(price, (int, float)) or not isinstance(high, (int, float)):
                            continue

                        msg = format_momentum_up(symbol=sym, features=feats)
                        alert = {
                            "id": str(uuid.uuid4()),
                            "ts": int(t0),
                            "t0": int(t0),
                            "symbol": sym,
                            "alert_type": ALERT_TYPE_MOMENTUM_UP,
                            "score": float(score),
                            "price": float(price),
                            "reason": "",
                            "message": msg,
                        }
                        alerts_out.append(alert)
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

                for symbol, t, o, h, l, c, v in rows:
                    t_i = int(t)
                    if current_t is None:
                        current_t = t_i
                    if t_i != current_t:
                        process_minute(current_t, updated_symbols)
                        updated_symbols = set()
                        current_t = t_i

                    sym = str(symbol)
                    if sym not in feature_state:
                        continue
                    cp = CandlePoint(
                        t=t_i,
                        o=float(o),
                        h=float(h),
                        l=float(l),
                        c=float(c),
                        v=float(v),
                    )
                    feature_state[sym].ingest(cp)
                    updated_symbols.add(sym)

                if current_t is not None:
                    process_minute(current_t, updated_symbols)

                # Progress log per chunk.
                logger.info(
                    "chunk_done",
                    extra={
                        "chunk_start": chunk_start,
                        "chunk_end": chunk_end,
                        "alerts": len(alerts_out),
                    },
                )

        out_path = Path(args.out_alerts_csv)
        with out_path.open("w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["id", "ts", "t0", "symbol", "alert_type", "score", "price", "reason", "message"],
            )
            writer.writeheader()
            writer.writerows(alerts_out)

        logger.info(
            "replay_done",
            extra={
                "alerts": len(alerts_out),
                "entries": sum(1 for a in alerts_out if a["alert_type"] == ALERT_TYPE_MOMENTUM_UP),
                "exits": sum(1 for a in alerts_out if a["alert_type"] == ALERT_TYPE_MOMENTUM_SLOWING),
                "out_csv": str(out_path),
            },
        )

        return 0
    finally:
        await engine.dispose()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return asyncio.run(_main_async(sys.argv[1:]))


if __name__ == "__main__":
    raise SystemExit(main())
