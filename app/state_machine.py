from __future__ import annotations

import logging
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.alerting import (
    ALERT_TYPE_MOMENTUM_SLOWING,
    ALERT_TYPE_MOMENTUM_UP,
    format_momentum_slowing,
    format_momentum_up,
)
from app.config import Settings
from app.exchange_client import CryptoComExchangeClient
from app.models import Alert, SignalState, UniverseMembership
from app.notifier.base import Notifier
from app.time_utils import now_ms

logger = logging.getLogger(__name__)


def _ms(minutes: int) -> int:
    return int(minutes * 60_000)


def _spread_from_book(book: dict[str, Any]) -> Optional[float]:
    bids = book.get("bids")
    asks = book.get("asks")
    if not isinstance(bids, list) or not bids:
        return None
    if not isinstance(asks, list) or not asks:
        return None
    try:
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
    except (TypeError, ValueError, IndexError):
        return None
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0.0:
        return None
    return (best_ask - best_bid) / mid


def determine_exit_reason(
    *,
    score: float,
    trend_ok: bool,
    dv_z: float,
    t0: int,
    peak_ts: Optional[int],
    settings: Settings,
) -> Optional[str]:
    if score <= settings.exit_score_threshold:
        return "score_below_exit_threshold"
    if trend_ok is False:
        return "trend_break"
    if (
        peak_ts is not None
        and (int(t0) - int(peak_ts)) >= _ms(settings.stall_minutes)
        and float(dv_z) < settings.stall_dvz_max
    ):
        return "stall"
    return None


async def _alert_counts(session: AsyncSession, since_ts: int) -> dict[str, int]:
    rows = (
        await session.execute(
            select(Alert.alert_type, func.count()).where(Alert.ts >= since_ts).group_by(Alert.alert_type)
        )
    ).all()
    return {str(t): int(c) for (t, c) in rows}


async def _last_entry_ts(session: AsyncSession) -> Optional[int]:
    return (
        await session.execute(select(func.max(Alert.ts)).where(Alert.alert_type == ALERT_TYPE_MOMENTUM_UP))
    ).scalar_one_or_none()


async def _load_universe_symbols(session: AsyncSession, quote_ccy: str, baseline_symbol: Optional[str]) -> list[str]:
    symbols = (
        await session.execute(
            select(UniverseMembership.symbol)
            .where(UniverseMembership.quote_ccy == quote_ccy, UniverseMembership.is_active.is_(True))
            .order_by(UniverseMembership.liquidity_rank.asc())
        )
    ).scalars().all()
    out = [str(s) for s in symbols]
    if baseline_symbol and baseline_symbol in out:
        out = [s for s in out if s != baseline_symbol]
    return out


async def _load_signal_states(session: AsyncSession, symbols: list[str], now: int) -> dict[str, SignalState]:
    if not symbols:
        return {}
    rows = (await session.execute(select(SignalState).where(SignalState.symbol.in_(symbols)))).scalars().all()
    state_by_symbol = {r.symbol: r for r in rows}
    for sym in symbols:
        if sym not in state_by_symbol:
            st = SignalState(symbol=sym, state="OUT", last_state_change_ts=now)
            session.add(st)
            state_by_symbol[sym] = st
    return state_by_symbol


async def run_state_machine_and_alert(
    session_factory: async_sessionmaker[AsyncSession],
    exchange: CryptoComExchangeClient,
    notifier: Notifier,
    settings: Settings,
    signals_snapshot: dict[str, Any],
) -> dict[str, Any]:
    now = now_ms()
    lookback_ms = int(settings.alert_lookback_hours * 3600 * 1000)
    window_start = now - lookback_ms

    baseline_symbol = signals_snapshot.get("baseline_symbol")
    snapshot_error = signals_snapshot.get("error")
    signals = signals_snapshot.get("signals") or []
    if not isinstance(signals, list):
        return {"ok": False, "error": "signals_snapshot.signals is not a list"}

    signals_by_symbol = {str(r["symbol"]): r for r in signals if isinstance(r, dict) and "symbol" in r}

    async with session_factory() as session:
        counts = await _alert_counts(session, since_ts=window_start)
        entry_count = counts.get(ALERT_TYPE_MOMENTUM_UP, 0)
        exit_count = counts.get(ALERT_TYPE_MOMENTUM_SLOWING, 0)
        total_count = sum(counts.values())

        last_entry_ts = await _last_entry_ts(session)
        global_entry_ok = (
            last_entry_ts is None or (now - int(last_entry_ts)) >= _ms(settings.global_entry_cooldown_min)
        )

        universe_symbols = await _load_universe_symbols(
            session=session,
            quote_ccy=settings.quote_ccy,
            baseline_symbol=str(baseline_symbol) if baseline_symbol else None,
        )
        state_by_symbol = await _load_signal_states(session, universe_symbols, now=now)

        new_alerts: list[Alert] = []

        # 1) Exits first (more important if you're already "in").
        for sym in universe_symbols:
            st = state_by_symbol.get(sym)
            if st is None or st.state != "IN":
                continue
            r = signals_by_symbol.get(sym)

            exit_reason: Optional[str] = None
            score = 0.0
            features: dict[str, Any] = {}

            if r is None:
                # If we have a good snapshot but a symbol is missing, don't get stuck IN forever.
                # (If the snapshot itself errored, avoid forcing exits due to missing data.)
                if snapshot_error is None:
                    exit_reason = "data_missing"
                    features = {"score": 0.0, "dv_z": 0.0}
                else:
                    continue
            else:
                try:
                    score = float(r.get("score", 0.0))
                except (TypeError, ValueError):
                    score = 0.0

                raw_features = r.get("features") or {}
                if not isinstance(raw_features, dict):
                    if snapshot_error is not None:
                        continue
                    exit_reason = "data_missing"
                    features = {"score": score, "dv_z": 0.0}
                else:
                    features = raw_features
                    t0 = features.get("t0")
                    price = features.get("price")
                    dv_z = features.get("dv_z")
                    trend_ok = features.get("trend_ok")
                    if (
                        not isinstance(t0, int)
                        or not isinstance(price, (int, float))
                        or not isinstance(dv_z, (int, float))
                        or not isinstance(trend_ok, bool)
                    ):
                        if snapshot_error is not None:
                            continue
                        exit_reason = "data_missing"
                        features = {"score": score, "dv_z": float(dv_z) if isinstance(dv_z, (int, float)) else 0.0}
                    else:
                        # Update peak tracking.
                        if st.peak_price_since_entry is None or float(price) > float(st.peak_price_since_entry):
                            st.peak_price_since_entry = float(price)
                            st.peak_ts_since_entry = int(t0)
                        high = features.get("high")
                        if isinstance(high, (int, float)):
                            if st.peak_high_since_entry is None or float(high) > float(st.peak_high_since_entry):
                                st.peak_high_since_entry = float(high)
                                st.peak_high_ts_since_entry = int(t0)

                        exit_reason = determine_exit_reason(
                            score=score,
                            trend_ok=trend_ok,
                            dv_z=float(dv_z),
                            t0=int(t0),
                            peak_ts=st.peak_ts_since_entry,
                            settings=settings,
                        )

            if exit_reason is None:
                continue

            should_alert = True
            # Exit alerts only if we had an entry alert for this symbol in the last 24h.
            if st.last_entry_alert_ts is None or (now - int(st.last_entry_alert_ts)) > lookback_ms:
                should_alert = False
            if st.last_exit_alert_ts is not None and (now - int(st.last_exit_alert_ts)) < _ms(
                settings.symbol_exit_cooldown_min
            ):
                should_alert = False
            if exit_count >= settings.max_exit_alerts_24h or total_count >= settings.max_total_alerts_24h:
                should_alert = False

            if should_alert:
                peak_price = float(st.peak_price_since_entry) if st.peak_price_since_entry is not None else None
                message = format_momentum_slowing(
                    symbol=sym,
                    reason=exit_reason,
                    features=features,
                    peak_price=peak_price,
                )
                alert = Alert(
                    ts=now,
                    symbol=sym,
                    alert_type=ALERT_TYPE_MOMENTUM_SLOWING,
                    score=score,
                    features_json={**features, "reason": exit_reason},
                    message=message,
                    delivered=False,
                    delivery_channel=None,
                )
                session.add(alert)
                new_alerts.append(alert)

                st.last_exit_alert_ts = now
                exit_count += 1
                total_count += 1

            # Always transition state OUT when exit conditions are met, even if we don't emit an alert
            # (e.g. entry was older than the rolling lookback window).
            st.state = "OUT"
            st.last_state_change_ts = now
            st.peak_price_since_entry = None
            st.peak_ts_since_entry = None
            st.peak_high_since_entry = None
            st.peak_high_ts_since_entry = None

        # 2) Entries (budgeted + throttled).
        entries_fired = 0
        if global_entry_ok and entry_count < settings.max_entry_alerts_24h and total_count < settings.max_total_alerts_24h:
            # `signals` is already score-sorted; walk until we drop below threshold.
            for r in signals:
                if entry_count >= settings.max_entry_alerts_24h or total_count >= settings.max_total_alerts_24h:
                    break
                if entries_fired >= settings.max_entry_alerts_per_scan:
                    break
                if not isinstance(r, dict):
                    continue
                sym = str(r.get("symbol", "")).strip()
                if not sym:
                    continue

                score = float(r.get("score", 0.0))
                if score < settings.entry_score_threshold:
                    break

                st = state_by_symbol.get(sym)
                if st is None:
                    continue
                if st.state != "OUT":
                    continue

                if st.last_entry_alert_ts is not None and (now - int(st.last_entry_alert_ts)) < _ms(
                    settings.symbol_entry_cooldown_min
                ):
                    continue

                if not bool(r.get("passes_hard_gates")):
                    continue

                # Spread gate is only computed for top candidates (see signals.py). Avoid doing
                # on-demand book checks here to prevent bursts of API calls.
                spread = r.get("spread")
                if settings.book_check_top_n > 0 and r.get("passes_spread_gate") is not True:
                    continue

                features = r.get("features") or {}
                if not isinstance(features, dict):
                    continue
                # Ensure alert message has the spread we checked.
                features = {**features, "spread": spread}
                message = format_momentum_up(symbol=sym, features=features)

                alert = Alert(
                    ts=now,
                    symbol=sym,
                    alert_type=ALERT_TYPE_MOMENTUM_UP,
                    score=score,
                    features_json=features,
                    message=message,
                    delivered=False,
                    delivery_channel=None,
                )
                session.add(alert)
                new_alerts.append(alert)

                st.state = "IN"
                st.last_state_change_ts = now
                st.last_entry_alert_ts = now
                price = features.get("price")
                t0 = features.get("t0")
                high = features.get("high")
                if isinstance(price, (int, float)):
                    st.peak_price_since_entry = float(price)
                if isinstance(t0, int):
                    st.peak_ts_since_entry = int(t0)
                if isinstance(high, (int, float)):
                    st.peak_high_since_entry = float(high)
                if isinstance(t0, int) and isinstance(high, (int, float)):
                    st.peak_high_ts_since_entry = int(t0)

                entries_fired += 1
                entry_count += 1
                total_count += 1
                if settings.global_entry_cooldown_min > 0:
                    break

        # Persist state + alerts.
        if new_alerts:
            await session.commit()
        else:
            await session.commit()

    return {"ok": True, "new_alerts": len(new_alerts)}
