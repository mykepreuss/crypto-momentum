from __future__ import annotations

from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.alerting import ALERT_TYPE_MOMENTUM_SLOWING, ALERT_TYPE_MOMENTUM_UP
from app.config import Settings
from app.models import Alert, Base, SignalState, UniverseMembership
from app.notifier.base import NullNotifier
from app.state_machine import run_state_machine_and_alert
from app.time_utils import now_ms


async def _make_sessionmaker():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


def _signal(
    *,
    symbol: str,
    score: float,
    t0: int,
    price: float,
    rank_rel_r15: float = 1.0,
    dv_z: float = 2.0,
    trend_ok: bool = True,
    passes_hard_gates: bool = True,
    passes_spread_gate: bool = True,
    spread: float = 0.001,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "score": score,
        "rank_rel_r15": rank_rel_r15,
        "passes_hard_gates": passes_hard_gates,
        "passes_spread_gate": passes_spread_gate,
        "spread": spread,
        "features": {
            "t0": t0,
            "price": price,
            "score": score,
            "rel_r15": 0.01,
            "dv_z": dv_z,
            "extension": 0.01,
            "trend_ok": trend_ok,
        },
    }


class NoBookExchange:
    async def get_book(self, instrument_name: str, depth: int = 10):
        raise AssertionError("get_book should not be called when passes_spread_gate=True")


async def test_state_machine_entry_budget_blocks_second_entry_same_scan() -> None:
    engine, session_factory = await _make_sessionmaker()
    try:
        async with session_factory() as session:
            session.add_all(
                [
                    UniverseMembership(
                        symbol="AAA_USDT",
                        quote_ccy="USDT",
                        is_active=True,
                        is_baseline=False,
                        liquidity_rank=1,
                        dollar_vol_24h=Decimal("100000"),
                        updated_ts=now_ms(),
                    ),
                    UniverseMembership(
                        symbol="BBB_USDT",
                        quote_ccy="USDT",
                        is_active=True,
                        is_baseline=False,
                        liquidity_rank=2,
                        dollar_vol_24h=Decimal("90000"),
                        updated_ts=now_ms(),
                    ),
                ]
            )
            await session.commit()

        t0 = 1_700_000_000_000
        signals_snapshot = {
            "baseline_symbol": "BTC_USDT",
            "signals": [
                _signal(symbol="AAA_USDT", score=0.90, t0=t0, price=10.0),
                _signal(symbol="BBB_USDT", score=0.85, t0=t0, price=5.0),
            ],
        }

        settings = Settings(
            quote_ccy="USDT",
            entry_score_threshold=0.80,
            max_entry_alerts_24h=1,
            max_total_alerts_24h=10,
            global_entry_cooldown_min=0,
            max_entry_alerts_per_scan=10,
            database_url="sqlite+aiosqlite:///:memory:",
        )

        res = await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=NoBookExchange(),  # type: ignore[arg-type]
            notifier=NullNotifier(),
            settings=settings,
            signals_snapshot=signals_snapshot,
        )
        assert res["ok"] is True
        assert res["new_alerts"] == 1

        async with session_factory() as session:
            entries = (
                await session.execute(sa.select(Alert).where(Alert.alert_type == ALERT_TYPE_MOMENTUM_UP))
            ).scalars().all()
            assert len(entries) == 1
            assert entries[0].symbol == "AAA_USDT"

            states = (await session.execute(sa.select(SignalState))).scalars().all()
            by_sym = {s.symbol: s for s in states}
            assert by_sym["AAA_USDT"].state == "IN"
            assert by_sym["BBB_USDT"].state == "OUT"
    finally:
        await engine.dispose()


async def test_state_machine_btc_regime_gate_blocks_entries_only() -> None:
    engine, session_factory = await _make_sessionmaker()
    try:
        async with session_factory() as session:
            session.add(
                UniverseMembership(
                    symbol="AAA_USDT",
                    quote_ccy="USDT",
                    is_active=True,
                    is_baseline=False,
                    liquidity_rank=1,
                    dollar_vol_24h=Decimal("100000"),
                    updated_ts=now_ms(),
                )
            )
            await session.commit()

        t0 = 1_700_000_000_000
        settings = Settings(
            quote_ccy="USDT",
            entry_score_threshold=0.80,
            require_btc_trend_ok_for_entries=True,
            global_entry_cooldown_min=0,
            max_entry_alerts_per_scan=10,
            max_entry_alerts_24h=50,
            max_total_alerts_24h=50,
            database_url="sqlite+aiosqlite:///:memory:",
        )

        res1 = await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=NoBookExchange(),  # type: ignore[arg-type]
            notifier=NullNotifier(),
            settings=settings,
            signals_snapshot={
                "baseline_symbol": "BTC_USDT",
                "baseline_trend_ok": False,
                "signals": [_signal(symbol="AAA_USDT", score=0.90, t0=t0, price=10.0)],
            },
        )
        assert res1["ok"] is True
        assert res1["new_alerts"] == 0

        res2 = await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=NoBookExchange(),  # type: ignore[arg-type]
            notifier=NullNotifier(),
            settings=settings,
            signals_snapshot={
                "baseline_symbol": "BTC_USDT",
                "baseline_trend_ok": True,
                "signals": [_signal(symbol="AAA_USDT", score=0.90, t0=t0 + 60_000, price=10.1)],
            },
        )
        assert res2["ok"] is True
        assert res2["new_alerts"] == 1
    finally:
        await engine.dispose()


async def test_state_machine_min_rank_rel_r15_gate_blocks_entries() -> None:
    engine, session_factory = await _make_sessionmaker()
    try:
        async with session_factory() as session:
            session.add(
                UniverseMembership(
                    symbol="AAA_USDT",
                    quote_ccy="USDT",
                    is_active=True,
                    is_baseline=False,
                    liquidity_rank=1,
                    dollar_vol_24h=Decimal("100000"),
                    updated_ts=now_ms(),
                )
            )
            await session.commit()

        t0 = 1_700_000_000_000
        settings = Settings(
            quote_ccy="USDT",
            entry_score_threshold=0.80,
            min_rank_rel_r15=0.90,
            global_entry_cooldown_min=0,
            max_entry_alerts_per_scan=10,
            max_entry_alerts_24h=50,
            max_total_alerts_24h=50,
            database_url="sqlite+aiosqlite:///:memory:",
        )

        res1 = await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=NoBookExchange(),  # type: ignore[arg-type]
            notifier=NullNotifier(),
            settings=settings,
            signals_snapshot={
                "baseline_symbol": "BTC_USDT",
                "signals": [_signal(symbol="AAA_USDT", score=0.90, rank_rel_r15=0.50, t0=t0, price=10.0)],
            },
        )
        assert res1["ok"] is True
        assert res1["new_alerts"] == 0

        res2 = await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=NoBookExchange(),  # type: ignore[arg-type]
            notifier=NullNotifier(),
            settings=settings,
            signals_snapshot={
                "baseline_symbol": "BTC_USDT",
                "signals": [_signal(symbol="AAA_USDT", score=0.90, rank_rel_r15=0.95, t0=t0 + 60_000, price=10.1)],
            },
        )
        assert res2["ok"] is True
        assert res2["new_alerts"] == 1
    finally:
        await engine.dispose()


async def test_state_machine_exit_requires_recent_entry_alert() -> None:
    engine, session_factory = await _make_sessionmaker()
    try:
        async with session_factory() as session:
            session.add(
                UniverseMembership(
                    symbol="AAA_USDT",
                    quote_ccy="USDT",
                    is_active=True,
                    is_baseline=False,
                    liquidity_rank=1,
                    dollar_vol_24h=Decimal("100000"),
                    updated_ts=now_ms(),
                )
            )
            lookback_ms = 24 * 3600 * 1000
            old_entry_ts = now_ms() - (lookback_ms + 60_000)
            session.add(
                SignalState(
                    symbol="AAA_USDT",
                    state="IN",
                    last_state_change_ts=old_entry_ts,
                    last_entry_alert_ts=old_entry_ts,
                    last_exit_alert_ts=None,
                    peak_price_since_entry=Decimal("10"),
                    peak_ts_since_entry=1_700_000_000_000,
                )
            )
            await session.commit()

        t0 = 1_700_000_000_000
        signals_snapshot = {
            "baseline_symbol": "BTC_USDT",
            "signals": [
                _signal(symbol="AAA_USDT", score=0.60, t0=t0, price=9.0, trend_ok=False),
            ],
        }
        settings = Settings(
            quote_ccy="USDT",
            exit_score_threshold=0.55,
            symbol_exit_cooldown_min=0,
            alert_lookback_hours=24,
            database_url="sqlite+aiosqlite:///:memory:",
        )

        res = await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=NoBookExchange(),  # type: ignore[arg-type]
            notifier=NullNotifier(),
            settings=settings,
            signals_snapshot=signals_snapshot,
        )
        assert res["ok"] is True
        assert res["new_alerts"] == 0

        async with session_factory() as session:
            exits = (
                await session.execute(
                    sa.select(Alert).where(Alert.alert_type == ALERT_TYPE_MOMENTUM_SLOWING)
                )
            ).scalars().all()
            assert exits == []

            st = (await session.execute(sa.select(SignalState).where(SignalState.symbol == "AAA_USDT"))).scalars().one()
            assert st.state == "OUT"
    finally:
        await engine.dispose()


async def test_state_machine_exit_entry_lookback_hours_decouples_from_budget_window() -> None:
    engine, session_factory = await _make_sessionmaker()
    try:
        now = now_ms()
        async with session_factory() as session:
            session.add(
                UniverseMembership(
                    symbol="AAA_USDT",
                    quote_ccy="USDT",
                    is_active=True,
                    is_baseline=False,
                    liquidity_rank=1,
                    dollar_vol_24h=Decimal("100000"),
                    updated_ts=now,
                )
            )
            # Entry was 25h ago: outside 24h budget window, but inside 7d exit eligibility window.
            entry_ts = now - (25 * 3600 * 1000)
            session.add(
                SignalState(
                    symbol="AAA_USDT",
                    state="IN",
                    last_state_change_ts=entry_ts,
                    last_entry_alert_ts=entry_ts,
                    last_exit_alert_ts=None,
                    peak_price_since_entry=Decimal("10"),
                    peak_ts_since_entry=1_700_000_000_000,
                )
            )
            await session.commit()

        t0 = 1_700_000_000_000
        settings = Settings(
            quote_ccy="USDT",
            # Budgets remain 24h.
            alert_lookback_hours=24,
            # Exit eligibility widened for higher-timeframe holds.
            exit_entry_lookback_hours=168,
            exit_score_threshold=0.55,
            symbol_exit_cooldown_min=0,
            max_exit_alerts_24h=50,
            max_total_alerts_24h=200,
            database_url="sqlite+aiosqlite:///:memory:",
        )

        res = await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=NoBookExchange(),  # type: ignore[arg-type]
            notifier=NullNotifier(),
            settings=settings,
            signals_snapshot={
                "baseline_symbol": "BTC_USDT",
                "signals": [_signal(symbol="AAA_USDT", score=0.50, t0=t0, price=9.0)],
            },
        )
        assert res["ok"] is True
        assert res["new_alerts"] == 1

        async with session_factory() as session:
            exits = (
                await session.execute(sa.select(Alert).where(Alert.alert_type == ALERT_TYPE_MOMENTUM_SLOWING))
            ).scalars().all()
            assert len(exits) == 1
            assert exits[0].symbol == "AAA_USDT"
    finally:
        await engine.dispose()


async def test_state_machine_exits_on_missing_signal_row_to_avoid_stuck_in() -> None:
    engine, session_factory = await _make_sessionmaker()
    try:
        now = now_ms()
        async with session_factory() as session:
            session.add_all(
                [
                    UniverseMembership(
                        symbol="AAA_USDT",
                        quote_ccy="USDT",
                        is_active=True,
                        is_baseline=False,
                        liquidity_rank=1,
                        dollar_vol_24h=Decimal("100000"),
                        updated_ts=now,
                    ),
                    UniverseMembership(
                        symbol="BBB_USDT",
                        quote_ccy="USDT",
                        is_active=True,
                        is_baseline=False,
                        liquidity_rank=2,
                        dollar_vol_24h=Decimal("90000"),
                        updated_ts=now,
                    ),
                    SignalState(
                        symbol="AAA_USDT",
                        state="IN",
                        last_state_change_ts=now - 60_000,
                        last_entry_alert_ts=now - 60_000,
                        last_exit_alert_ts=None,
                        peak_price_since_entry=Decimal("10"),
                        peak_ts_since_entry=1_700_000_000_000,
                    ),
                ]
            )
            await session.commit()

        t0 = 1_700_000_000_000
        signals_snapshot = {
            "baseline_symbol": "BTC_USDT",
            # BBB has a row, AAA is missing => AAA should exit due to data_missing.
            "signals": [
                _signal(symbol="BBB_USDT", score=0.10, t0=t0, price=5.0),
            ],
        }
        settings = Settings(
            quote_ccy="USDT",
            entry_score_threshold=0.80,
            exit_score_threshold=0.55,
            symbol_exit_cooldown_min=0,
            alert_lookback_hours=24,
            max_exit_alerts_24h=10,
            max_total_alerts_24h=100,
            database_url="sqlite+aiosqlite:///:memory:",
        )

        res = await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=NoBookExchange(),  # type: ignore[arg-type]
            notifier=NullNotifier(),
            settings=settings,
            signals_snapshot=signals_snapshot,
        )
        assert res["ok"] is True
        assert res["new_alerts"] == 1

        async with session_factory() as session:
            exits = (
                await session.execute(
                    sa.select(Alert).where(Alert.alert_type == ALERT_TYPE_MOMENTUM_SLOWING)
                )
            ).scalars().all()
            assert len(exits) == 1
            assert exits[0].symbol == "AAA_USDT"
            assert exits[0].features_json["reason"] == "data_missing"

            st = (await session.execute(sa.select(SignalState).where(SignalState.symbol == "AAA_USDT"))).scalars().one()
            assert st.state == "OUT"
    finally:
        await engine.dispose()


async def test_state_machine_missing_signal_row_does_not_force_exit_on_snapshot_error() -> None:
    engine, session_factory = await _make_sessionmaker()
    try:
        now = now_ms()
        async with session_factory() as session:
            session.add(
                UniverseMembership(
                    symbol="AAA_USDT",
                    quote_ccy="USDT",
                    is_active=True,
                    is_baseline=False,
                    liquidity_rank=1,
                    dollar_vol_24h=Decimal("100000"),
                    updated_ts=now,
                )
            )
            session.add(
                SignalState(
                    symbol="AAA_USDT",
                    state="IN",
                    last_state_change_ts=now - 60_000,
                    last_entry_alert_ts=now - 60_000,
                    last_exit_alert_ts=None,
                    peak_price_since_entry=Decimal("10"),
                    peak_ts_since_entry=1_700_000_000_000,
                )
            )
            await session.commit()

        signals_snapshot = {
            "baseline_symbol": "BTC_USDT",
            "error": "signal engine unavailable",
            "signals": [],
        }
        settings = Settings(
            quote_ccy="USDT",
            symbol_exit_cooldown_min=0,
            alert_lookback_hours=24,
            database_url="sqlite+aiosqlite:///:memory:",
        )

        res = await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=NoBookExchange(),  # type: ignore[arg-type]
            notifier=NullNotifier(),
            settings=settings,
            signals_snapshot=signals_snapshot,
        )
        assert res["ok"] is True
        assert res["new_alerts"] == 0

        async with session_factory() as session:
            exits = (
                await session.execute(
                    sa.select(Alert).where(Alert.alert_type == ALERT_TYPE_MOMENTUM_SLOWING)
                )
            ).scalars().all()
            assert exits == []

            st = (await session.execute(sa.select(SignalState).where(SignalState.symbol == "AAA_USDT"))).scalars().one()
            assert st.state == "IN"
    finally:
        await engine.dispose()


async def test_state_machine_symbol_entry_cooldown_blocks_reentry() -> None:
    engine, session_factory = await _make_sessionmaker()
    try:
        async with session_factory() as session:
            session.add(
                UniverseMembership(
                    symbol="AAA_USDT",
                    quote_ccy="USDT",
                    is_active=True,
                    is_baseline=False,
                    liquidity_rank=1,
                    dollar_vol_24h=Decimal("100000"),
                    updated_ts=now_ms(),
                )
            )
            await session.commit()

        settings = Settings(
            quote_ccy="USDT",
            entry_score_threshold=0.80,
            exit_score_threshold=0.55,
            symbol_entry_cooldown_min=90,
            symbol_exit_cooldown_min=0,
            global_entry_cooldown_min=0,
            max_entry_alerts_per_scan=5,
            max_total_alerts_24h=50,
            max_entry_alerts_24h=50,
            max_exit_alerts_24h=50,
            database_url="sqlite+aiosqlite:///:memory:",
        )
        t0 = 1_700_000_000_000

        # 1) Entry
        res1 = await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=NoBookExchange(),  # type: ignore[arg-type]
            notifier=NullNotifier(),
            settings=settings,
            signals_snapshot={
                "baseline_symbol": "BTC_USDT",
                "signals": [_signal(symbol="AAA_USDT", score=0.90, t0=t0, price=10.0)],
            },
        )
        assert res1["new_alerts"] == 1

        # 2) Exit immediately
        res2 = await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=NoBookExchange(),  # type: ignore[arg-type]
            notifier=NullNotifier(),
            settings=settings,
            signals_snapshot={
                "baseline_symbol": "BTC_USDT",
                "signals": [_signal(symbol="AAA_USDT", score=0.60, t0=t0 + 60_000, price=9.0, trend_ok=False)],
            },
        )
        assert res2["new_alerts"] == 1

        # 3) Attempt re-entry (should be blocked by symbol_entry_cooldown_min)
        res3 = await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=NoBookExchange(),  # type: ignore[arg-type]
            notifier=NullNotifier(),
            settings=settings,
            signals_snapshot={
                "baseline_symbol": "BTC_USDT",
                "signals": [_signal(symbol="AAA_USDT", score=0.90, t0=t0 + 120_000, price=10.5)],
            },
        )
        assert res3["new_alerts"] == 0

        async with session_factory() as session:
            entries = (
                await session.execute(sa.select(Alert).where(Alert.alert_type == ALERT_TYPE_MOMENTUM_UP))
            ).scalars().all()
            assert len(entries) == 1
    finally:
        await engine.dispose()
