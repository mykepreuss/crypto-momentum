from __future__ import annotations

from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.candles import CandleStore, upsert_candle_rows
from app.config import Settings
from app.jobs.candle_ingestion import ingest_latest_candles_once
from app.models import Base, Candle1m, UniverseMembership
from app.time_utils import now_ms


async def _make_sessionmaker() -> tuple[AsyncEngine, async_sessionmaker]:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


async def test_upsert_candle_rows_is_idempotent_and_updates() -> None:
    engine, session_factory = await _make_sessionmaker()
    try:
        rows_v1 = [
            {"symbol": "SOL_USDT", "t": 60_000, "o": Decimal("1"), "h": Decimal("2"), "l": Decimal("0.5"), "c": Decimal("1.5"), "v": Decimal("10")},
            {"symbol": "SOL_USDT", "t": 120_000, "o": Decimal("1.5"), "h": Decimal("2"), "l": Decimal("1.0"), "c": Decimal("1.6"), "v": Decimal("5")},
        ]
        rows_v2 = [
            # same keys; second candle close changed
            {"symbol": "SOL_USDT", "t": 60_000, "o": Decimal("1"), "h": Decimal("2"), "l": Decimal("0.5"), "c": Decimal("1.5"), "v": Decimal("10")},
            {"symbol": "SOL_USDT", "t": 120_000, "o": Decimal("1.5"), "h": Decimal("2"), "l": Decimal("1.0"), "c": Decimal("1.7"), "v": Decimal("5")},
        ]

        async with session_factory() as session:
            await upsert_candle_rows(session, rows_v1)
            await upsert_candle_rows(session, rows_v1)
            await session.commit()

        async with session_factory() as session:
            count = (
                await session.execute(
                    sa.select(sa.func.count()).select_from(Candle1m).where(Candle1m.symbol == "SOL_USDT")
                )
            ).scalar_one()
            assert int(count) == 2

        async with session_factory() as session:
            await upsert_candle_rows(session, rows_v2)
            await session.commit()

        async with session_factory() as session:
            c = (await session.execute(sa.select(Candle1m).where(Candle1m.symbol == "SOL_USDT", Candle1m.t == 120_000))).scalars().one()
            assert float(c.c) == 1.7
    finally:
        await engine.dispose()


async def test_upsert_candle_rows_chunks_large_batches() -> None:
    engine, session_factory = await _make_sessionmaker()
    try:
        rows = [
            {
                "symbol": "SOL_USDT",
                "t": i * 60_000,
                "o": Decimal("1"),
                "h": Decimal("2"),
                "l": Decimal("0.5"),
                "c": Decimal("1.5"),
                "v": Decimal("10"),
            }
            for i in range(1, 400)
        ]

        async with session_factory() as session:
            await upsert_candle_rows(session, rows)
            await session.commit()

        async with session_factory() as session:
            count = (
                await session.execute(sa.select(sa.func.count()).select_from(Candle1m).where(Candle1m.symbol == "SOL_USDT"))
            ).scalar_one()
            assert int(count) == len(rows)
    finally:
        await engine.dispose()


async def test_ingest_latest_candles_once_reads_universe_and_upserts() -> None:
    engine, session_factory = await _make_sessionmaker()

    class FakeExchange:
        async def get_candlestick(self, instrument_name: str, timeframe: str = "1m", count: int = 2):
            assert instrument_name == "SOL_USDT"
            assert timeframe == "1m"
            assert count == 2
            return [
                {"t": 60_000, "o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10"},
                {"t": 120_000, "o": "1.5", "h": "1.6", "l": "1.4", "c": "1.55", "v": "5"},
            ]

    try:
        async with session_factory() as session:
            session.add(
                UniverseMembership(
                    symbol="SOL_USDT",
                    quote_ccy="USDT",
                    is_active=True,
                    is_baseline=False,
                    liquidity_rank=1,
                    dollar_vol_24h=Decimal("12345"),
                    updated_ts=now_ms(),
                )
            )
            await session.commit()

        store = CandleStore(buffer_size=10)
        settings = Settings(quote_ccy="USDT", candle_timeframe="1m", candle_poll_count=2, database_url="sqlite+aiosqlite:///:memory:")

        await ingest_latest_candles_once(
            session_factory=session_factory,
            exchange=FakeExchange(),  # type: ignore[arg-type]
            settings=settings,
            store=store,
        )
        await ingest_latest_candles_once(
            session_factory=session_factory,
            exchange=FakeExchange(),  # type: ignore[arg-type]
            settings=settings,
            store=store,
        )

        async with session_factory() as session:
            count = (
                await session.execute(
                    sa.select(sa.func.count()).select_from(Candle1m).where(Candle1m.symbol == "SOL_USDT")
                )
            ).scalar_one()
            assert int(count) == 2

        latest = store.latest("SOL_USDT")
        assert latest is not None
        assert latest.t == 120_000
    finally:
        await engine.dispose()
