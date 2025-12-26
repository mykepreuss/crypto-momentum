from __future__ import annotations

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.config import Settings
from app.config_store import apply_overrides, load_settings_overrides, save_settings_overrides
from app.models import Base


async def _make_sessionmaker():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


async def test_apply_overrides_updates_runtime_settings() -> None:
    base = Settings(dvz_min=1.5, max_entry_alerts_24h=5)
    updated = apply_overrides(base, {"dvz_min": 2.25, "max_entry_alerts_24h": 3})
    assert updated.dvz_min == 2.25
    assert updated.max_entry_alerts_24h == 3


async def test_settings_overrides_round_trip() -> None:
    engine, session_factory = await _make_sessionmaker()
    try:
        async with session_factory() as session:
            ts1 = await save_settings_overrides(session, {"dvz_min": 2.5})
            await session.commit()

        async with session_factory() as session:
            overrides, updated_ts = await load_settings_overrides(session)
            assert overrides == {"dvz_min": 2.5}
            assert updated_ts == ts1

        async with session_factory() as session:
            ts2 = await save_settings_overrides(session, {"dvz_min": 3.0, "max_total_alerts_24h": 9})
            await session.commit()

        async with session_factory() as session:
            overrides, updated_ts = await load_settings_overrides(session)
            assert overrides == {"dvz_min": 3.0, "max_total_alerts_24h": 9}
            assert updated_ts == ts2
    finally:
        await engine.dispose()
