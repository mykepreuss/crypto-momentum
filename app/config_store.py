from __future__ import annotations

from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import ConfigKV
from app.time_utils import now_ms

SETTINGS_OVERRIDES_KEY = "settings_overrides_v1"


def _insert_for_dialect(dialect_name: str):
    if dialect_name == "postgresql":
        return pg_insert
    if dialect_name == "sqlite":
        return sqlite_insert
    return sa.insert


async def load_settings_overrides(session: AsyncSession) -> tuple[dict[str, Any], Optional[int]]:
    row = await session.get(ConfigKV, SETTINGS_OVERRIDES_KEY)
    if row is None:
        return {}, None
    value = row.value_json if isinstance(row.value_json, dict) else {}
    updated_ts = int(row.updated_ts) if row.updated_ts is not None else None
    return value, updated_ts


async def save_settings_overrides(session: AsyncSession, overrides: dict[str, Any]) -> int:
    updated_ts = now_ms()
    bind = session.get_bind()
    insert_fn = _insert_for_dialect(bind.dialect.name)

    stmt = insert_fn(ConfigKV.__table__).values(
        {
            "key": SETTINGS_OVERRIDES_KEY,
            "value_json": overrides,
            "updated_ts": updated_ts,
        }
    )
    if hasattr(stmt, "on_conflict_do_update"):
        stmt = stmt.on_conflict_do_update(
            index_elements=["key"],
            set_={
                "value_json": stmt.excluded.value_json,
                "updated_ts": stmt.excluded.updated_ts,
            },
        )
    await session.execute(stmt)
    return updated_ts


def apply_overrides(base: Settings, overrides: dict[str, Any]) -> Settings:
    merged = {**base.model_dump(), **overrides}
    return Settings.model_validate(merged)

