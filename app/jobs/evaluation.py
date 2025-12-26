from __future__ import annotations

import asyncio
import logging
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.alerting import ALERT_TYPE_MOMENTUM_UP
from app.candles import Candle
from app.evaluation import MINUTE_MS, compute_entry_evaluation
from app.models import Alert, AlertEvaluation, Candle1m
from app.time_utils import now_ms

logger = logging.getLogger(__name__)


def _insert_for_dialect(dialect_name: str):
    if dialect_name == "postgresql":
        return pg_insert
    if dialect_name == "sqlite":
        return sqlite_insert
    return sa.insert


def _as_int(v: object) -> Optional[int]:
    try:
        return int(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


async def compute_entry_evaluations_once(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    batch_size: int = 200,
) -> dict[str, int]:
    """
    Find MOMENTUM_UP alerts without an evaluation row and compute metrics once
    enough future candles exist.
    """
    now = now_ms()
    skipped = 0

    async with session_factory() as session:
        alerts = (
            await session.execute(
                select(Alert)
                .outerjoin(AlertEvaluation, AlertEvaluation.alert_id == Alert.id)
                .where(
                    Alert.alert_type == ALERT_TYPE_MOMENTUM_UP,
                    AlertEvaluation.alert_id.is_(None),
                )
                .order_by(Alert.ts.asc())
                .limit(batch_size)
            )
        ).scalars().all()

        evaluation_rows: list[dict[str, object]] = []

        for alert in alerts:
            features = alert.features_json if isinstance(alert.features_json, dict) else {}
            entry_t0 = _as_int(features.get("t0"))
            if entry_t0 is None:
                skipped += 1
                continue

            end_t = entry_t0 + 60 * MINUTE_MS
            # Quick time-based guard to avoid DB reads before the future window can exist.
            if now < (end_t + MINUTE_MS):
                skipped += 1
                continue

            candle_rows = (
                await session.execute(
                    select(Candle1m)
                    .where(
                        Candle1m.symbol == alert.symbol,
                        Candle1m.t >= entry_t0,
                        Candle1m.t <= end_t,
                    )
                    .order_by(Candle1m.t.asc())
                )
            ).scalars().all()

            candles = [Candle(t=int(c.t), o=c.o, h=c.h, l=c.l, c=c.c, v=c.v) for c in candle_rows]
            metrics = compute_entry_evaluation(entry_t0=entry_t0, candles_1m=candles)
            if metrics is None:
                skipped += 1
                continue

            evaluation_rows.append(
                {
                    "alert_id": alert.id,
                    "r_5m": metrics.r_5m,
                    "r_15m": metrics.r_15m,
                    "r_60m": metrics.r_60m,
                    "mae_60m": metrics.mae_60m,
                    "mfe_60m": metrics.mfe_60m,
                    "computed_ts": now,
                }
            )

        inserted = 0
        if evaluation_rows:
            bind = session.get_bind()
            insert_fn = _insert_for_dialect(bind.dialect.name)
            stmt = insert_fn(AlertEvaluation.__table__).values(evaluation_rows)
            if hasattr(stmt, "on_conflict_do_nothing"):
                stmt = stmt.on_conflict_do_nothing(index_elements=["alert_id"])
            result = await session.execute(stmt)
            inserted = int(getattr(result, "rowcount", 0) or 0)

        await session.commit()

    return {"computed": inserted, "skipped": skipped}


async def run_evaluation_service(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    interval_s: float = 60.0,
) -> None:
    while True:
        try:
            res = await compute_entry_evaluations_once(session_factory=session_factory)
            if res["computed"]:
                logger.info("evaluation tick", extra=res)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("evaluation tick failed")

        await asyncio.sleep(max(1.0, float(interval_s)))
