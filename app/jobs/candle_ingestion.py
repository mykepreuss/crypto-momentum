from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from typing import Awaitable, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.candles import CandleStore, candle_rows, filter_closed_candles, parse_candles, prune_old_candles, upsert_candle_rows
from app.config import Settings
from app.exchange_client import CryptoComExchangeClient
from app.models import UniverseMembership
from app.time_utils import floor_minute_ms, now_ms

logger = logging.getLogger(__name__)


def _chunked(items: list[dict[str, object]], size: int) -> list[list[dict[str, object]]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


async def _load_active_symbols(session: AsyncSession, quote_ccy: str) -> list[str]:
    rows = (
        await session.execute(
            select(UniverseMembership.symbol)
            .where(UniverseMembership.quote_ccy == quote_ccy, UniverseMembership.is_active.is_(True))
            .order_by(UniverseMembership.liquidity_rank.asc())
        )
    ).scalars().all()
    return [str(s) for s in rows]


def _sleep_seconds_to_next_minute(delay_s: float) -> float:
    # Align to minute boundaries with a small delay to let the last candle close and propagate.
    now = now_ms()
    next_minute_ms = floor_minute_ms(now) + 60_000
    target_ms = next_minute_ms + int(delay_s * 1000)
    return max(0.0, (target_ms - now) / 1000.0)


async def backfill_universe_candles(
    session_factory: async_sessionmaker[AsyncSession],
    exchange: CryptoComExchangeClient,
    settings: Settings,
    store: CandleStore,
) -> None:
    async with session_factory() as session:
        symbols = await _load_active_symbols(session, settings.quote_ccy)

    if not symbols:
        logger.warning("candle backfill skipped; universe is empty")
        return

    current_minute_start = floor_minute_ms(now_ms())

    async def fetch_one(symbol: str):
        raw = await exchange.get_candlestick(
            instrument_name=symbol,
            timeframe=settings.candle_timeframe,
            count=settings.candle_backfill_count,
        )
        candles = filter_closed_candles(parse_candles(raw), current_minute_start_ms=current_minute_start)
        return symbol, candles

    results = await asyncio.gather(*(fetch_one(s) for s in symbols), return_exceptions=True)

    rows: list[dict[str, object]] = []
    failures = 0
    for symbol, res in zip(symbols, results):
        if isinstance(res, Exception):
            failures += 1
            logger.error("candle backfill fetch failed", extra={"symbol": symbol, "error": repr(res)})
            continue
        s, candles = res
        store.upsert_many(s, candles)
        rows.extend(candle_rows(s, candles))

    async with session_factory() as session:
        try:
            for batch in _chunked(rows, settings.db_upsert_batch_size):
                await upsert_candle_rows(session, batch)
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    logger.info(
        "candle backfill complete",
        extra={"symbols": len(symbols), "failures": failures, "rows": len(rows)},
    )


async def ingest_latest_candles_once(
    session_factory: async_sessionmaker[AsyncSession],
    exchange: CryptoComExchangeClient,
    settings: Settings,
    store: CandleStore,
) -> dict[str, object]:
    async with session_factory() as session:
        symbols = await _load_active_symbols(session, settings.quote_ccy)

    if not symbols:
        logger.warning("candle ingest skipped; universe is empty")
        return {"symbols": 0, "failures": 0, "rows": 0, "max_t": None}

    current_minute_start = floor_minute_ms(now_ms())

    async def fetch_one(symbol: str):
        raw = await exchange.get_candlestick(
            instrument_name=symbol,
            timeframe=settings.candle_timeframe,
            count=settings.candle_poll_count,
        )
        candles = filter_closed_candles(parse_candles(raw), current_minute_start_ms=current_minute_start)
        return symbol, candles

    results = await asyncio.gather(*(fetch_one(s) for s in symbols), return_exceptions=True)

    rows: list[dict[str, object]] = []
    failures = 0
    for symbol, res in zip(symbols, results):
        if isinstance(res, Exception):
            failures += 1
            logger.error("candle ingest fetch failed", extra={"symbol": symbol, "error": repr(res)})
            continue
        s, candles = res
        store.upsert_many(s, candles)
        rows.extend(candle_rows(s, candles))

    async with session_factory() as session:
        try:
            await upsert_candle_rows(session, rows)
            await session.commit()
        except Exception:
            await session.rollback()
            raise

    max_t = max((int(r.get("t")) for r in rows if "t" in r), default=None)
    logger.info(
        "candle ingest tick",
        extra={"symbols": len(symbols), "failures": failures, "rows": len(rows)},
    )
    return {"symbols": len(symbols), "failures": failures, "rows": len(rows), "max_t": max_t}


async def run_candle_ingestion_service(
    session_factory: async_sessionmaker[AsyncSession],
    exchange: CryptoComExchangeClient,
    settings_provider: Callable[[], Settings],
    store: CandleStore,
    startup_universe_task: Optional[asyncio.Task] = None,
    on_after_ingest: Optional[Callable[[], Awaitable[None]]] = None,
    on_ingest_result: Optional[Callable[[dict[str, object]], None]] = None,
) -> None:
    # Wait for initial universe selection to populate `universe_membership`.
    if startup_universe_task is not None:
        try:
            await startup_universe_task
        except Exception:
            logger.exception("startup universe task failed; continuing (will retry universe reads)")

    try:
        settings = settings_provider()
        await backfill_universe_candles(
            session_factory=session_factory,
            exchange=exchange,
            settings=settings,
            store=store,
        )
    except Exception:
        logger.exception("candle backfill failed; continuing with live ingestion")

    last_prune_ms: Optional[int] = None

    after_task: Optional[asyncio.Task] = None
    pending_after = False
    try:
        while True:
            settings = settings_provider()
            await asyncio.sleep(_sleep_seconds_to_next_minute(settings.candle_fetch_delay_s))

            try:
                ingest_result = await ingest_latest_candles_once(
                    session_factory=session_factory,
                    exchange=exchange,
                    settings=settings,
                    store=store,
                )
                if on_ingest_result is not None:
                    try:
                        on_ingest_result(ingest_result)
                    except Exception:
                        logger.exception("on_ingest_result hook failed")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("candle ingest tick failed")

            if on_after_ingest is not None:
                if after_task is None or after_task.done():

                    pending_after = False

                    async def _run_after_loop() -> None:
                        nonlocal pending_after
                        while True:
                            try:
                                await on_after_ingest()
                            except asyncio.CancelledError:
                                raise
                            except Exception:
                                logger.exception("on_after_ingest hook failed")

                            if not pending_after:
                                break
                            pending_after = False

                    after_task = asyncio.create_task(_run_after_loop())
                else:
                    # Avoid overlapping scans/alerts, but don't silently fall behind: coalesce
                    # missed ticks into a single extra run as soon as the current one completes.
                    pending_after = True
                    logger.warning("on_after_ingest still running; will coalesce another run after completion")

            now = now_ms()
            prune_interval_ms = int(settings.candle_prune_interval_hours * 3600 * 1000)
            retention_ms = int(settings.candle_retention_days * 24 * 3600 * 1000)
            if last_prune_ms is None or (now - last_prune_ms) >= prune_interval_ms:
                cutoff = floor_minute_ms(now) - retention_ms
                async with session_factory() as session:
                    try:
                        deleted = await prune_old_candles(session, cutoff_ts_ms=cutoff)
                        await session.commit()
                        logger.info(
                            "candle prune complete",
                            extra={
                                "retention_days": settings.candle_retention_days,
                                "cutoff_ts_ms": cutoff,
                                "deleted": deleted,
                            },
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        await session.rollback()
                        logger.exception("candle prune failed")
                last_prune_ms = now
    finally:
        if after_task is not None and not after_task.done():
            after_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await after_task
