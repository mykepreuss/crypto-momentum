#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from decimal import Decimal
from math import ceil
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import select

from app.bars import BarAggregator, MINUTE_MS
from app.candles import upsert_candle_rows_table
from app.config import get_settings
from app.db import create_engine, create_sessionmaker, session_scope
from app.models import Candle1mHist, CandleHistBar, UniverseMembership
from app.time_utils import floor_minute_ms, now_ms

DAY_MS = 24 * 60 * MINUTE_MS

logger = logging.getLogger("build_hist_bars")


def _parse_symbols_csv(s: str) -> list[str]:
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


async def _load_universe_symbols(*, session, quote_ccy: str, max_symbols: Optional[int]) -> list[str]:
    rows = (
        await session.execute(
            select(UniverseMembership.symbol)
            .where(UniverseMembership.quote_ccy == quote_ccy, UniverseMembership.is_active.is_(True))
            .order_by(UniverseMembership.liquidity_rank.asc())
        )
    ).scalars().all()
    symbols = [str(s) for s in rows]
    if max_symbols is not None:
        symbols = symbols[: max(0, int(max_symbols))]
    return symbols


async def build_bars_for_symbol(
    *,
    session,
    symbol: str,
    timeframe_min: int,
    ingest_start_t0: int,
    ingest_end_t0: int,
    store_start_t0: int,
    store_end_t0: int,
    commit_every_bars: int,
    replace: bool,
) -> dict[str, int]:
    agg = BarAggregator(bar_minutes=timeframe_min)
    bar_ms = int(timeframe_min) * MINUTE_MS

    buffered_rows: list[dict[str, object]] = []
    bars = 0
    skipped = 0

    if replace:
        await session.execute(
            sa.delete(CandleHistBar).where(
                CandleHistBar.timeframe_min == int(timeframe_min),
                CandleHistBar.symbol == symbol,
            )
        )
        await session.commit()

    stmt = (
        select(
            Candle1mHist.t,
            Candle1mHist.o,
            Candle1mHist.h,
            Candle1mHist.l,
            Candle1mHist.c,
            Candle1mHist.v,
        )
        .where(
            Candle1mHist.symbol == symbol,
            Candle1mHist.t >= int(ingest_start_t0),
            Candle1mHist.t <= int(ingest_end_t0),
        )
        .order_by(Candle1mHist.t.asc())
    )

    result = await session.stream(stmt)
    async for row in result:
        t, o, h, l, c, v = row
        out = agg.ingest_1m(
            t=int(t),
            o=Decimal(o),
            h=Decimal(h),
            l=Decimal(l),
            c=Decimal(c),
            v=Decimal(v),
        )
        if out is not None:
            # IMPORTANT: store bars at BAR CLOSE time to avoid lookahead in replay.
            bar_t = int(out.t) + int(bar_ms)
            if int(bar_t) < int(store_start_t0) or int(bar_t) > int(store_end_t0):
                skipped += 1
                continue
            buffered_rows.append(
                {
                    "timeframe_min": int(timeframe_min),
                    "symbol": symbol,
                    "t": int(bar_t),
                    "o": out.o,
                    "h": out.h,
                    "l": out.l,
                    "c": out.c,
                    "v": out.v,
                }
            )
            bars += 1

            if len(buffered_rows) >= int(commit_every_bars):
                await upsert_candle_rows_table(
                    session,
                    CandleHistBar.__table__,
                    buffered_rows,
                    index_elements=["timeframe_min", "symbol", "t"],
                )
                await session.commit()
                buffered_rows = []

    last = agg.flush()
    if last is not None:
        bar_t = int(last.t) + int(bar_ms)
        if int(store_start_t0) <= int(bar_t) <= int(store_end_t0):
            buffered_rows.append(
                {
                    "timeframe_min": int(timeframe_min),
                    "symbol": symbol,
                    "t": int(bar_t),
                    "o": last.o,
                    "h": last.h,
                    "l": last.l,
                    "c": last.c,
                    "v": last.v,
                }
            )
            bars += 1

    if buffered_rows:
        await upsert_candle_rows_table(
            session,
            CandleHistBar.__table__,
            buffered_rows,
            index_elements=["timeframe_min", "symbol", "t"],
        )
        await session.commit()

    return {"bars": int(bars), "skipped": int(skipped)}


async def _main_async(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="scripts/build_hist_bars.py",
        description="Build aggregated historical bars from candles_1m_hist into candles_hist_bars (offline only).",
    )
    parser.add_argument("--days", type=int, default=365, help="How many days to include (default: 365)")
    parser.add_argument(
        "--bar-minutes",
        type=int,
        default=60,
        help="Bar size in minutes (default: 60). Example: 240 for 4h bars.",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default="BTC_USDT,ETH_USDT",
        help="Comma-separated symbols to build bars for (default: BTC_USDT,ETH_USDT)",
    )
    parser.add_argument(
        "--from-universe",
        action="store_true",
        help="Build bars for the current active universe from DB (overrides --symbols).",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Limit symbols when using --from-universe.",
    )
    parser.add_argument(
        "--commit-every-bars",
        type=int,
        default=2_000,
        help="Upsert+commit after this many bars buffered (default: 2000)",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete existing bars for the selected timeframe+symbols before rebuilding (recommended when changing bar params).",
    )
    args = parser.parse_args(argv)

    if args.days < 1:
        raise SystemExit("--days must be >= 1")
    if args.bar_minutes < 2:
        raise SystemExit("--bar-minutes must be >= 2 (use 1m candles directly otherwise)")
    if args.commit_every_bars < 100:
        raise SystemExit("--commit-every-bars must be >= 100")

    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_sessionmaker(engine)

    # Use last fully closed 1m candle as "now", then align to the last fully closed bar end.
    end_minute_t0 = floor_minute_ms(now_ms()) - MINUTE_MS
    bar_ms = int(args.bar_minutes) * MINUTE_MS
    end_bar_t0 = (int(end_minute_t0) // int(bar_ms)) * int(bar_ms)

    warmup_bars = 60  # dv/vwap window
    warmup_days = int(ceil((warmup_bars * int(args.bar_minutes)) / 1440.0))
    total_days = int(args.days) + int(warmup_days)

    store_start_t0 = int(end_bar_t0) - (int(total_days) * DAY_MS)
    store_end_t0 = int(end_bar_t0)

    ingest_start_t0 = int(store_start_t0) - int(bar_ms)
    ingest_end_t0 = int(store_end_t0) - MINUTE_MS

    async with session_scope(session_factory) as session:
        if args.from_universe:
            symbols = await _load_universe_symbols(session=session, quote_ccy=settings.quote_ccy, max_symbols=args.max_symbols)
        else:
            symbols = _parse_symbols_csv(args.symbols)

    if not symbols:
        raise SystemExit("No symbols selected. Use --symbols or run /universe/refresh then use --from-universe.")

    logger.info(
        "bars_build_start",
        extra={
            "timeframe_min": int(args.bar_minutes),
            "symbols": len(symbols),
            "ingest_start_t0": int(ingest_start_t0),
            "ingest_end_t0": int(ingest_end_t0),
            "store_start_t0": int(store_start_t0),
            "store_end_t0": int(store_end_t0),
            "days": int(args.days),
            "warmup_days": int(warmup_days),
            "replace": bool(args.replace),
        },
    )

    total_bars = 0
    total_skipped = 0
    async with session_scope(session_factory) as session:
        for i, sym in enumerate(symbols, start=1):
            logger.info("symbol_start", extra={"symbol": sym, "i": i, "n": len(symbols)})
            res = await build_bars_for_symbol(
                session=session,
                symbol=sym,
                timeframe_min=int(args.bar_minutes),
                ingest_start_t0=int(ingest_start_t0),
                ingest_end_t0=int(ingest_end_t0),
                store_start_t0=int(store_start_t0),
                store_end_t0=int(store_end_t0),
                commit_every_bars=int(args.commit_every_bars),
                replace=bool(args.replace),
            )
            total_bars += int(res["bars"])
            total_skipped += int(res["skipped"])
            logger.info("symbol_done", extra={"symbol": sym, **res})

    logger.info("bars_build_done", extra={"bars": total_bars, "skipped": total_skipped})
    await engine.dispose()
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return asyncio.run(_main_async(sys.argv[1:]))


if __name__ == "__main__":
    raise SystemExit(main())
