#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Optional

from sqlalchemy import func, select

from app.candles import candle_rows, parse_candles, upsert_candle_rows_table
from app.config import get_settings
from app.db import create_engine, create_sessionmaker, session_scope
from app.exchange_client import CryptoComExchangeClient
from app.models import Candle1mHist, UniverseMembership
from app.time_utils import floor_minute_ms, now_ms


MINUTE_MS = 60_000
DAY_MS = 24 * 60 * MINUTE_MS
MAX_CANDLESTICK_COUNT = 300  # Crypto.com returns up to 300 even if a higher count is requested.

logger = logging.getLogger("backfill_hist_candles")


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


async def _min_t_existing(*, session, symbol: str) -> Optional[int]:
    return (
        await session.execute(select(func.min(Candle1mHist.t)).where(Candle1mHist.symbol == symbol))
    ).scalar_one_or_none()


async def backfill_symbol(
    *,
    session_factory,
    client: CryptoComExchangeClient,
    symbol: str,
    start_t0: int,
    end_t0: int,
    commit_every_rows: int,
) -> dict[str, int]:
    # Use end_ts slightly before the next candle boundary to avoid pulling an in-progress candle.
    cursor_end_ts = end_t0 + MINUTE_MS - 1

    total_rows = 0
    requests = 0

    async with session_scope(session_factory) as session:
        existing_min = await _min_t_existing(session=session, symbol=symbol)
        if existing_min is not None and int(existing_min) <= start_t0:
            logger.info("already_backfilled", extra={"symbol": symbol, "min_t": int(existing_min)})
            return {"rows": 0, "requests": 0}

        # If we already have some history for this symbol, continue extending backwards from the
        # oldest known candle rather than refetching the whole window.
        if existing_min is not None:
            cursor_end_ts = int(existing_min) - 1

        buffered_rows: list[dict[str, object]] = []

        while cursor_end_ts >= start_t0:
            raw = await client.get_candlestick(
                instrument_name=symbol,
                timeframe="1m",
                count=MAX_CANDLESTICK_COUNT,
                start_ts=start_t0,
                end_ts=cursor_end_ts,
            )
            requests += 1

            candles = parse_candles(raw)
            if not candles:
                break

            # Keep only the requested window.
            candles = [c for c in candles if start_t0 <= c.t <= end_t0]
            if not candles:
                break

            buffered_rows.extend(candle_rows(symbol, candles))
            total_rows += len(candles)

            if len(buffered_rows) >= commit_every_rows:
                await upsert_candle_rows_table(session, Candle1mHist.__table__, buffered_rows)
                await session.commit()
                buffered_rows = []

            earliest_t = candles[0].t
            if earliest_t <= start_t0:
                break
            next_cursor = earliest_t - 1
            if next_cursor >= cursor_end_ts:
                # Safety valve: ensure we always make progress backwards.
                break
            cursor_end_ts = next_cursor

        if buffered_rows:
            await upsert_candle_rows_table(session, Candle1mHist.__table__, buffered_rows)
            await session.commit()

    return {"rows": total_rows, "requests": requests}


async def _main_async(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="scripts/backfill_hist_candles.py",
        description="Backfill 1m candles into candles_1m_hist for long lookback evaluations (no pruning).",
    )
    parser.add_argument("--days", type=int, default=365, help="How many days of history to backfill (default: 365)")
    parser.add_argument(
        "--symbols",
        type=str,
        default="BTC_USDT,ETH_USDT",
        help="Comma-separated symbols to backfill (default: BTC_USDT,ETH_USDT)",
    )
    parser.add_argument(
        "--from-universe",
        action="store_true",
        help="Backfill the current active universe from DB (overrides --symbols).",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Limit symbols when using --from-universe (useful to avoid massive backfills).",
    )
    parser.add_argument(
        "--commit-every-rows",
        type=int,
        default=5_000,
        help="Upsert+commit after this many rows buffered (default: 5000)",
    )
    args = parser.parse_args(argv)

    if args.days < 1:
        raise SystemExit("--days must be >= 1")
    if args.commit_every_rows < 300:
        raise SystemExit("--commit-every-rows must be >= 300")

    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_sessionmaker(engine)
    client = CryptoComExchangeClient.from_settings(settings)

    try:
        # Use last fully closed 1m candle as the end.
        end_t0 = floor_minute_ms(now_ms()) - MINUTE_MS
        start_t0 = end_t0 - (args.days * DAY_MS)

        async with session_scope(session_factory) as session:
            if args.from_universe:
                symbols = await _load_universe_symbols(
                    session=session,
                    quote_ccy=settings.quote_ccy,
                    max_symbols=args.max_symbols,
                )
            else:
                symbols = _parse_symbols_csv(args.symbols)

        if not symbols:
            raise SystemExit("No symbols selected. Use --symbols or run /universe/refresh then use --from-universe.")

        logger.info(
            "backfill_start",
            extra={"symbols": len(symbols), "start_t0": start_t0, "end_t0": end_t0, "days": args.days},
        )

        total_rows = 0
        total_requests = 0
        for i, sym in enumerate(symbols, start=1):
            logger.info("symbol_start", extra={"symbol": sym, "i": i, "n": len(symbols)})
            res = await backfill_symbol(
                session_factory=session_factory,
                client=client,
                symbol=sym,
                start_t0=start_t0,
                end_t0=end_t0,
                commit_every_rows=int(args.commit_every_rows),
            )
            total_rows += int(res["rows"])
            total_requests += int(res["requests"])
            logger.info("symbol_done", extra={"symbol": sym, **res})

        logger.info("backfill_done", extra={"rows": total_rows, "requests": total_requests})
        return 0
    finally:
        await client.aclose()
        await engine.dispose()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return asyncio.run(_main_async(sys.argv[1:]))


if __name__ == "__main__":
    raise SystemExit(main())

