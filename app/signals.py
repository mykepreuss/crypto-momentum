from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.candles import CandleStore
from app.config import Settings
from app.exchange_client import CryptoComExchangeClient
from app.features import FeatureSet, compute_features
from app.models import UniverseMembership
from app.scoring import ScoreComponents, compute_score, dv_term_from_dvz, percentile_ranks
from app.time_utils import now_ms

logger = logging.getLogger(__name__)


def _compute_spread(book: dict[str, Any]) -> Optional[float]:
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


async def _load_universe(
    session: AsyncSession,
    quote_ccy: str,
) -> tuple[list[str], Optional[str]]:
    rows = (
        await session.execute(
            select(UniverseMembership.symbol, UniverseMembership.is_baseline)
            .where(UniverseMembership.quote_ccy == quote_ccy, UniverseMembership.is_active.is_(True))
            .order_by(UniverseMembership.liquidity_rank.asc())
        )
    ).all()

    symbols = [str(r[0]) for r in rows]
    baseline = next((str(r[0]) for r in rows if bool(r[1])), None)
    return symbols, baseline


def _feature_dict(fs: FeatureSet, score: float, spread: Optional[float]) -> dict[str, Any]:
    return {
        "t0": fs.t0,
        "price": fs.price,
        "score": score,
        "rel_r15": fs.rel_r15,
        "rel_r5": fs.rel_r5,
        "accel": fs.accel,
        "dv_z": fs.dv_z,
        "avg_dv_1m": fs.avg_dv_1m,
        "extension": fs.extension,
        "breakout": fs.breakout,
        "trend_ok": fs.trend_ok,
        "spread": spread,
    }


async def compute_latest_signals(
    session_factory: async_sessionmaker[AsyncSession],
    exchange: CryptoComExchangeClient,
    settings: Settings,
    store: CandleStore,
) -> dict[str, Any]:
    ts = now_ms()

    async with session_factory() as session:
        symbols, baseline_symbol = await _load_universe(session, settings.quote_ccy)

    if not baseline_symbol:
        return {
            "ts": ts,
            "quote_ccy": settings.quote_ccy,
            "baseline_symbol": None,
            "error": "Baseline symbol not found (run /universe/refresh).",
            "signals": [],
        }

    baseline_candles = store.candles(baseline_symbol)
    if not baseline_candles:
        return {
            "ts": ts,
            "quote_ccy": settings.quote_ccy,
            "baseline_symbol": baseline_symbol,
            "error": "Baseline candles not available yet (waiting for backfill/ingest).",
            "signals": [],
        }

    candidate_symbols = [s for s in symbols if s != baseline_symbol]

    features: dict[str, FeatureSet] = {}
    for sym in candidate_symbols:
        fs = compute_features(store.candles(sym), baseline_candles)
        if fs is not None:
            features[sym] = fs

    if not features:
        return {
            "ts": ts,
            "quote_ccy": settings.quote_ccy,
            "baseline_symbol": baseline_symbol,
            "error": "No symbols have enough candle history yet.",
            "signals": [],
        }

    ranks_rel_r15 = percentile_ranks({sym: fs.rel_r15 for sym, fs in features.items()})
    ranks_accel = percentile_ranks({sym: fs.accel for sym, fs in features.items()})

    rows: list[dict[str, Any]] = []
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

        rows.append(
            {
                "symbol": sym,
                "score": score,
                "rank_rel_r15": components.rank_rel_r15,
                "rank_accel": components.rank_accel,
                "hard_gates": hard_gates,
                "passes_hard_gates": passes_hard,
                "spread": None,
                "passes_spread_gate": None,
                "features": _feature_dict(fs, score=score, spread=None),
            }
        )

    rows.sort(key=lambda r: r["score"], reverse=True)

    # Microstructure gate: check top N by score among hard-gated candidates only.
    book_candidates = [r for r in rows if r["passes_hard_gates"]][: settings.book_check_top_n]

    async def fetch_spread(symbol: str):
        book = await exchange.get_book(symbol, depth=10)
        return symbol, _compute_spread(book)

    spreads: dict[str, Optional[float]] = {}
    if book_candidates:
        results = await asyncio.gather(
            *(fetch_spread(r["symbol"]) for r in book_candidates),
            return_exceptions=True,
        )
        for r, res in zip(book_candidates, results):
            sym = r["symbol"]
            if isinstance(res, Exception):
                logger.error("order book fetch failed", extra={"symbol": sym, "error": repr(res)})
                spreads[sym] = None
            else:
                _sym, spread = res
                spreads[sym] = spread

    for r in book_candidates:
        sym = r["symbol"]
        spread = spreads.get(sym)
        r["spread"] = spread
        r["passes_spread_gate"] = spread is not None and spread <= settings.spread_max
        r["features"]["spread"] = spread

    return {
        "ts": ts,
        "quote_ccy": settings.quote_ccy,
        "baseline_symbol": baseline_symbol,
        "computed_symbols": len(features),
        "signals": rows,
    }
