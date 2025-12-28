from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.exchange_client import CryptoComExchangeClient
from app.models import Instrument, Ticker24h, UniverseMembership
from app.time_utils import now_ms

logger = logging.getLogger(__name__)


def _to_decimal(v: object) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _extract_str(d: dict[str, Any], keys: list[str]) -> Optional[str]:
    for k in keys:
        if k in d and d[k] is not None:
            s = str(d[k]).strip()
            if s:
                return s
    return None


def _extract_bool(d: dict[str, Any], keys: list[str]) -> bool:
    for k in keys:
        if k in d:
            v = d[k]
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return bool(v)
            if isinstance(v, str):
                vv = v.strip().lower()
                if vv in ("true", "1", "yes", "y"):
                    return True
                if vv in ("false", "0", "no", "n"):
                    return False
    return False


@dataclass(frozen=True)
class ParsedInstrument:
    symbol: str
    inst_type: Optional[str]
    base_ccy: Optional[str]
    quote_ccy: Optional[str]
    tradable: bool
    display_name: Optional[str]


@dataclass(frozen=True)
class ParsedTicker:
    symbol: str
    last_price: Decimal
    vol_24h_base: Decimal

    @property
    def dollar_vol_24h(self) -> Decimal:
        return self.last_price * self.vol_24h_base


@dataclass(frozen=True)
class UniverseMember:
    symbol: str
    dollar_vol_24h: Decimal
    liquidity_rank: int
    is_baseline: bool


@dataclass(frozen=True)
class UniverseSelection:
    quote_ccy: str
    baseline_symbol: str
    members: list[UniverseMember]
    updated_ts: int


def parse_instruments(raw: list[dict[str, Any]]) -> list[ParsedInstrument]:
    out: list[ParsedInstrument] = []
    for item in raw:
        symbol = _extract_str(item, ["symbol", "instrument_name", "i"])
        if not symbol:
            continue
        base_ccy = _extract_str(item, ["base_ccy", "base_currency", "base_ccy_name"])
        quote_ccy = _extract_str(item, ["quote_ccy", "quote_currency", "quote_ccy_name"])
        if (base_ccy is None or quote_ccy is None) and "_" in symbol:
            parts = symbol.split("_", 1)
            if len(parts) == 2:
                base_ccy = base_ccy or parts[0]
                quote_ccy = quote_ccy or parts[1]

        out.append(
            ParsedInstrument(
                symbol=symbol,
                inst_type=_extract_str(item, ["inst_type", "instrument_type", "type"]),
                base_ccy=base_ccy,
                quote_ccy=quote_ccy,
                tradable=_extract_bool(item, ["tradable"]),
                display_name=_extract_str(item, ["display_name", "displayName", "name"]),
            )
        )
    return out


def parse_tickers(raw: list[dict[str, Any]]) -> dict[str, ParsedTicker]:
    out: dict[str, ParsedTicker] = {}
    for item in raw:
        symbol = _extract_str(item, ["symbol", "instrument_name", "i"])
        if not symbol:
            continue

        last_price = _to_decimal(item.get("a") if "a" in item else item.get("last_price"))
        vol_24h_base = _to_decimal(item.get("v") if "v" in item else item.get("vol_24h_base"))
        if last_price is None or vol_24h_base is None:
            continue
        out[symbol] = ParsedTicker(symbol=symbol, last_price=last_price, vol_24h_base=vol_24h_base)
    return out


def _is_spot_like_symbol(symbol: str) -> bool:
    if "_" not in symbol:
        return False
    if "-" in symbol:
        return False
    if "-PERP" in symbol.upper():
        return False
    return True


def _parse_csv_set(s: Optional[str]) -> set[str]:
    if s is None:
        return set()
    parts = [p.strip().upper() for p in str(s).split(",")]
    return {p for p in parts if p}


def select_universe(
    instruments: list[ParsedInstrument],
    tickers: dict[str, ParsedTicker],
    quote_ccy: str,
    max_size: int,
    updated_ts: int,
    *,
    exclude_base_ccy: Optional[set[str]] = None,
    exclude_symbols: Optional[set[str]] = None,
) -> UniverseSelection:
    exclude_base_ccy = exclude_base_ccy or set()
    exclude_symbols = exclude_symbols or set()

    candidates: list[tuple[str, Decimal, str]] = []
    for inst in instruments:
        if not inst.tradable:
            continue
        if inst.quote_ccy != quote_ccy:
            continue
        if not _is_spot_like_symbol(inst.symbol):
            continue
        if inst.symbol.upper() in exclude_symbols:
            continue
        t = tickers.get(inst.symbol)
        if t is None:
            continue
        base = inst.base_ccy or inst.symbol.split("_", 1)[0]
        if base.strip().upper() in exclude_base_ccy:
            continue
        candidates.append((inst.symbol, t.dollar_vol_24h, base))

    if not candidates:
        raise ValueError(f"no universe candidates for quote_ccy={quote_ccy}")

    candidates.sort(key=lambda x: x[1], reverse=True)

    # Baseline: highest dollar-volume BTC pair in the chosen quote currency.
    btc_candidates = [c for c in candidates if c[2].upper() == "BTC" or c[0].upper().startswith("BTC_")]
    baseline_symbol = (btc_candidates[0][0] if btc_candidates else candidates[0][0])

    top = candidates[:max_size]
    selected_symbols = {s for (s, _, _) in top}
    if baseline_symbol not in selected_symbols:
        top.append(next(c for c in candidates if c[0] == baseline_symbol))
        top.sort(key=lambda x: x[1], reverse=True)

    members: list[UniverseMember] = []
    for i, (symbol, dv, _base) in enumerate(top, start=1):
        members.append(
            UniverseMember(
                symbol=symbol,
                dollar_vol_24h=dv,
                liquidity_rank=i,
                is_baseline=(symbol == baseline_symbol),
            )
        )

    return UniverseSelection(
        quote_ccy=quote_ccy,
        baseline_symbol=baseline_symbol,
        members=members,
        updated_ts=updated_ts,
    )


def _insert_for_dialect(dialect_name: str):
    if dialect_name == "postgresql":
        return pg_insert
    if dialect_name == "sqlite":
        return sqlite_insert
    return sa.insert


async def persist_market_data(
    session: AsyncSession,
    instruments: list[ParsedInstrument],
    tickers: dict[str, ParsedTicker],
    quote_ccy: str,
    updated_ts: int,
) -> None:
    bind = session.get_bind()
    dialect_name = bind.dialect.name
    insert_fn = _insert_for_dialect(dialect_name)

    instrument_rows = [
        {
            "symbol": inst.symbol,
            "inst_type": inst.inst_type,
            "base_ccy": inst.base_ccy,
            "quote_ccy": inst.quote_ccy,
            "tradable": inst.tradable,
            "display_name": inst.display_name,
            "last_seen_ts": updated_ts,
        }
        for inst in instruments
    ]
    if instrument_rows:
        stmt = insert_fn(Instrument.__table__).values(instrument_rows)
        if hasattr(stmt, "on_conflict_do_update"):
            stmt = stmt.on_conflict_do_update(
                index_elements=["symbol"],
                set_={
                    "inst_type": stmt.excluded.inst_type,
                    "base_ccy": stmt.excluded.base_ccy,
                    "quote_ccy": stmt.excluded.quote_ccy,
                    "tradable": stmt.excluded.tradable,
                    "display_name": stmt.excluded.display_name,
                    "last_seen_ts": stmt.excluded.last_seen_ts,
                },
            )
        await session.execute(stmt)

    # Persist tickers only for this quote currency's instruments.
    relevant_symbols = {inst.symbol for inst in instruments if inst.quote_ccy == quote_ccy}
    ticker_rows = [
        {
            "symbol": t.symbol,
            "last_price": t.last_price,
            "vol_24h_base": t.vol_24h_base,
            "updated_ts": updated_ts,
        }
        for t in tickers.values()
        if t.symbol in relevant_symbols
    ]
    if ticker_rows:
        stmt = insert_fn(Ticker24h.__table__).values(ticker_rows)
        if hasattr(stmt, "on_conflict_do_update"):
            stmt = stmt.on_conflict_do_update(
                index_elements=["symbol"],
                set_={
                    "last_price": stmt.excluded.last_price,
                    "vol_24h_base": stmt.excluded.vol_24h_base,
                    "updated_ts": stmt.excluded.updated_ts,
                },
            )
        await session.execute(stmt)


async def persist_universe(
    session: AsyncSession,
    selection: UniverseSelection,
) -> None:
    bind = session.get_bind()
    dialect_name = bind.dialect.name
    insert_fn = _insert_for_dialect(dialect_name)

    # Mark existing memberships inactive for the quote currency.
    await session.execute(
        sa.update(UniverseMembership)
        .where(UniverseMembership.quote_ccy == selection.quote_ccy)
        .values(is_active=False, is_baseline=False, updated_ts=selection.updated_ts)
    )

    rows = [
        {
            "symbol": m.symbol,
            "quote_ccy": selection.quote_ccy,
            "is_active": True,
            "is_baseline": m.is_baseline,
            "liquidity_rank": m.liquidity_rank,
            "dollar_vol_24h": m.dollar_vol_24h,
            "updated_ts": selection.updated_ts,
        }
        for m in selection.members
    ]
    stmt = insert_fn(UniverseMembership.__table__).values(rows)
    if hasattr(stmt, "on_conflict_do_update"):
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol"],
            set_={
                "quote_ccy": stmt.excluded.quote_ccy,
                "is_active": stmt.excluded.is_active,
                "is_baseline": stmt.excluded.is_baseline,
                "liquidity_rank": stmt.excluded.liquidity_rank,
                "dollar_vol_24h": stmt.excluded.dollar_vol_24h,
                "updated_ts": stmt.excluded.updated_ts,
            },
        )
    await session.execute(stmt)


async def refresh_universe(
    session: AsyncSession,
    client: CryptoComExchangeClient,
    settings: Settings,
) -> UniverseSelection:
    updated_ts = now_ms()

    raw_instruments = await client.get_instruments()
    raw_tickers = await client.get_tickers()

    instruments = parse_instruments(raw_instruments)
    tickers = parse_tickers(raw_tickers)

    selection = select_universe(
        instruments=instruments,
        tickers=tickers,
        quote_ccy=settings.quote_ccy,
        max_size=settings.max_universe_size,
        updated_ts=updated_ts,
        exclude_base_ccy=_parse_csv_set(settings.universe_exclude_base_ccy),
        exclude_symbols=_parse_csv_set(settings.universe_exclude_symbols),
    )

    await persist_market_data(
        session=session,
        instruments=instruments,
        tickers=tickers,
        quote_ccy=settings.quote_ccy,
        updated_ts=updated_ts,
    )
    await persist_universe(session=session, selection=selection)

    logger.info(
        "universe refreshed",
        extra={
            "quote_ccy": selection.quote_ccy,
            "baseline_symbol": selection.baseline_symbol,
            "members": len(selection.members),
        },
    )
    return selection
