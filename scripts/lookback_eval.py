#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import bisect
import csv
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

from sqlalchemy import func, select

from app.config import get_settings
from app.db import create_engine, create_sessionmaker, session_scope
from app.lookback_eval import CostModel, max_drawdown, buy_with_costs, sell_with_costs
from app.models import Alert, Candle1m, Candle1mHist, CandleHistBar
from app.time_utils import floor_minute_ms, now_ms


MINUTE_MS = 60_000
DAY_MS = 24 * 60 * MINUTE_MS


def _decimal_equal(a: object, b: object) -> bool:
    try:
        return Decimal(str(a)) == Decimal(str(b))
    except (InvalidOperation, ValueError, TypeError):
        return False


async def _assert_hist_bars_close_timestamps(*, session, timeframe_min: int, symbol: str) -> None:
    """
    Guardrail: `candles_hist_bars.t` must represent BAR CLOSE time.

    Earlier versions stored bars at bar start while also using bar close prices, which introduced
    a lookahead in offline evaluations. This check detects that condition and tells the operator
    to rebuild with `scripts/build_hist_bars.py --replace`.
    """
    row = (
        await session.execute(
            select(CandleHistBar.t, CandleHistBar.c)
            .where(CandleHistBar.timeframe_min == int(timeframe_min), CandleHistBar.symbol == str(symbol))
            .order_by(CandleHistBar.t.desc())
            .limit(1)
        )
    ).first()
    if row is None:
        return
    bar_t, bar_c = row

    bar_ms = int(timeframe_min) * MINUTE_MS
    close_semantics_last_1m_t = int(bar_t) - MINUTE_MS
    start_semantics_last_1m_t = int(bar_t) + int(bar_ms) - MINUTE_MS

    c_close = (
        await session.execute(
            select(Candle1mHist.c).where(Candle1mHist.symbol == str(symbol), Candle1mHist.t == int(close_semantics_last_1m_t))
        )
    ).scalar_one_or_none()
    if c_close is not None and _decimal_equal(c_close, bar_c):
        return

    c_start = (
        await session.execute(
            select(Candle1mHist.c).where(Candle1mHist.symbol == str(symbol), Candle1mHist.t == int(start_semantics_last_1m_t))
        )
    ).scalar_one_or_none()
    if c_start is not None and _decimal_equal(c_start, bar_c):
        raise SystemExit(
            "candles_hist_bars appears to use BAR START timestamps (old behavior). "
            "Rebuild with: python scripts/build_hist_bars.py ... --replace (stores BAR CLOSE timestamps)."
        )


@dataclass(frozen=True)
class AlertEvent:
    id: str
    ts: int
    symbol: str
    t0: int
    price: float
    score: float


@dataclass(frozen=True)
class TradeRecord:
    symbol: str
    entry_alert_id: str
    entry_ts: int
    entry_t0: int
    exit_t0: int
    exit_reason: str
    entry_price: float
    exit_price: float
    duration_min: int
    gross_ret: float
    net_ret: float
    costs_fee: float
    costs_slippage: float
    equity_before: float
    equity_after: float
    # quantities for equity curve segments
    alt_qty: float
    btc_qty_after: float
    eth_qty_after: float


class PriceSeries:
    def __init__(self, points: dict[int, float]) -> None:
        self._points = points
        self._ts = sorted(points.keys())

    @property
    def first_t(self) -> Optional[int]:
        return self._ts[0] if self._ts else None

    @property
    def last_t(self) -> Optional[int]:
        return self._ts[-1] if self._ts else None

    def price_at_or_before(self, t: int) -> Optional[float]:
        if not self._ts:
            return None
        idx = bisect.bisect_right(self._ts, t) - 1
        if idx < 0:
            return None
        return self._points[self._ts[idx]]

    def point_at_or_before(self, t: int) -> Optional[tuple[int, float]]:
        if not self._ts:
            return None
        idx = bisect.bisect_right(self._ts, t) - 1
        if idx < 0:
            return None
        ts = self._ts[idx]
        return ts, self._points[ts]


def _safe_float(v) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def _alert_t0(alert: Alert) -> Optional[int]:
    if not isinstance(alert.features_json, dict):
        return None
    t0 = alert.features_json.get("t0")
    if t0 is None:
        return None
    try:
        return int(t0)
    except Exception:
        return None


def _alert_price(alert: Alert) -> Optional[float]:
    if not isinstance(alert.features_json, dict):
        return None
    return _safe_float(alert.features_json.get("price"))


def _summarize(values: list[float]) -> dict[str, Optional[float]]:
    if not values:
        return {"mean": None, "median": None, "p05": None, "p95": None}
    values_sorted = sorted(values)
    n = len(values_sorted)

    def pct(p: float) -> float:
        if n == 1:
            return values_sorted[0]
        k = int(round((p / 100.0) * (n - 1)))
        return values_sorted[max(0, min(k, n - 1))]

    return {
        "mean": float(statistics.mean(values_sorted)),
        "median": float(statistics.median(values_sorted)),
        "p05": float(pct(5)),
        "p95": float(pct(95)),
    }


async def _load_price_series(
    *,
    session,
    symbol: str,
    start_t0: int,
    end_t0: int,
    candles_model=Candle1m,
    timeframe_min: Optional[int] = None,
) -> PriceSeries:
    stmt = select(candles_model.t, candles_model.c).where(
        candles_model.symbol == symbol,
        candles_model.t >= start_t0,
        candles_model.t <= end_t0,
    )
    if candles_model is CandleHistBar:
        if timeframe_min is None:
            raise ValueError("timeframe_min is required for CandleHistBar")
        stmt = stmt.where(CandleHistBar.timeframe_min == int(timeframe_min))

    stmt = stmt.order_by(candles_model.t.asc())
    rows = (await session.execute(stmt)).all()
    points: dict[int, float] = {}
    for (t, c) in rows:
        points[int(t)] = float(c)
    return PriceSeries(points)


async def _load_trade_extremes(
    *,
    session,
    symbol: str,
    entry_t0: int,
    exit_t0: int,
    candles_model=Candle1m,
    timeframe_min: Optional[int] = None,
) -> tuple[Optional[float], Optional[float]]:
    stmt = select(func.max(candles_model.h), func.min(candles_model.l)).where(
        candles_model.symbol == symbol,
        candles_model.t >= int(entry_t0),
        candles_model.t <= int(exit_t0),
    )
    if candles_model is CandleHistBar:
        if timeframe_min is None:
            raise ValueError("timeframe_min is required for CandleHistBar")
        stmt = stmt.where(CandleHistBar.timeframe_min == int(timeframe_min))
    row = (await session.execute(stmt)).one()
    max_h, min_l = row
    return (
        float(max_h) if max_h is not None else None,
        float(min_l) if min_l is not None else None,
    )


async def _load_alerts(
    *,
    session,
    start_ts: int,
    end_ts: int,
) -> tuple[list[AlertEvent], dict[str, list[int]]]:
    stmt = (
        select(Alert)
        .where(
            Alert.ts >= start_ts,
            Alert.ts <= end_ts,
            Alert.alert_type.in_(["MOMENTUM_UP", "MOMENTUM_SLOWING"]),
        )
        .order_by(Alert.ts.asc())
    )
    rows = (await session.execute(stmt)).scalars().all()

    entries: list[AlertEvent] = []
    exits_by_symbol: dict[str, list[int]] = defaultdict(list)

    for a in rows:
        t0 = _alert_t0(a)
        price = _alert_price(a)
        if t0 is None or price is None:
            continue
        evt = AlertEvent(
            id=str(a.id),
            ts=int(a.ts),
            symbol=str(a.symbol),
            t0=int(t0),
            price=float(price),
            score=float(a.score),
        )
        if a.alert_type == "MOMENTUM_UP":
            entries.append(evt)
        elif a.alert_type == "MOMENTUM_SLOWING":
            exits_by_symbol[evt.symbol].append(evt.t0)

    for symbol in list(exits_by_symbol.keys()):
        exits_by_symbol[symbol].sort()

    entries.sort(key=lambda e: e.t0)
    return entries, exits_by_symbol


def _load_alerts_csv(path: Path) -> tuple[list[AlertEvent], dict[str, list[int]]]:
    entries: list[AlertEvent] = []
    exits_by_symbol: dict[str, list[int]] = defaultdict(list)

    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            alert_type = (row.get("alert_type") or "").strip()
            symbol = (row.get("symbol") or "").strip()
            if not symbol or alert_type not in ("MOMENTUM_UP", "MOMENTUM_SLOWING"):
                continue

            try:
                t0 = int(row.get("t0") or row.get("features_t0") or "")
            except Exception:
                continue

            price = _safe_float(row.get("price"))
            score = _safe_float(row.get("score"))
            if price is None or score is None:
                continue

            try:
                ts = int(row.get("ts") or t0)
            except Exception:
                ts = t0

            alert_id = (row.get("id") or "").strip() or f"csv:{symbol}:{t0}:{alert_type}"
            evt = AlertEvent(id=alert_id, ts=ts, symbol=symbol, t0=t0, price=float(price), score=float(score))

            if alert_type == "MOMENTUM_UP":
                entries.append(evt)
            else:
                exits_by_symbol[symbol].append(t0)

    for symbol in list(exits_by_symbol.keys()):
        exits_by_symbol[symbol].sort()
    entries.sort(key=lambda e: e.t0)
    return entries, exits_by_symbol


async def _main_async(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="scripts/lookback_eval.py",
        description="Lookback evaluation: trade MOMENTUM_UP -> MOMENTUM_SLOWING and compare vs BTC/ETH hold.",
    )
    parser.add_argument("--days", type=int, default=30, help="Lookback window in days (default: 30)")
    parser.add_argument("--initial-usd", type=float, default=500.0, help="Initial portfolio value in USD (default: 500)")
    parser.add_argument("--fee-bps", type=float, default=10.0, help="Fee per side in bps (default: 10)")
    parser.add_argument("--slippage-bps", type=float, default=5.0, help="Slippage per side in bps (default: 5)")
    parser.add_argument(
        "--do-cost-per-month",
        type=float,
        default=15.0,
        help="DigitalOcean cost per month in USD (default: 15)",
    )
    parser.add_argument(
        "--max-hold-min",
        type=int,
        default=360,
        help="If no exit alert, force exit after this many minutes (default: 360 = 6h)",
    )
    parser.add_argument(
        "--btc-symbol",
        type=str,
        default="BTC_USDT",
        help="BTC symbol used for baseline hold (default: BTC_USDT)",
    )
    parser.add_argument(
        "--eth-symbol",
        type=str,
        default="ETH_USDT",
        help="ETH symbol used for baseline hold (default: ETH_USDT)",
    )
    parser.add_argument(
        "--include-btc-eth-trades",
        action="store_true",
        help="Allow trading BTC_USDT / ETH_USDT alerts (default: excluded)",
    )
    parser.add_argument(
        "--trade-mode",
        type=str,
        choices=["rebalance", "usdt-sleeve"],
        default="rebalance",
        help=(
            "Trading simulation mode: "
            "rebalance=sell BTC+ETH into ALT and rebalance back (6 legs), "
            "usdt-sleeve=keep BTC/ETH hold and trade a USDT sleeve only (2 legs)."
        ),
    )
    parser.add_argument(
        "--sleeve-usdt",
        type=float,
        default=200.0,
        help="USDT sleeve size (USD notionals) for --trade-mode usdt-sleeve (default: 200)",
    )
    parser.add_argument(
        "--candles-source",
        type=str,
        choices=["live", "hist"],
        default="live",
        help="Which candle table to read from: live=candles_1m, hist=candles_1m_hist (default: live)",
    )
    parser.add_argument(
        "--bar-minutes",
        type=int,
        default=1,
        help=(
            "When --candles-source hist, optionally read aggregated bars from candles_hist_bars. "
            "Example: --bar-minutes 60 (requires scripts/build_hist_bars.py). Default: 1 (use 1m candles)."
        ),
    )
    parser.add_argument(
        "--alerts-csv",
        type=str,
        default=None,
        help="Optional path to alerts CSV (from replay/backtest). If set, DB alerts are ignored.",
    )
    parser.add_argument(
        "--out-csv",
        type=str,
        default="lookback_trades.csv",
        help="Output CSV path (default: lookback_trades.csv)",
    )
    args = parser.parse_args(argv)

    if args.days < 1:
        raise SystemExit("--days must be >= 1")
    if args.initial_usd <= 0:
        raise SystemExit("--initial-usd must be > 0")
    if args.max_hold_min < 1:
        raise SystemExit("--max-hold-min must be >= 1")
    if args.trade_mode == "usdt-sleeve":
        if args.sleeve_usdt < 0:
            raise SystemExit("--sleeve-usdt must be >= 0")
        if args.sleeve_usdt > args.initial_usd:
            raise SystemExit("--sleeve-usdt must be <= --initial-usd")
    if args.bar_minutes < 1:
        raise SystemExit("--bar-minutes must be >= 1")
    if args.candles_source == "live" and args.bar_minutes != 1:
        raise SystemExit("--bar-minutes is only supported with --candles-source hist")

    settings = get_settings()
    engine = create_engine(settings)
    session_factory = create_sessionmaker(engine)

    # Use last fully closed 1m candle as the default end.
    desired_end_t0 = floor_minute_ms(now_ms()) - MINUTE_MS
    if args.candles_source == "hist" and int(args.bar_minutes) > 1:
        bar_ms = int(args.bar_minutes) * MINUTE_MS
        desired_end_t0 = (int(desired_end_t0) // int(bar_ms)) * int(bar_ms)
    desired_start_t0 = desired_end_t0 - (args.days * DAY_MS)

    excluded_symbols: set[str] = set()
    if not args.include_btc_eth_trades:
        excluded_symbols.update([args.btc_symbol, args.eth_symbol])

    cost = CostModel(fee_bps=args.fee_bps, slippage_bps=args.slippage_bps)
    max_hold_ms = args.max_hold_min * MINUTE_MS
    timeframe_min: Optional[int] = None
    if args.candles_source == "hist" and int(args.bar_minutes) > 1:
        candles_model = CandleHistBar
        timeframe_min = int(args.bar_minutes)
    else:
        candles_model = Candle1mHist if args.candles_source == "hist" else Candle1m

    async with session_scope(session_factory) as session:
        if candles_model is CandleHistBar and timeframe_min is not None:
            await _assert_hist_bars_close_timestamps(session=session, timeframe_min=int(timeframe_min), symbol=args.btc_symbol)

        btc_series = await _load_price_series(
            session=session,
            symbol=args.btc_symbol,
            start_t0=desired_start_t0,
            end_t0=desired_end_t0,
            candles_model=candles_model,
            timeframe_min=timeframe_min,
        )
        eth_series = await _load_price_series(
            session=session,
            symbol=args.eth_symbol,
            start_t0=desired_start_t0,
            end_t0=desired_end_t0,
            candles_model=candles_model,
            timeframe_min=timeframe_min,
        )

        if btc_series.first_t is None or eth_series.first_t is None:
            raise SystemExit(
                f"Missing candles for {args.btc_symbol} or {args.eth_symbol}. "
                "Either run the app longer so it ingests candles, or backfill candles_1m_hist and use --candles-source hist."
            )

        start_t0 = max(btc_series.first_t, eth_series.first_t, desired_start_t0)
        end_t0 = min(btc_series.last_t or desired_end_t0, eth_series.last_t or desired_end_t0, desired_end_t0)

        if end_t0 <= start_t0:
            raise SystemExit("Not enough overlapping BTC/ETH candle history for the requested window.")

        # Load alerts using ts filtering (indexed). We intentionally set end_ts to "now" to avoid missing
        # alerts whose t0 is within the window but were created slightly after the candle boundary.
        if args.alerts_csv:
            entries_all, exits_by_symbol_all = _load_alerts_csv(Path(args.alerts_csv))
        else:
            entries_all, exits_by_symbol_all = await _load_alerts(
                session=session,
                start_ts=start_t0,
                end_ts=now_ms(),
            )

        # Filter entries into the final candle window.
        entries = [e for e in entries_all if start_t0 <= e.t0 <= end_t0 and e.symbol not in excluded_symbols]

        exits_by_symbol: dict[str, list[int]] = {}
        exits_total = 0
        for symbol, exit_t0s in exits_by_symbol_all.items():
            kept = [t for t in exit_t0s if start_t0 <= t <= end_t0]
            if kept:
                exits_by_symbol[symbol] = kept
                exits_total += len(kept)

        # Baseline hold: 50/50 BTC+ETH, no rebalancing.
        btc_px_start = btc_series.price_at_or_before(start_t0)
        eth_px_start = eth_series.price_at_or_before(start_t0)
        btc_px_end = btc_series.price_at_or_before(end_t0)
        eth_px_end = eth_series.price_at_or_before(end_t0)
        if btc_px_start is None or eth_px_start is None or btc_px_end is None or eth_px_end is None:
            raise SystemExit("Missing BTC/ETH prices at the lookback window boundaries.")

        baseline_usdt = 0.0
        baseline_core_usd = float(args.initial_usd)
        if args.trade_mode == "usdt-sleeve":
            baseline_usdt = float(args.sleeve_usdt)
            baseline_core_usd = float(args.initial_usd) - float(args.sleeve_usdt)

        baseline_btc_qty = (baseline_core_usd * 0.5) / btc_px_start if baseline_core_usd > 0.0 else 0.0
        baseline_eth_qty = (baseline_core_usd * 0.5) / eth_px_start if baseline_core_usd > 0.0 else 0.0

        baseline_equity_start = args.initial_usd
        baseline_equity_end = baseline_usdt + (baseline_btc_qty * btc_px_end) + (baseline_eth_qty * eth_px_end)
        baseline_return = (baseline_equity_end / baseline_equity_start) - 1.0

        trade_mode = str(args.trade_mode)

        # Active strategy
        if trade_mode == "rebalance":
            # Start 50/50 BTC+ETH, rebalance to 50/50 after each completed trade.
            active_btc_qty = (args.initial_usd * 0.5) / btc_px_start
            active_eth_qty = (args.initial_usd * 0.5) / eth_px_start
            active_sleeve_usdt = 0.0
        else:
            # Hold BTC/ETH core; trade only the USDT sleeve.
            active_btc_qty = float(baseline_btc_qty)
            active_eth_qty = float(baseline_eth_qty)
            active_sleeve_usdt = float(args.sleeve_usdt)

        total_fee_paid = 0.0
        total_slippage_paid = 0.0
        trades: list[TradeRecord] = []
        trade_mfe: list[float] = []
        trade_mae: list[float] = []
        trade_exit_ret: list[float] = []
        trade_capture: list[float] = []
        trade_hold_bars: list[float] = []

        current_position_end_t0: Optional[int] = None

        for entry in entries:
            if current_position_end_t0 is not None and entry.t0 <= current_position_end_t0:
                continue

            # Find earliest eligible exit alert, else time stop/end-of-window.
            exit_limit_t0 = min(entry.t0 + max_hold_ms, end_t0)
            exit_list = exits_by_symbol.get(entry.symbol, [])
            idx = bisect.bisect_right(exit_list, entry.t0)
            if idx < len(exit_list) and exit_list[idx] <= exit_limit_t0:
                exit_t0 = exit_list[idx]
                exit_reason = "exit_alert"
            else:
                exit_t0 = exit_limit_t0
                exit_reason = "time_stop" if exit_limit_t0 < end_t0 else "end_of_window"

            # Load symbol prices for entry/exit (DB first, then fallback to alert payload).
            sym_series = await _load_price_series(
                session=session,
                symbol=entry.symbol,
                start_t0=entry.t0,
                end_t0=exit_t0,
                candles_model=candles_model,
                timeframe_min=timeframe_min,
            )
            entry_point = sym_series.point_at_or_before(entry.t0)
            entry_px = entry_point[1] if entry_point else entry.price

            exit_point = sym_series.point_at_or_before(exit_t0)
            if exit_point is None:
                continue
            exit_t0 = exit_point[0]
            exit_px = exit_point[1]

            btc_px_entry = btc_series.price_at_or_before(entry.t0)
            eth_px_entry = eth_series.price_at_or_before(entry.t0)
            btc_px_exit = btc_series.price_at_or_before(exit_t0)
            eth_px_exit = eth_series.price_at_or_before(exit_t0)
            if btc_px_entry is None or eth_px_entry is None or btc_px_exit is None or eth_px_exit is None:
                continue

            equity_before = (active_btc_qty * btc_px_entry) + (active_eth_qty * eth_px_entry) + float(active_sleeve_usdt)

            if trade_mode == "rebalance":
                # Entry: sell BTC + ETH -> USDT, then buy ALT.
                usdt_from_btc, cost_btc_sell = sell_with_costs(active_btc_qty, price=btc_px_entry, cost=cost)
                usdt_from_eth, cost_eth_sell = sell_with_costs(active_eth_qty, price=eth_px_entry, cost=cost)
                total_fee_paid += cost_btc_sell.fee_paid + cost_eth_sell.fee_paid
                total_slippage_paid += cost_btc_sell.slippage_paid + cost_eth_sell.slippage_paid

                active_btc_qty = 0.0
                active_eth_qty = 0.0
                usdt_total = usdt_from_btc + usdt_from_eth

                alt_qty, cost_alt_buy = buy_with_costs(usdt_total, price=entry_px, cost=cost)
                total_fee_paid += cost_alt_buy.fee_paid
                total_slippage_paid += cost_alt_buy.slippage_paid

                # Exit: sell ALT -> USDT, then buy BTC + ETH (rebalance 50/50).
                usdt_after_sell, cost_alt_sell = sell_with_costs(alt_qty, price=exit_px, cost=cost)
                total_fee_paid += cost_alt_sell.fee_paid
                total_slippage_paid += cost_alt_sell.slippage_paid

                usdt_half = usdt_after_sell * 0.5
                new_btc_qty, cost_btc_buy = buy_with_costs(usdt_half, price=btc_px_exit, cost=cost)
                new_eth_qty, cost_eth_buy = buy_with_costs(usdt_half, price=eth_px_exit, cost=cost)
                total_fee_paid += cost_btc_buy.fee_paid + cost_eth_buy.fee_paid
                total_slippage_paid += cost_btc_buy.slippage_paid + cost_eth_buy.slippage_paid

                active_btc_qty = new_btc_qty
                active_eth_qty = new_eth_qty

                equity_after = (active_btc_qty * btc_px_exit) + (active_eth_qty * eth_px_exit)
                net_ret = (equity_after / equity_before) - 1.0 if equity_before > 0.0 else 0.0

                costs_fee = (
                    cost_btc_sell.fee_paid
                    + cost_eth_sell.fee_paid
                    + cost_alt_buy.fee_paid
                    + cost_alt_sell.fee_paid
                    + cost_btc_buy.fee_paid
                    + cost_eth_buy.fee_paid
                )
                costs_slippage = (
                    cost_btc_sell.slippage_paid
                    + cost_eth_sell.slippage_paid
                    + cost_alt_buy.slippage_paid
                    + cost_alt_sell.slippage_paid
                    + cost_btc_buy.slippage_paid
                    + cost_eth_buy.slippage_paid
                )
            else:
                # USDT sleeve mode: buy/sell only with the sleeve; core BTC/ETH stays held.
                if active_sleeve_usdt <= 0.0:
                    continue

                alt_qty, cost_alt_buy = buy_with_costs(active_sleeve_usdt, price=entry_px, cost=cost)
                total_fee_paid += cost_alt_buy.fee_paid
                total_slippage_paid += cost_alt_buy.slippage_paid
                alt_qty_entry = float(alt_qty)
                active_sleeve_usdt = 0.0

                usdt_after_sell, cost_alt_sell = sell_with_costs(alt_qty, price=exit_px, cost=cost)
                total_fee_paid += cost_alt_sell.fee_paid
                total_slippage_paid += cost_alt_sell.slippage_paid
                active_sleeve_usdt = float(usdt_after_sell)

                equity_after = (active_btc_qty * btc_px_exit) + (active_eth_qty * eth_px_exit) + float(active_sleeve_usdt)
                net_ret = (equity_after / equity_before) - 1.0 if equity_before > 0.0 else 0.0

                costs_fee = cost_alt_buy.fee_paid + cost_alt_sell.fee_paid
                costs_slippage = cost_alt_buy.slippage_paid + cost_alt_sell.slippage_paid
                alt_qty = alt_qty_entry

            duration_min = int((exit_t0 - entry.t0) / MINUTE_MS)
            gross_ret = (exit_px / entry_px) - 1.0 if entry_px > 0.0 else 0.0

            max_high, min_low = await _load_trade_extremes(
                session=session,
                symbol=entry.symbol,
                entry_t0=entry.t0,
                exit_t0=exit_t0,
                candles_model=candles_model,
                timeframe_min=timeframe_min,
            )
            if entry_px > 0.0 and max_high is not None and min_low is not None:
                mfe = (float(max_high) / float(entry_px)) - 1.0
                mae = (float(min_low) / float(entry_px)) - 1.0
                trade_mfe.append(float(mfe))
                trade_mae.append(float(mae))
                trade_exit_ret.append(float(gross_ret))
                if float(mfe) > 0.0:
                    trade_capture.append(float(gross_ret) / float(mfe))

            bar_minutes = int(args.bar_minutes) if int(args.bar_minutes) > 0 else 1
            trade_hold_bars.append(float(duration_min) / float(bar_minutes))

            trades.append(
                TradeRecord(
                    symbol=entry.symbol,
                    entry_alert_id=entry.id,
                    entry_ts=entry.ts,
                    entry_t0=entry.t0,
                    exit_t0=exit_t0,
                    exit_reason=exit_reason,
                    entry_price=float(entry_px),
                    exit_price=float(exit_px),
                    duration_min=duration_min,
                    gross_ret=gross_ret,
                    net_ret=net_ret,
                    costs_fee=costs_fee,
                    costs_slippage=costs_slippage,
                    equity_before=equity_before,
                    equity_after=equity_after,
                    alt_qty=float(alt_qty),
                    btc_qty_after=float(active_btc_qty),
                    eth_qty_after=float(active_eth_qty),
                )
            )

            current_position_end_t0 = exit_t0

        # Compute end equity for active strategy.
        if trade_mode == "rebalance":
            active_equity_end = (active_btc_qty * btc_px_end) + (active_eth_qty * eth_px_end)
        else:
            active_equity_end = (active_btc_qty * btc_px_end) + (active_eth_qty * eth_px_end) + float(active_sleeve_usdt)
        active_return = (active_equity_end / args.initial_usd) - 1.0

        window_days = (end_t0 - start_t0) / float(DAY_MS)
        do_cost = args.do_cost_per_month * (window_days / 30.0)
        active_equity_after_do = active_equity_end - do_cost
        active_return_after_do = (active_equity_after_do / args.initial_usd) - 1.0

        # Compute equity curves (per-minute) for drawdown estimates.
        times = list(range(start_t0, end_t0 + MINUTE_MS, MINUTE_MS))

        # Baseline curve
        baseline_curve: list[float] = []
        for t in times:
            btc_px = btc_series.price_at_or_before(t)
            eth_px = eth_series.price_at_or_before(t)
            if btc_px is None or eth_px is None:
                continue
            baseline_curve.append(float(baseline_usdt) + (baseline_btc_qty * btc_px) + (baseline_eth_qty * eth_px))

        baseline_mdd = max_drawdown(baseline_curve)

        # Active curve
        trade_by_entry: dict[int, TradeRecord] = {tr.entry_t0: tr for tr in trades}

        active_curve: list[float] = []
        in_trade: Optional[TradeRecord] = None

        # Cache symbol series for trade windows
        symbol_series_cache: dict[str, PriceSeries] = {}

        if trade_mode == "rebalance":
            active_btc_qty_curve = (args.initial_usd * 0.5) / btc_px_start
            active_eth_qty_curve = (args.initial_usd * 0.5) / eth_px_start
            active_alt_qty_curve = 0.0

            for t in times:
                # apply state transitions before valuing
                if in_trade is None and t in trade_by_entry:
                    tr = trade_by_entry[t]
                    in_trade = tr
                    active_alt_qty_curve = tr.alt_qty
                    active_btc_qty_curve = 0.0
                    active_eth_qty_curve = 0.0
                if in_trade is not None and t == in_trade.exit_t0:
                    tr = in_trade
                    in_trade = None
                    active_alt_qty_curve = 0.0
                    active_btc_qty_curve = tr.btc_qty_after
                    active_eth_qty_curve = tr.eth_qty_after

                if in_trade is None:
                    btc_px = btc_series.price_at_or_before(t)
                    eth_px = eth_series.price_at_or_before(t)
                    if btc_px is None or eth_px is None:
                        continue
                    active_curve.append((active_btc_qty_curve * btc_px) + (active_eth_qty_curve * eth_px))
                else:
                    sym = in_trade.symbol
                    series = symbol_series_cache.get(sym)
                    if (
                        series is None
                        or series.first_t is None
                        or series.first_t > in_trade.entry_t0
                        or (series.last_t or 0) < in_trade.exit_t0
                    ):
                        series = await _load_price_series(
                            session=session,
                            symbol=sym,
                            start_t0=in_trade.entry_t0,
                            end_t0=in_trade.exit_t0,
                            candles_model=candles_model,
                            timeframe_min=timeframe_min,
                        )
                        symbol_series_cache[sym] = series
                    px = series.price_at_or_before(t)
                    if px is None:
                        continue
                    active_curve.append(active_alt_qty_curve * px)
        else:
            core_btc_qty_curve = float(baseline_btc_qty)
            core_eth_qty_curve = float(baseline_eth_qty)
            sleeve_usdt_curve = float(args.sleeve_usdt)
            alt_qty_curve = 0.0

            for t in times:
                if in_trade is None and t in trade_by_entry:
                    tr = trade_by_entry[t]
                    in_trade = tr
                    alt_qty_curve = tr.alt_qty
                    sleeve_usdt_curve = 0.0
                if in_trade is not None and t == in_trade.exit_t0:
                    tr = in_trade
                    in_trade = None
                    btc_px = btc_series.price_at_or_before(t)
                    eth_px = eth_series.price_at_or_before(t)
                    if btc_px is not None and eth_px is not None:
                        core_val = (core_btc_qty_curve * btc_px) + (core_eth_qty_curve * eth_px)
                        sleeve_usdt_curve = float(tr.equity_after) - float(core_val)
                    alt_qty_curve = 0.0

                btc_px = btc_series.price_at_or_before(t)
                eth_px = eth_series.price_at_or_before(t)
                if btc_px is None or eth_px is None:
                    continue
                core_val = (core_btc_qty_curve * btc_px) + (core_eth_qty_curve * eth_px)

                if in_trade is None:
                    active_curve.append(float(core_val) + float(sleeve_usdt_curve))
                else:
                    sym = in_trade.symbol
                    series = symbol_series_cache.get(sym)
                    if (
                        series is None
                        or series.first_t is None
                        or series.first_t > in_trade.entry_t0
                        or (series.last_t or 0) < in_trade.exit_t0
                    ):
                        series = await _load_price_series(
                            session=session,
                            symbol=sym,
                            start_t0=in_trade.entry_t0,
                            end_t0=in_trade.exit_t0,
                            candles_model=candles_model,
                            timeframe_min=timeframe_min,
                        )
                        symbol_series_cache[sym] = series
                    px = series.price_at_or_before(t)
                    if px is None:
                        continue
                    active_curve.append(float(core_val) + (alt_qty_curve * px))

        active_mdd = max_drawdown(active_curve)

    # Write trades CSV
    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "entry_ts",
                "entry_t0",
                "exit_t0",
                "symbol",
                "entry_alert_id",
                "entry_px",
                "exit_px",
                "duration_min",
                "exit_reason",
                "gross_ret",
                "net_ret",
                "equity_before",
                "equity_after",
                "fee_paid_usd",
                "slippage_paid_usd",
            ]
        )
        for tr in trades:
            w.writerow(
                [
                    tr.entry_ts,
                    tr.entry_t0,
                    tr.exit_t0,
                    tr.symbol,
                    tr.entry_alert_id,
                    f"{tr.entry_price:.12g}",
                    f"{tr.exit_price:.12g}",
                    tr.duration_min,
                    tr.exit_reason,
                    f"{tr.gross_ret:.8f}",
                    f"{tr.net_ret:.8f}",
                    f"{tr.equity_before:.6f}",
                    f"{tr.equity_after:.6f}",
                    f"{tr.costs_fee:.6f}",
                    f"{tr.costs_slippage:.6f}",
                ]
            )

    trade_net_rets = [t.net_ret for t in trades]
    trade_gross_rets = [t.gross_ret for t in trades]
    win_rate = (sum(1 for r in trade_net_rets if r > 0.0) / len(trade_net_rets)) if trade_net_rets else 0.0

    net_summary = _summarize(trade_net_rets)
    gross_summary = _summarize(trade_gross_rets)
    gross_pnl = (active_equity_end - float(args.initial_usd)) + float(total_fee_paid) + float(total_slippage_paid)
    net_pnl = active_equity_end - float(args.initial_usd)
    avg_hold_bars = float(statistics.mean(trade_hold_bars)) if trade_hold_bars else None
    med_mfe = float(statistics.median(trade_mfe)) if trade_mfe else None
    med_mae = float(statistics.median(trade_mae)) if trade_mae else None
    med_exit_ret = float(statistics.median(trade_exit_ret)) if trade_exit_ret else None
    med_capture = float(statistics.median(trade_capture)) if trade_capture else None

    print("")
    print("Lookback evaluation (alerts-only, no signal regeneration)")
    if trade_mode == "usdt-sleeve":
        print(f"- Trade mode: usdt-sleeve (sleeve_usdt=${float(args.sleeve_usdt):.2f})")
    else:
        print("- Trade mode: rebalance (sell BTC+ETH into ALT, then rebalance back)")
    print(f"- Window: t0 [{start_t0} .. {end_t0}] (~{window_days:.2f} days)")
    print(f"- Trades: {len(trades)} (one position at a time)")
    print(f"- Entry alerts in window: {len(entries)}")
    print(f"- Exit alerts in window: {exits_total}")
    print(f"- Win rate (net): {win_rate:.2%}")
    print(f"- Total fees paid (est): ${total_fee_paid:,.2f}")
    print(f"- Total slippage (est): ${total_slippage_paid:,.2f}")
    print(f"- Gross vs net: gross_pnl=${gross_pnl:,.2f} | fees=${total_fee_paid:,.2f} | slippage=${total_slippage_paid:,.2f} | net_pnl=${net_pnl:,.2f}")
    avg_hold_s = "n/a" if avg_hold_bars is None else f"{avg_hold_bars:.2f}"
    print(f"- Turnover: trades={len(trades)} | entries={len(entries)} | exits={exits_total} | avg_hold_bars={avg_hold_s}")
    mfe_s = "n/a" if med_mfe is None else f"{med_mfe:.2%}"
    mae_s = "n/a" if med_mae is None else f"{med_mae:.2%}"
    exit_ret_s = "n/a" if med_exit_ret is None else f"{med_exit_ret:.2%}"
    cap_s = "n/a" if med_capture is None else f"{med_capture:.2f}"
    print(f"- Tail capture (median): mfe={mfe_s} | mae={mae_s} | exit_return={exit_ret_s} | capture_ratio={cap_s}")
    print(f"- Trades CSV: {out_path}")
    print("")

    print("Strategy (trade alerts)")
    print(f"- Final equity: ${active_equity_end:,.2f} (return {active_return:.2%})")
    print(f"- Max drawdown (est): {active_mdd:.2%}")
    print(f"- Final equity after DO cost (${do_cost:,.2f}): ${active_equity_after_do:,.2f} (return {active_return_after_do:.2%})")
    print(f"- Net trade return summary: {net_summary}")
    print(f"- Gross trade return summary: {gross_summary}")
    print("")

    if trade_mode == "usdt-sleeve":
        print("Baseline (hold 50/50 BTC+ETH core + USDT sleeve)")
    else:
        print("Baseline (hold 50/50 BTC+ETH)")
    print(f"- Final equity: ${baseline_equity_end:,.2f} (return {baseline_return:.2%})")
    print(f"- Max drawdown (est): {baseline_mdd:.2%}")
    print("")

    diff = active_equity_end - baseline_equity_end
    diff_after_do = active_equity_after_do - baseline_equity_end
    print("Active vs Hold")
    print(f"- Equity delta: ${diff:,.2f}")
    print(f"- Equity delta after DO: ${diff_after_do:,.2f}")
    print("")

    if not trades:
        print("No trades found in window. If you expected trades, try:")
        print("- Run the app longer so it emits alerts")
        print("- Temporarily relax thresholds via POST /config (then re-run lookback later)")
        print("")

    return 0


def main() -> None:
    try:
        rc = asyncio.run(_main_async(sys.argv[1:]))
    except KeyboardInterrupt:
        rc = 130
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
