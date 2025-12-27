from __future__ import annotations

import asyncio
import contextlib
import logging
from contextlib import AsyncExitStack, asynccontextmanager

import sqlalchemy as sa
from fastapi import Depends, FastAPI, HTTPException, Request
from sqlalchemy import select

from app.api_schemas import ConfigPatch
from app.candles import CandleStore
from app.alerting import ALERT_TYPE_MOMENTUM_UP
from app.config import Settings, get_settings
from app.config_store import apply_overrides, load_settings_overrides, save_settings_overrides
from app.db import create_engine, create_sessionmaker
from app.exchange_client import CryptoComExchangeClient
from app.jobs.alert_delivery import run_alert_delivery_service
from app.jobs.candle_ingestion import run_candle_ingestion_service
from app.jobs.evaluation import run_evaluation_service
from app.models import Alert, AlertEvaluation, UniverseMembership
from app.notifier.factory import build_notifier
from app.signals import compute_latest_signals
from app.stats import float_or_none, mean, median
from app.state_machine import run_state_machine_and_alert
from app.time_utils import now_ms
from app.universe import refresh_universe

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    exit_stack = AsyncExitStack()
    base_settings = get_settings()
    app.state.base_settings = base_settings

    if base_settings.app_env == "test":
        app.state.settings = base_settings
        yield
        await exit_stack.aclose()
        return

    engine = create_engine(base_settings)
    session_factory = create_sessionmaker(engine)

    # Load persisted overrides (if any) and apply on top of env/default settings.
    overrides = {}
    overrides_updated_ts = None
    async with session_factory() as session:
        try:
            overrides, overrides_updated_ts = await load_settings_overrides(session)
        except Exception:
            logger.exception("failed to load persisted config overrides; using env/default settings")
            overrides = {}
            overrides_updated_ts = None

    settings = apply_overrides(base_settings, overrides)
    app.state.settings = settings
    app.state.settings_overrides = overrides
    app.state.settings_overrides_updated_ts = overrides_updated_ts

    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))

    exchange = CryptoComExchangeClient.from_settings(settings)
    exit_stack.push_async_callback(exchange.aclose)

    notifier = build_notifier(settings)
    if hasattr(notifier, "aclose"):
        exit_stack.push_async_callback(notifier.aclose)  # type: ignore[attr-defined]

    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.exchange_client = exchange
    app.state.notifier = notifier

    candle_store = CandleStore(buffer_size=settings.candle_buffer_size)
    app.state.candle_store = candle_store
    app.state.last_candle_ingest = None

    async def _update_latest_signals() -> None:
        settings = app.state.settings
        snapshot = await compute_latest_signals(
            session_factory=session_factory,
            exchange=exchange,
            settings=settings,
            store=candle_store,
        )
        app.state.latest_signals = snapshot
        await run_state_machine_and_alert(
            session_factory=session_factory,
            exchange=exchange,
            notifier=notifier,
            settings=settings,
            signals_snapshot=snapshot,
        )

    async def _startup_universe_refresh() -> None:
        async with session_factory() as session:
            try:
                await refresh_universe(session=session, client=exchange, settings=app.state.settings)
                await session.commit()
            except Exception:
                await session.rollback()
                logger.exception("startup universe refresh failed")

    app.state.startup_universe_task = asyncio.create_task(_startup_universe_refresh())

    def _record_ingest(result: dict[str, object]) -> None:
        app.state.last_candle_ingest = {**result, "ts": now_ms()}

    app.state.candle_ingestion_task = asyncio.create_task(
        run_candle_ingestion_service(
            session_factory=session_factory,
            exchange=exchange,
            settings_provider=lambda: app.state.settings,
            store=candle_store,
            startup_universe_task=app.state.startup_universe_task,
            on_after_ingest=_update_latest_signals,
            on_ingest_result=_record_ingest,
        )
    )
    app.state.evaluation_task = asyncio.create_task(run_evaluation_service(session_factory=session_factory))
    app.state.alert_delivery_task = asyncio.create_task(
        run_alert_delivery_service(
            session_factory=session_factory,
            notifier=notifier,
            settings_provider=lambda: app.state.settings,
        )
    )

    yield

    for task_name in ("alert_delivery_task", "evaluation_task", "candle_ingestion_task", "startup_universe_task"):
        task = getattr(app.state, task_name, None)
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    await exit_stack.aclose()
    await engine.dispose()


app = FastAPI(title="Crypto Momentum Scout", lifespan=lifespan)


def settings_dep(request: Request) -> Settings:
    return request.app.state.settings


async def db_session_dep(request: Request):
    async with request.app.state.session_factory() as session:
        yield session


@app.get("/health")
async def health(request: Request):
    db_ok = None
    if hasattr(request.app.state, "session_factory"):
        try:
            async with request.app.state.session_factory() as session:
                await session.execute(sa.text("SELECT 1"))
            db_ok = True
        except Exception:
            db_ok = False

    latest_signals = getattr(request.app.state, "latest_signals", None)
    latest_signals_ts = latest_signals.get("ts") if isinstance(latest_signals, dict) else None

    tasks: dict[str, dict[str, object]] = {}
    for name in ("startup_universe_task", "candle_ingestion_task", "evaluation_task", "alert_delivery_task"):
        task = getattr(request.app.state, name, None)
        if task is None:
            continue
        info: dict[str, object] = {"done": bool(task.done()), "cancelled": bool(task.cancelled())}
        if task.done() and not task.cancelled():
            exc = task.exception()
            if exc is not None:
                info["error"] = repr(exc)
        tasks[name] = info

    return {
        "ok": True,
        "db_ok": db_ok,
        "latest_signals_ts": latest_signals_ts,
        "last_candle_ingest": getattr(request.app.state, "last_candle_ingest", None),
        "tasks": tasks,
    }


@app.get("/config")
async def get_config(settings: Settings = Depends(settings_dep)):
    # Return a safe view (avoid leaking secrets if added later)
    return {
        "overrides": getattr(app.state, "settings_overrides", {}),
        "overrides_updated_ts": getattr(app.state, "settings_overrides_updated_ts", None),
        "app_env": settings.app_env,
        "quote_ccy": settings.quote_ccy,
        "max_universe_size": settings.max_universe_size,
        "max_concurrent_requests": settings.max_concurrent_requests,
        "http_timeout_s": settings.http_timeout_s,
        "alert_lookback_hours": settings.alert_lookback_hours,
        "max_entry_alerts_24h": settings.max_entry_alerts_24h,
        "max_exit_alerts_24h": settings.max_exit_alerts_24h,
        "max_total_alerts_24h": settings.max_total_alerts_24h,
        "candle_timeframe": settings.candle_timeframe,
        "candle_backfill_count": settings.candle_backfill_count,
        "candle_poll_count": settings.candle_poll_count,
        "candle_fetch_delay_s": settings.candle_fetch_delay_s,
        "candle_buffer_size": settings.candle_buffer_size,
        "candle_retention_days": settings.candle_retention_days,
        "candle_prune_interval_hours": settings.candle_prune_interval_hours,
        "dvz_min": settings.dvz_min,
        "extension_max": settings.extension_max,
        "min_dv_1m_usd": settings.min_dv_1m_usd,
        "spread_max": settings.spread_max,
        "book_check_top_n": settings.book_check_top_n,
        "signals_return_limit": settings.signals_return_limit,
        "entry_score_threshold": settings.entry_score_threshold,
        "exit_score_threshold": settings.exit_score_threshold,
        "stall_minutes": settings.stall_minutes,
        "stall_dvz_max": settings.stall_dvz_max,
        "global_entry_cooldown_min": settings.global_entry_cooldown_min,
        "symbol_entry_cooldown_min": settings.symbol_entry_cooldown_min,
        "symbol_exit_cooldown_min": settings.symbol_exit_cooldown_min,
        "max_entry_alerts_per_scan": settings.max_entry_alerts_per_scan,
        "slack_enabled": settings.slack_webhook_url is not None,
        "slack_channel_name": settings.slack_channel_name,
    }


@app.post("/config")
async def update_config(patch: ConfigPatch, request: Request, session=Depends(db_session_dep)):
    if not isinstance(getattr(request.app.state, "base_settings", None), Settings):
        raise HTTPException(status_code=500, detail="Base settings not initialized")

    settings = request.app.state.settings
    if settings.admin_token is not None:
        provided = request.headers.get("x-admin-token")
        if provided != settings.admin_token:
            raise HTTPException(status_code=401, detail="Unauthorized")

    delta = patch.model_dump(exclude_none=True)
    if not delta:
        return await get_config(settings=settings)

    current_overrides = getattr(request.app.state, "settings_overrides", {}) or {}
    if not isinstance(current_overrides, dict):
        current_overrides = {}

    new_overrides = {**current_overrides, **delta}

    # Validate by re-materializing Settings.
    try:
        new_settings = apply_overrides(request.app.state.base_settings, new_overrides)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid config: {e}") from e

    try:
        updated_ts = await save_settings_overrides(session, new_overrides)
        await session.commit()
    except Exception:
        await session.rollback()
        raise

    request.app.state.settings = new_settings
    request.app.state.settings_overrides = new_overrides
    request.app.state.settings_overrides_updated_ts = updated_ts

    return await get_config(settings=new_settings)


@app.post("/notify/test")
async def notify_test(payload: dict, settings: Settings = Depends(settings_dep)):
    if settings.app_env != "dev":
        raise HTTPException(status_code=404, detail="Not found")
    if settings.slack_webhook_url is None:
        return {"ok": False, "error": "Slack not configured (set SLACK_WEBHOOK_URL)."}
    text = str(payload.get("text") or "").strip()
    if not text:
        return {"ok": False, "error": "Missing 'text'."}
    await app.state.notifier.send_text(text)
    return {"ok": True}


@app.get("/universe")
async def get_universe(settings: Settings = Depends(settings_dep), session=Depends(db_session_dep)):
    rows = (
        await session.execute(
            select(UniverseMembership)
            .where(
                UniverseMembership.quote_ccy == settings.quote_ccy,
                UniverseMembership.is_active.is_(True),
            )
            .order_by(UniverseMembership.liquidity_rank.asc())
        )
    ).scalars().all()

    baseline = next((r.symbol for r in rows if r.is_baseline), None)
    updated_ts = max((int(r.updated_ts) for r in rows), default=None)

    return {
        "quote_ccy": settings.quote_ccy,
        "baseline_symbol": baseline,
        "updated_ts": updated_ts,
        "members": [
            {
                "symbol": r.symbol,
                "liquidity_rank": int(r.liquidity_rank),
                "dollar_vol_24h": float(r.dollar_vol_24h),
                "is_baseline": bool(r.is_baseline),
            }
            for r in rows
        ],
    }


@app.post("/universe/refresh")
async def refresh_universe_endpoint(
    settings: Settings = Depends(settings_dep),
    session=Depends(db_session_dep),
):
    if settings.app_env != "dev":
        raise HTTPException(status_code=404, detail="Not found")
    selection = await refresh_universe(
        session=session,
        client=app.state.exchange_client,
        settings=settings,
    )
    await session.commit()
    return {
        "quote_ccy": selection.quote_ccy,
        "baseline_symbol": selection.baseline_symbol,
        "updated_ts": selection.updated_ts,
        "members": len(selection.members),
    }


@app.get("/signals/latest")
async def signals_latest(limit: int = 50):
    snapshot = getattr(app.state, "latest_signals", None)
    if snapshot is None:
        return {"ts": None, "error": "No snapshot yet.", "signals": []}
    signals = snapshot.get("signals")
    if isinstance(signals, list):
        limit = max(1, min(int(limit), 500))
        return {**snapshot, "signals": signals[:limit]}
    return snapshot


@app.get("/alerts")
async def list_alerts(limit: int = 100, session=Depends(db_session_dep)):
    limit = max(1, min(int(limit), 500))
    rows = (await session.execute(select(Alert).order_by(Alert.ts.desc()).limit(limit))).scalars().all()
    return {
        "alerts": [
            {
                "id": a.id,
                "ts": int(a.ts),
                "symbol": a.symbol,
                "alert_type": a.alert_type,
                "score": float(a.score),
                "features": a.features_json,
                "message": a.message,
                "delivered": bool(a.delivered),
                "delivery_channel": a.delivery_channel,
                "delivery_attempts": int(a.delivery_attempts or 0),
                "last_delivery_attempt_ts": int(a.last_delivery_attempt_ts) if a.last_delivery_attempt_ts else None,
                "delivery_error": a.delivery_error,
            }
            for a in rows
        ]
    }


@app.get("/alerts/{alert_id}")
async def get_alert(alert_id: str, session=Depends(db_session_dep)):
    alert = (await session.execute(select(Alert).where(Alert.id == alert_id))).scalars().first()
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {
        "id": alert.id,
        "ts": int(alert.ts),
        "symbol": alert.symbol,
        "alert_type": alert.alert_type,
        "score": float(alert.score),
        "features": alert.features_json,
        "message": alert.message,
        "delivered": bool(alert.delivered),
        "delivery_channel": alert.delivery_channel,
        "delivery_attempts": int(alert.delivery_attempts or 0),
        "last_delivery_attempt_ts": int(alert.last_delivery_attempt_ts) if alert.last_delivery_attempt_ts else None,
        "delivery_error": alert.delivery_error,
    }


@app.get("/eval")
async def eval_summary(days: int = 7, session=Depends(db_session_dep)):
    days = max(1, min(int(days), 90))
    cutoff = now_ms() - (days * 24 * 3600 * 1000)

    rows = (
        await session.execute(
            select(Alert, AlertEvaluation)
            .join(AlertEvaluation, AlertEvaluation.alert_id == Alert.id, isouter=True)
            .where(Alert.alert_type == ALERT_TYPE_MOMENTUM_UP, Alert.ts >= cutoff)
            .order_by(Alert.ts.desc())
        )
    ).all()

    total_entries = len(rows)
    evals = [ev for (_a, ev) in rows if ev is not None]

    r5 = [float_or_none(ev.r_5m) for ev in evals]
    r15 = [float_or_none(ev.r_15m) for ev in evals]
    r60 = [float_or_none(ev.r_60m) for ev in evals]
    mae = [float_or_none(ev.mae_60m) for ev in evals]
    mfe = [float_or_none(ev.mfe_60m) for ev in evals]

    r5v = [x for x in r5 if x is not None]
    r15v = [x for x in r15 if x is not None]
    r60v = [x for x in r60 if x is not None]
    maev = [x for x in mae if x is not None]
    mfev = [x for x in mfe if x is not None]

    precision_pos_r15 = None
    if r15v:
        precision_pos_r15 = sum(1 for x in r15v if x > 0) / len(r15v)

    return {
        "days": days,
        "cutoff_ts": cutoff,
        "entry_alerts": total_entries,
        "evaluated": len(evals),
        "precision_proxy_pos_r15": precision_pos_r15,
        "avg_r_5m": mean(r5v),
        "avg_r_15m": mean(r15v),
        "avg_r_60m": mean(r60v),
        "median_r_15m": median(r15v),
        "avg_mae_60m": mean(maev),
        "avg_mfe_60m": mean(mfev),
        "median_mae_60m": median(maev),
        "median_mfe_60m": median(mfev),
    }
