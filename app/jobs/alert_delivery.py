from __future__ import annotations

import asyncio
import logging
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Alert
from app.notifier.base import Notifier
from app.time_utils import now_ms

logger = logging.getLogger(__name__)

# Keep retry semantics simple and predictable:
# - attempts are "send_text()" calls (SlackWebhookNotifier already retries transient HTTP issues internally)
# - retry window is short to avoid delivering stale alerts

_MAX_DELIVERY_ATTEMPTS = 4  # initial attempt + 3 retries
_MAX_ALERT_AGE_MS = 3 * 60_000  # don't deliver if older than this
_BATCH_SIZE = 50

# Delay since last attempt for the next attempt, keyed by current attempts made.
# attempts=0 -> attempt immediately
_RETRY_DELAYS_S: dict[int, float] = {
    0: 0.0,
    1: 5.0,
    2: 15.0,
    3: 45.0,
}

_FAILURES_BEFORE_PAUSE = 3
_PAUSE_S = 90.0


def _truncate(s: str, max_len: int = 500) -> str:
    if len(s) <= max_len:
        return s
    return s[:max_len]


def _retry_delay_s(attempts: int) -> float:
    attempts = int(attempts)
    return _RETRY_DELAYS_S.get(attempts, 45.0)


async def deliver_pending_alerts_once(
    session_factory: async_sessionmaker[AsyncSession],
    notifier: Notifier,
    *,
    enabled: bool,
    delivery_channel: str = "slack",
    batch_size: int = _BATCH_SIZE,
) -> dict[str, int]:
    """
    Attempt delivery for recently created, undelivered alerts.

    Budgets are enforced at alert creation time (state machine). Delivery is best-effort and does not
    affect eligibility.
    """
    if not enabled:
        return {"attempted": 0, "delivered": 0, "failed": 0, "skipped_not_due": 0, "skipped_stale": 0}

    now = now_ms()
    cutoff = now - _MAX_ALERT_AGE_MS

    attempted = 0
    delivered = 0
    failed = 0
    skipped_not_due = 0
    skipped_stale = 0

    async with session_factory() as session:
        # Only look at very recent alerts to avoid stale delivery after restarts.
        rows = (
            await session.execute(
                select(Alert)
                .where(
                    Alert.delivered.is_(False),
                    Alert.ts >= cutoff,
                    Alert.delivery_attempts < _MAX_DELIVERY_ATTEMPTS,
                )
                .order_by(Alert.ts.asc())
                .limit(int(batch_size))
            )
        ).scalars().all()

        for alert in rows:
            # Time-based stale check is enforced in SQL, but keep it defensive.
            if int(alert.ts) < cutoff:
                skipped_stale += 1
                continue

            current_attempts = int(alert.delivery_attempts or 0)
            if current_attempts >= _MAX_DELIVERY_ATTEMPTS:
                continue

            last_ts = alert.last_delivery_attempt_ts
            if last_ts is not None:
                delay_ms = int(_retry_delay_s(current_attempts) * 1000)
                if delay_ms > 0 and (now - int(last_ts)) < delay_ms:
                    skipped_not_due += 1
                    continue

            attempted += 1
            alert.delivery_attempts = current_attempts + 1
            alert.last_delivery_attempt_ts = now

            try:
                await notifier.send_text(alert.message)
            except Exception as e:
                failed += 1
                alert.delivery_error = _truncate(repr(e))
                logger.exception(
                    "alert delivery failed",
                    extra={"alert_id": alert.id, "attempt": alert.delivery_attempts},
                )
                continue

            delivered += 1
            alert.delivered = True
            alert.delivery_channel = delivery_channel
            alert.delivery_error = None

        await session.commit()

    return {
        "attempted": attempted,
        "delivered": delivered,
        "failed": failed,
        "skipped_not_due": skipped_not_due,
        "skipped_stale": skipped_stale,
    }


async def run_alert_delivery_service(
    session_factory: async_sessionmaker[AsyncSession],
    notifier: Notifier,
    settings_provider,
    *,
    interval_s: float = 2.0,
) -> None:
    pause_until_ms: Optional[int] = None
    consecutive_failures = 0

    while True:
        try:
            settings = settings_provider()
            enabled = bool(getattr(settings, "slack_webhook_url", None))

            now = now_ms()
            if pause_until_ms is not None and now < pause_until_ms:
                await asyncio.sleep(max(0.5, (pause_until_ms - now) / 1000.0))
                continue

            res = await deliver_pending_alerts_once(session_factory, notifier, enabled=enabled)
            if res["failed"] > 0 and res["delivered"] == 0:
                consecutive_failures += res["failed"]
            elif res["delivered"] > 0:
                consecutive_failures = 0

            if consecutive_failures >= _FAILURES_BEFORE_PAUSE:
                pause_until_ms = now_ms() + int(_PAUSE_S * 1000)
                logger.warning(
                    "notifier paused after repeated failures",
                    extra={"pause_s": _PAUSE_S, "failures": consecutive_failures},
                )
                consecutive_failures = 0
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("alert delivery tick failed")

        await asyncio.sleep(max(0.5, float(interval_s)))

