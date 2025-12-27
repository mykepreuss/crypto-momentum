from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

from app.notifier.base import Notifier

logger = logging.getLogger(__name__)

_MAX_RETRIES = 5
_MAX_RETRY_AFTER_S = 30.0
_INITIAL_BACKOFF_S = 0.5
_MAX_BACKOFF_S = 8.0


class SlackWebhookNotifier(Notifier):
    def __init__(self, webhook_url: str, channel_name: Optional[str] = None) -> None:
        self._webhook_url = webhook_url
        self._channel_name = channel_name
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(10.0))

    async def aclose(self) -> None:
        await self._client.aclose()

    async def send_text(self, text: str) -> None:
        payload: dict[str, object] = {"text": text}
        if self._channel_name:
            payload["channel"] = self._channel_name

        backoff_s = _INITIAL_BACKOFF_S
        last_exc: Optional[BaseException] = None
        last_resp: Optional[httpx.Response] = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = await self._client.post(self._webhook_url, json=payload)
            except (httpx.TimeoutException, httpx.RequestError) as e:
                last_exc = e
                logger.warning(
                    "slack webhook request error; retrying",
                    extra={"attempt": attempt, "error": repr(e)},
                )
                await asyncio.sleep(backoff_s)
                backoff_s = min(_MAX_BACKOFF_S, backoff_s * 2.0)
                continue

            last_resp = resp
            if resp.status_code < 400:
                return

            # Rate limited: respect Retry-After if provided.
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    retry_after_s = float(retry_after) if retry_after is not None else 1.0
                except ValueError:
                    retry_after_s = 1.0
                retry_after_s = max(0.0, min(_MAX_RETRY_AFTER_S, retry_after_s))
                logger.warning(
                    "slack webhook rate limited; retrying",
                    extra={"attempt": attempt, "retry_after_s": retry_after_s},
                )
                await asyncio.sleep(retry_after_s)
                continue

            # Transient server errors.
            if 500 <= resp.status_code < 600:
                logger.warning(
                    "slack webhook server error; retrying",
                    extra={"attempt": attempt, "status_code": resp.status_code},
                )
                await asyncio.sleep(backoff_s)
                backoff_s = min(_MAX_BACKOFF_S, backoff_s * 2.0)
                continue

            # Non-retryable client error.
            try:
                resp.raise_for_status()
            except Exception as e:
                body = (resp.text or "")[:200]
                logger.exception(
                    "slack webhook failed",
                    extra={"status_code": resp.status_code, "body": body},
                )
                raise e

        if last_resp is not None:
            last_resp.raise_for_status()
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("slack webhook failed after retries")
