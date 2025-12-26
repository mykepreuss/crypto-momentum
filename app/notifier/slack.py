from __future__ import annotations

import logging
from typing import Optional

import httpx

from app.notifier.base import Notifier

logger = logging.getLogger(__name__)


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
        resp = await self._client.post(self._webhook_url, json=payload)
        try:
            resp.raise_for_status()
        except Exception:
            logger.exception("slack webhook failed", extra={"status_code": resp.status_code})
            raise
