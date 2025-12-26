from __future__ import annotations

from app.config import Settings
from app.notifier.base import Notifier, NullNotifier
from app.notifier.slack import SlackWebhookNotifier


def build_notifier(settings: Settings) -> Notifier:
    if settings.slack_webhook_url:
        return SlackWebhookNotifier(
            webhook_url=str(settings.slack_webhook_url),
            channel_name=settings.slack_channel_name,
        )
    return NullNotifier()

