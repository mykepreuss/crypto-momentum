from __future__ import annotations

import respx
from httpx import Response

from app.notifier.slack import SlackWebhookNotifier


@respx.mock
async def test_slack_webhook_posts_text() -> None:
    notifier = SlackWebhookNotifier("https://hooks.slack.test/services/abc", channel_name="#alerts")
    try:
        route = respx.post("https://hooks.slack.test/services/abc").mock(return_value=Response(200))
        await notifier.send_text("hello")
        assert route.called
    finally:
        await notifier.aclose()

