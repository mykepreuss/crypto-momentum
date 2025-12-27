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


@respx.mock
async def test_slack_webhook_retries_on_rate_limit() -> None:
    notifier = SlackWebhookNotifier("https://hooks.slack.test/services/abc", channel_name="#alerts")
    try:
        route = respx.post("https://hooks.slack.test/services/abc").mock(
            side_effect=[
                Response(429, headers={"Retry-After": "0"}),
                Response(200),
            ]
        )
        await notifier.send_text("hello")
        assert route.call_count == 2
    finally:
        await notifier.aclose()
