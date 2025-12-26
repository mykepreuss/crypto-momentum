from __future__ import annotations

import respx
from httpx import Response

from app.config import Settings
from app.exchange_client import CryptoComExchangeClient


@respx.mock
async def test_get_instruments_parses_result() -> None:
    # Regression: base_url contains a path segment ("/exchange/v1"), so requests must preserve it.
    settings = Settings(cryptocom_exchange_base_url="https://example.test/exchange/v1", max_concurrent_requests=1)
    client = CryptoComExchangeClient.from_settings(settings)
    try:
        respx.get("https://example.test/exchange/v1/public/get-instruments").mock(
            return_value=Response(
                200,
                json={
                    "code": 0,
                    "result": {"data": [{"symbol": "BTC_USDT", "tradable": True}]},
                },
            )
        )
        instruments = await client.get_instruments()
        assert instruments[0]["symbol"] == "BTC_USDT"
    finally:
        await client.aclose()


@respx.mock
async def test_get_book_accepts_list_shape() -> None:
    settings = Settings(cryptocom_exchange_base_url="https://example.test/exchange/v1", max_concurrent_requests=1)
    client = CryptoComExchangeClient.from_settings(settings)
    try:
        respx.get("https://example.test/exchange/v1/public/get-book").mock(
            return_value=Response(
                200,
                json={
                    "code": 0,
                    "result": {"data": [{"bids": [["1", "2"]], "asks": [["3", "4"]]}]},
                },
            )
        )
        book = await client.get_book("BTC_USDT", depth=10)
        assert "bids" in book and "asks" in book
    finally:
        await client.aclose()


@respx.mock
async def test_accepts_code_string_zero() -> None:
    settings = Settings(cryptocom_exchange_base_url="https://example.test/exchange/v1", max_concurrent_requests=1)
    client = CryptoComExchangeClient.from_settings(settings)
    try:
        respx.get("https://example.test/exchange/v1/public/get-instruments").mock(
            return_value=Response(
                200,
                json={
                    "code": "0",
                    "result": {"data": [{"symbol": "BTC_USDT", "tradable": True}]},
                },
            )
        )
        instruments = await client.get_instruments()
        assert instruments[0]["symbol"] == "BTC_USDT"
    finally:
        await client.aclose()
