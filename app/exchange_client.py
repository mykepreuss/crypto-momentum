from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Optional

import httpx
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from app.config import Settings

logger = logging.getLogger(__name__)


class CryptoComAPIError(RuntimeError):
    pass


class CryptoComRateLimitError(CryptoComAPIError):
    pass


class CryptoComBadResponseError(CryptoComAPIError):
    pass


def _log_retry(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning("cryptocom request failed; retrying", extra={"exc": repr(exc)})


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, (httpx.RequestError, CryptoComRateLimitError, CryptoComBadResponseError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return 500 <= status < 600
    return False


@dataclass(frozen=True)
class CryptoComExchangeClient:
    _client: httpx.AsyncClient
    _semaphore: asyncio.Semaphore

    @classmethod
    def from_settings(cls, settings: Settings) -> "CryptoComExchangeClient":
        timeout = httpx.Timeout(settings.http_timeout_s)
        # httpx base_url joining follows RFC 3986. If base_url does not end with a trailing slash,
        # relative paths can replace the final segment (e.g. ".../v1" + "public/get-*" => ".../public/get-*").
        # Also, a leading slash in the request path overrides the base_url path entirely.
        base_url = str(settings.cryptocom_exchange_base_url).rstrip("/") + "/"
        client = httpx.AsyncClient(base_url=base_url, timeout=timeout)
        return cls(client, asyncio.Semaphore(settings.max_concurrent_requests))

    async def aclose(self) -> None:
        await self._client.aclose()

    @retry(
        retry=retry_if_exception(_should_retry),
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=0.5, max=8.0),
        before_sleep=_log_retry,
        reraise=True,
    )
    async def _request(
        self,
        method: str,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        method = method.lstrip("/")
        async with self._semaphore:
            resp = await self._client.get(method, params=params)

        if resp.status_code == 429:
            raise CryptoComRateLimitError(f"rate limited calling {method}")
        resp.raise_for_status()

        try:
            payload = resp.json()
        except ValueError as e:
            raise CryptoComBadResponseError(f"invalid JSON calling {method}: {e}") from e

        if not isinstance(payload, dict):
            raise CryptoComBadResponseError(f"unexpected JSON type calling {method}: payload={payload!r}")

        code = payload.get("code")
        if code not in (None, 0, "0"):
            raise CryptoComAPIError(f"non-zero code calling {method}: {code} payload={payload!r}")
        if "result" not in payload:
            raise CryptoComBadResponseError(f"missing result calling {method}: payload={payload!r}")
        return payload["result"]

    async def get_instruments(self) -> list[dict[str, Any]]:
        result = await self._request("public/get-instruments")
        if "data" in result:
            instruments = result.get("data")
        elif "instruments" in result:
            instruments = result.get("instruments")
        else:
            instruments = None
        if not isinstance(instruments, list):
            raise CryptoComAPIError(f"unexpected instruments payload: {result!r}")
        return instruments

    async def get_tickers(self) -> list[dict[str, Any]]:
        result = await self._request("public/get-tickers")
        data = result.get("data")
        if not isinstance(data, list):
            raise CryptoComAPIError(f"unexpected tickers payload: {result!r}")
        return data

    async def get_candlestick(
        self,
        instrument_name: str,
        timeframe: str = "1m",
        count: int = 2,
        *,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "instrument_name": instrument_name,
            "timeframe": timeframe,
            "count": count,
        }
        if start_ts is not None:
            params["start_ts"] = int(start_ts)
        if end_ts is not None:
            params["end_ts"] = int(end_ts)
        result = await self._request(
            "public/get-candlestick",
            params=params,
        )
        data = result.get("data")
        if not isinstance(data, list):
            raise CryptoComAPIError(f"unexpected candlestick payload: {result!r}")
        return data

    async def get_book(self, instrument_name: str, depth: int = 10) -> dict[str, Any]:
        result = await self._request(
            "public/get-book",
            params={"instrument_name": instrument_name, "depth": depth},
        )
        data = result.get("data")
        if isinstance(data, list) and data:
            # API sometimes returns a list of books
            return data[0]
        if isinstance(data, dict):
            return data
        raise CryptoComAPIError(f"unexpected book payload: {result!r}")
