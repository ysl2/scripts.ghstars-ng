from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import aiohttp


HTTP_TOTAL_TIMEOUT = 20
HTTP_CONNECT_TIMEOUT = 10
MAX_RETRIES = 2


class RateLimiter:
    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self.last_request_time = 0.0
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        loop = asyncio.get_event_loop()
        async with self.lock:
            now = loop.time()
            wait_until = max(now, self.last_request_time + self.min_interval)
            self.last_request_time = wait_until

        delay = wait_until - now
        if delay > 0:
            await asyncio.sleep(delay)


def build_timeout() -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(total=HTTP_TOTAL_TIMEOUT, connect=HTTP_CONNECT_TIMEOUT)


def _retry_delay_seconds(attempt: int, response_headers: Mapping[str, str] | None = None) -> float:
    base_delay = 0.5 * (2**attempt)
    if not response_headers:
        return base_delay
    retry_after = (response_headers.get("Retry-After") or "").strip()
    if not retry_after:
        return base_delay
    try:
        return max(base_delay, float(retry_after))
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(retry_after)
    except (TypeError, ValueError, IndexError, OverflowError):
        return base_delay
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    return max(base_delay, max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds()))


async def request_text(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, str] | None = None,
    semaphore: asyncio.Semaphore,
    rate_limiter: RateLimiter,
    retry_prefix: str,
    allowed_statuses: set[int] | None = None,
    max_retries: int | None = None,
) -> tuple[int | None, str | None, dict[str, str], str | None]:
    allowed = allowed_statuses or set()
    retry_limit = MAX_RETRIES if max_retries is None else max(0, max_retries)
    for attempt in range(retry_limit + 1):
        async with semaphore:
            await rate_limiter.acquire()
            try:
                async with session.get(url, headers=headers, params=params) as response:
                    response_headers = dict(response.headers)
                    body = await response.text()
                    if response.status == 200 or response.status in allowed:
                        return response.status, body, response_headers, None
                    if response.status in {429, 500, 502, 503, 504} and attempt < retry_limit:
                        await asyncio.sleep(_retry_delay_seconds(attempt, response_headers))
                        continue
                    return response.status, body, response_headers, f"{retry_prefix} error ({response.status})"
            except asyncio.TimeoutError:
                if attempt < retry_limit:
                    await asyncio.sleep(0.5 * (2**attempt))
                    continue
                return None, None, {}, f"{retry_prefix} timeout"
            except Exception as exc:
                if attempt < retry_limit:
                    await asyncio.sleep(0.5 * (2**attempt))
                    continue
                return None, None, {}, f"{retry_prefix} request failed: {exc}"
    return None, None, {}, f"{retry_prefix} error"
