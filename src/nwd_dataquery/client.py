"""Async client for USACE NWD Dataquery 2.0."""

from __future__ import annotations

import json
import logging
import warnings
from datetime import datetime, timedelta
from datetime import timezone as _tz
from typing import Any, Sequence

import httpx

from .errors import DataQueryError, UnknownTsidWarning

logger = logging.getLogger(__name__)

ENDPOINT = "https://www.nwd-wc.usace.army.mil/dd/common/web_service/webexec/getjson"
DEFAULT_LOOKBACK = timedelta(days=7)


class AsyncDataQueryClient:
    """Async client for the USACE NWD Dataquery 2.0 ``getjson`` endpoint.

    Thin transport over the one public endpoint. No retries, caching, or
    rate limiting — callers compose those themselves.
    """

    def __init__(
        self,
        *,
        endpoint: str = ENDPOINT,
        timezone: str = "GMT",
        timeout: float = 60.0,
        session: httpx.AsyncClient | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.timezone = timezone
        self.timeout = timeout
        self._session = session
        self._owns_session = session is None

    async def __aenter__(self) -> "AsyncDataQueryClient":
        self._session = self._get_or_build_session()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._session is not None and self._owns_session:
            await self._session.aclose()
            self._session = None

    def _get_or_build_session(self) -> httpx.AsyncClient:
        if self._session is None:
            self._session = httpx.AsyncClient(timeout=self.timeout)
        return self._session

    async def fetch_raw(
        self,
        tsids: str | Sequence[str],
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        lookback: timedelta = DEFAULT_LOOKBACK,
    ) -> dict[str, Any]:
        """Return the raw JSON payload for the given tsid(s)."""
        if isinstance(tsids, str):
            tsids = [tsids]
        tsids = list(tsids)
        if not tsids:
            raise ValueError("must provide at least one tsid")

        if start is None and end is None:
            end = datetime.now(_tz.utc)
            start = end - lookback
        elif start is None:
            assert end is not None
            start = end - lookback

        params: dict[str, str] = {
            "timezone": self.timezone,
            "query": json.dumps(tsids),
        }
        if start is not None:
            params["startdate"] = _iso(start)
        if end is not None:
            params["enddate"] = _iso(end)

        session = self._get_or_build_session()
        response = await session.get(self.endpoint, params=params)

        if response.headers.get("content-type", "").startswith("text/plain"):
            try:
                body = response.json()
            except ValueError:
                body = None
            if isinstance(body, dict) and "error" in body:
                raise DataQueryError(body["error"])

        response.raise_for_status()
        payload: dict[str, Any] = response.json()

        if not payload:
            warnings.warn(
                f"Empty response for {tsids!r} — tsid unknown, or no data in window",
                UnknownTsidWarning,
                stacklevel=2,
            )
        return payload


def _iso(dt: datetime) -> str:
    """Format a datetime as ISO-8601 with a Z suffix (server-accepted)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_tz.utc)
    return dt.astimezone(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
