"""Async client for USACE NWD Dataquery 2.0."""

from __future__ import annotations

import json
import logging
import ssl
import warnings
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from functools import cache
from typing import Any, Literal
from urllib.parse import urlsplit

import httpx
from aia_chaser import AiaChaser

from ._parse import parse_payload
from .errors import DataQueryError, UnknownTsidWarning

logger = logging.getLogger(__name__)

ENDPOINT = "https://www.nwd-wc.usace.army.mil/dd/common/web_service/webexec/getjson"
DEFAULT_LOOKBACK = timedelta(days=7)


def _ssl_context_for(endpoint: str) -> ssl.SSLContext:
    # USACE serves the leaf cert without the DigiCert intermediate, so Python's
    # ssl module can't build a chain on its own. Fetch the intermediate via the
    # leaf's AIA extension and cache the resulting context per origin
    # (scheme://host:port) — the path is irrelevant to the TLS handshake, so
    # different paths on the same host should share one context.
    parts = urlsplit(endpoint)
    return _build_ssl_context(f"{parts.scheme}://{parts.netloc}")


@cache
def _build_ssl_context(origin: str) -> ssl.SSLContext:
    return AiaChaser().make_ssl_context_for_url(origin)


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

    async def __aenter__(self) -> AsyncDataQueryClient:
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
            kwargs: dict[str, Any] = {"timeout": self.timeout}
            if self.endpoint.startswith("https://"):
                kwargs["verify"] = _ssl_context_for(self.endpoint)
            self._session = httpx.AsyncClient(**kwargs)
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
            end = datetime.now(UTC)
            start = end - lookback
        elif start is None and end is not None:
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

    async def fetch(
        self,
        tsids: str | Sequence[str],
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        lookback: timedelta = DEFAULT_LOOKBACK,
        backend: Literal["pyarrow", "polars", "pandas"] = "pyarrow",
    ) -> Any:
        """Return a long-format frame in the requested backend.

        Columns: ``timestamp`` (UTC), ``value``, ``quality``, ``tsid``,
        ``location``, ``parameter``, ``units``.
        """
        payload = await self.fetch_raw(tsids, start=start, end=end, lookback=lookback)
        table = parse_payload(payload)
        if backend == "pyarrow":
            return table
        if backend == "polars":
            import polars as pl

            return pl.from_arrow(table)
        if backend == "pandas":
            return table.to_pandas()
        raise ValueError(f"unknown backend: {backend!r}")

    async def describe(
        self,
        tsids: str | Sequence[str],
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        lookback: timedelta = DEFAULT_LOOKBACK,
    ) -> dict[str, Any]:
        """Return location + tsid metadata without the time series values."""
        payload = await self.fetch_raw(tsids, start=start, end=end, lookback=lookback)
        return {
            loc: {k: v for k, v in body.items() if k != "timeseries"}
            | {
                "timeseries": {
                    t: {k: v for k, v in tb.items() if k != "values"}
                    for t, tb in (body.get("timeseries") or {}).items()
                    if isinstance(tb, dict)
                }
            }
            for loc, body in payload.items()
            if isinstance(body, dict)
        }


def _iso(dt: datetime) -> str:
    """Format a datetime as ISO-8601 with a Z suffix (server-accepted)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
