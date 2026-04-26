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
        timeout: float = 60.0,
        session: httpx.AsyncClient | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.timeout = timeout
        self._session = session
        self._owns_session = session is None

    async def __aenter__(self) -> AsyncDataQueryClient:
        # Session creation is deferred to the first request so that simply
        # entering the context manager does not trigger an AIA fetch for
        # HTTPS endpoints — important for tests that mock `fetch()` and for
        # callers that may abandon the context without making a request.
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
        lookback: timedelta | None = None,
    ) -> dict[str, Any]:
        """Return the raw JSON payload for the given tsid(s)."""
        if isinstance(tsids, str):
            tsids = [tsids]
        tsids = list(tsids)
        if not tsids:
            raise ValueError("must provide at least one tsid")

        if start is not None and end is not None and lookback is not None:
            raise ValueError("lookback cannot be combined with both start and end")
        if start is not None and end is not None:
            start_utc = start.replace(tzinfo=UTC) if start.tzinfo is None else start
            end_utc = end.replace(tzinfo=UTC) if end.tzinfo is None else end
            if start_utc > end_utc:
                raise ValueError(
                    f"start ({start_utc.isoformat()}) is after end ({end_utc.isoformat()})"
                )
        if lookback is None:
            lookback = DEFAULT_LOOKBACK

        if start is None and end is None:
            end = datetime.now(UTC)
            start = end - lookback
        elif start is None and end is not None:
            start = end - lookback
        elif start is not None and end is None:
            end = datetime.now(UTC)

        # "GMT" is the only request value that produces timestamps consistent
        # with the parser's UTC assumption. The upstream silently falls back to
        # local-sensor time on unknown timezone strings (including "UTC"); see
        # docs/superpowers/specs/2026-04-26-drop-timezone-knob-design.md.
        params: dict[str, str] = {
            "timezone": "GMT",
            "query": json.dumps(tsids),
        }
        if start is not None:
            params["startdate"] = _iso(start)
        if end is not None:
            params["enddate"] = _iso(end)

        session = self._get_or_build_session()
        response = await session.get(self.endpoint, params=params)

        try:
            payload = response.json()
        except ValueError:
            # Body wasn't JSON at all — let an HTTP error take precedence,
            # otherwise re-raise the decode failure.
            response.raise_for_status()
            raise

        # Server-reported errors carry an actionable message; surface those
        # before raising for HTTP status so the message isn't lost behind a
        # generic 5xx.
        if isinstance(payload, dict) and "error" in payload:
            raise DataQueryError(payload["error"])

        response.raise_for_status()

        if not isinstance(payload, dict):
            raise DataQueryError(
                f"unexpected response payload: expected JSON object, got {type(payload).__name__}"
            )

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
        lookback: timedelta | None = None,
        backend: Literal["pyarrow", "polars", "pandas"] = "pyarrow",
    ) -> Any:
        """Return a long-format frame in the requested backend.

        Columns: ``timestamp`` (UTC), ``value``, ``quality``, ``tsid``,
        ``location``, ``parameter``, ``units``.
        """
        # Imported lazily so that `import nwd_dataquery` does not pull in
        # pyarrow on the version-check / metadata-only paths.
        from ._parse import parse_payload

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
        lookback: timedelta | None = None,
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
