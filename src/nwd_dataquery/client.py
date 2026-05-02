"""Async client for USACE NWD Dataquery 2.0."""

from __future__ import annotations

import json
import logging
import ssl
import warnings
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import cache
from typing import TYPE_CHECKING, Any, TypedDict
from urllib.parse import urlsplit

import httpx
from aia_chaser import AiaChaser

from ._time import is_window_inverted as _is_window_inverted
from ._time import to_utc as _to_utc
from .errors import DataQueryError, UnknownTsidWarning

if TYPE_CHECKING:
    from ._results import MetadataResult, QueryResult


class TimeseriesEntry(TypedDict, total=False):
    """Per-tsid body in the upstream payload.

    All fields optional (the upstream may omit any of them depending on the
    response). `values` is a list of `[timestamp_iso, value, quality]` triples.
    """

    parameter: str
    units: str
    values: list[list[Any]]
    count: int
    start_timestamp: str
    end_timestamp: str


class LocationEntry(TypedDict, total=False):
    """Per-location body in the upstream payload.

    The upstream returns additional fields (coordinates, elevation, etc.) that
    are not part of the typed contract; access them via `.get(...)` or `cast()`.
    """

    name: str
    timeseries: dict[str, TimeseriesEntry]


# Public type alias for the upstream JSON shape. `describe()` returns the same
# shape with per-timeseries `values` stripped — the `total=False` on
# TimeseriesEntry permits this.
DataQueryPayload = dict[str, LocationEntry]

logger = logging.getLogger(__name__)

ENDPOINT = "https://www.nwd-wc.usace.army.mil/dd/common/web_service/webexec/getjson"
DEFAULT_LOOKBACK = timedelta(days=7)


@dataclass(frozen=True, slots=True)
class _ExecuteOutcome:
    """Internal: the shared output of the request prelude.

    Carries the four pieces of context that all three public methods
    (``fetch_raw``, ``fetch``, ``describe``) need to construct their return
    shape: the decoded payload, the normalized tsid tuple, the resolved UTC
    window, and any warnings emitted during the call.
    """

    payload: DataQueryPayload
    requested_tsids: tuple[str, ...]
    resolved_window: tuple[datetime, datetime]
    warnings: tuple[Warning, ...]


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

    async def _request_payload(
        self,
        tsids: list[str],
        *,
        resolved_start: datetime,
        resolved_end: datetime,
    ) -> DataQueryPayload:
        """Issue the HTTP request and return the decoded payload.

        Caller is responsible for: tsid validation, window resolution,
        and any post-decode warning emission. This helper handles only
        URL construction, HTTP, JSON decode, and DataQueryError /
        HTTPStatusError surfacing.
        """
        params: dict[str, str] = {
            "timezone": "GMT",
            "query": json.dumps(tsids),
            "startdate": _iso(resolved_start),
            "enddate": _iso(resolved_end),
        }

        session = self._get_or_build_session()
        response = await session.get(self.endpoint, params=params)

        try:
            payload = response.json()
        except ValueError:
            # Body wasn't JSON at all — let an HTTP error take precedence,
            # otherwise re-raise the decode failure.
            response.raise_for_status()
            raise

        # Surface server-reported errors as DataQueryError, but only for
        # non-5xx responses. A 5xx with an error body is a transient server
        # failure dressed up with a message — let it raise HTTPStatusError so
        # callers can retry. The original message remains on
        # exc.response.json() for diagnostic display.
        if isinstance(payload, dict) and "error" in payload and response.status_code < 500:
            raise DataQueryError(payload["error"])

        response.raise_for_status()

        if not isinstance(payload, dict):
            raise DataQueryError(
                f"unexpected response payload: expected JSON object, got {type(payload).__name__}"
            )

        return payload

    def _resolve_window(
        self,
        start: datetime | None,
        end: datetime | None,
        lookback: timedelta | None,
    ) -> tuple[datetime, datetime]:
        """Validate and resolve the (start, end) window. Both returned datetimes are UTC-aware."""
        if start is not None and end is not None and lookback is not None:
            raise ValueError("lookback cannot be combined with both start and end")
        if lookback is not None and lookback < timedelta(0):
            raise ValueError(f"lookback must be non-negative, got {lookback!r}")
        if start is not None and end is not None and _is_window_inverted(start, end):
            start_utc = _to_utc(start)
            end_utc = _to_utc(end)
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

        if start is None or end is None:  # pragma: no cover
            raise RuntimeError("internal: failed to resolve both start and end after defaults")
        resolved_start = _to_utc(start)
        resolved_end = _to_utc(end)
        if resolved_start > resolved_end:
            raise ValueError(
                f"resolved window is inverted: start ({resolved_start.isoformat()}) "
                f"is after end ({resolved_end.isoformat()})"
            )
        return resolved_start, resolved_end

    async def _execute(
        self,
        tsids: str | Sequence[str],
        *,
        start: datetime | None,
        end: datetime | None,
        lookback: timedelta | None,
    ) -> _ExecuteOutcome:
        """Shared prelude for every public fetch method.

        Normalizes the tsids argument, resolves the window, issues the request,
        and emits/captures the empty-payload warning. Public methods compose
        the returned outcome into their own return shape.

        ``stacklevel=3`` on the warning skips this method and the public method
        that called it, landing on the user's call site.
        """
        if isinstance(tsids, str):
            tsids = [tsids]
        tsids = list(tsids)
        if not tsids:
            raise ValueError("must provide at least one tsid")

        resolved_start, resolved_end = self._resolve_window(start, end, lookback)

        payload = await self._request_payload(
            tsids, resolved_start=resolved_start, resolved_end=resolved_end
        )

        captured: list[Warning] = []
        if not payload:
            w = UnknownTsidWarning(
                f"Empty response for {tsids!r} — tsid unknown, or no data in window"
            )
            warnings.warn(w, stacklevel=3)
            captured.append(w)

        return _ExecuteOutcome(
            payload=payload,
            requested_tsids=tuple(tsids),
            resolved_window=(resolved_start, resolved_end),
            warnings=tuple(captured),
        )

    async def fetch_raw(
        self,
        tsids: str | Sequence[str],
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        lookback: timedelta | None = None,
    ) -> DataQueryPayload:
        """Return the raw JSON payload for the given tsid(s)."""
        outcome = await self._execute(tsids, start=start, end=end, lookback=lookback)
        return outcome.payload

    async def fetch(
        self,
        tsids: str | Sequence[str],
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        lookback: timedelta | None = None,
    ) -> QueryResult:
        """Return a QueryResult containing the parsed table, raw payload, and request context.

        Columns in `result.table`: ``timestamp`` (UTC), ``value``, ``quality``,
        ``tsid``, ``location``, ``parameter``, ``units``.
        """
        from ._parse import parse_payload
        from ._results import QueryResult

        outcome = await self._execute(tsids, start=start, end=end, lookback=lookback)
        return QueryResult(
            table=parse_payload(outcome.payload),
            payload=outcome.payload,
            requested_tsids=outcome.requested_tsids,
            resolved_window=outcome.resolved_window,
            endpoint=self.endpoint,
            warnings=outcome.warnings,
        )

    async def describe(
        self,
        tsids: str | Sequence[str],
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        lookback: timedelta | None = None,
    ) -> MetadataResult:
        """Return a MetadataResult: per-location and per-timeseries metadata,
        with `values` arrays stripped from each timeseries body.
        """
        from ._results import MetadataResult

        outcome = await self._execute(tsids, start=start, end=end, lookback=lookback)
        return MetadataResult.from_payload(
            outcome.payload,
            requested_tsids=outcome.requested_tsids,
            resolved_window=outcome.resolved_window,
            endpoint=self.endpoint,
            warnings=outcome.warnings,
        )


def _iso(dt: datetime) -> str:
    """Format a datetime as ISO-8601 with a Z suffix (server-accepted)."""
    return _to_utc(dt).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
