"""`nwd-dq` command-line interface."""

from __future__ import annotations

import asyncio
import io
import json
import re
import sys
import time
from collections.abc import Callable, Coroutine
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import httpx
import typer

from . import __version__
from .client import ENDPOINT, AsyncDataQueryClient
from .errors import DataQueryError

if TYPE_CHECKING:
    import pyarrow as pa

app = typer.Typer(
    name="nwd-dq",
    help="USACE NWD Dataquery 2.0 CLI.",
    add_completion=True,
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"nwd-dq {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            help="Show the nwd-dq version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """USACE NWD Dataquery 2.0 CLI."""


class OutputFormat(StrEnum):
    """Supported `--format` values for `nwd-dq fetch`."""

    csv = "csv"
    ndjson = "ndjson"
    json = "json"  # alias for ndjson; reserved for a future single-document JSON format
    parquet = "parquet"


_DATETIME_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
]

_DURATION_RE = re.compile(r"^\s*(\d+)\s*([yMwdhm])\s*$")
_DURATION_UNITS = {
    "y": lambda n: timedelta(days=365 * n),
    "M": lambda n: timedelta(days=30 * n),
    "w": lambda n: timedelta(weeks=n),
    "d": lambda n: timedelta(days=n),
    "h": lambda n: timedelta(hours=n),
    "m": lambda n: timedelta(minutes=n),
}

WindowStart = Annotated[
    datetime | None,
    typer.Option(
        help="Window start (inclusive). ISO-8601; UTC if no offset. Defaults to (end - lookback).",
        formats=_DATETIME_FORMATS,
    ),
]
WindowEnd = Annotated[
    datetime | None,
    typer.Option(
        help="Window end (inclusive). ISO-8601; UTC if no offset. Defaults to now.",
        formats=_DATETIME_FORMATS,
    ),
]
LookbackOpt = Annotated[
    str | None,
    typer.Option(
        help=(
            "Window length when --start and/or --end is omitted (e.g. 7d, 48h, 10y). "
            "Default: 7d. Rejected when both --start and --end are given."
        ),
    ),
]
TimeoutOpt = Annotated[float, typer.Option(help="HTTP timeout (seconds).")]
EndpointOpt = Annotated[
    str | None,
    typer.Option(help="Override the Dataquery 2.0 endpoint URL."),
]
QuietOpt = Annotated[bool, typer.Option("-q", "--quiet", help="Suppress warnings.")]
RetriesOpt = Annotated[
    int,
    typer.Option(
        "--retries",
        min=0,
        help="Retry attempts on transport/5xx errors.",
    ),
]


def _require_finite(value: float) -> float:
    """Reject NaN and infinity. Typer's `min=` validator passes both
    (`nan >= 0.0` is False but Click's FloatRange short-circuits the
    isfinite check, and `inf >= 0.0` is True). Without this guard,
    `time.sleep(nan)` would raise ValueError from inside the retry
    handler, escaping the retry loop ungracefully.
    """
    import math

    if not math.isfinite(value):
        raise typer.BadParameter("must be a finite number")
    return value


RetryBackoffOpt = Annotated[
    float,
    typer.Option(
        "--retry-backoff",
        min=0.0,
        callback=_require_finite,
        help="Base seconds for exponential backoff (1s, 2s, 4s, ...).",
    ),
]


def parse_duration(text: str) -> timedelta:
    """Parse simple durations like ``7d``, ``48h``, ``10y``."""
    m = _DURATION_RE.match(text)
    if not m:
        raise ValueError(f"unparseable duration: {text!r}")
    n = int(m.group(1))
    unit = m.group(2)
    return _DURATION_UNITS[unit](n)


def _describe(exc: httpx.TransportError | httpx.HTTPStatusError) -> str:
    """Class-aware human-friendly description of a transport/HTTP failure."""
    if isinstance(exc, httpx.HTTPStatusError):
        host = exc.request.url.host
        msg = f"server returned {exc.response.status_code} from {host}"
        try:
            body = exc.response.json()
            if isinstance(body, dict) and "error" in body:
                msg += f": {body['error']}"
        except ValueError:
            pass
        return msg
    if isinstance(exc, httpx.ConnectTimeout):
        return f"connect timeout to {exc.request.url.host}"
    if isinstance(exc, httpx.ReadTimeout):
        return f"read timeout from {exc.request.url.host}"
    if isinstance(exc, httpx.ConnectError):
        return f"connect failed to {exc.request.url.host}: {exc}"
    return f"transport error: {exc}"


def _client(*, timeout: float, endpoint: str | None) -> AsyncDataQueryClient:
    return AsyncDataQueryClient(
        timeout=timeout,
        endpoint=endpoint or ENDPOINT,
    )


def _run[T](
    coro_factory: Callable[[], Coroutine[Any, Any, T]],
    *,
    retries: int = 0,
    retry_backoff: float = 1.0,
    quiet: bool = False,
) -> T:
    for attempt in range(retries + 1):
        try:
            return asyncio.run(coro_factory())
        except DataQueryError as exc:
            typer.secho(f"server error: {exc}", fg="red", err=True)
            raise typer.Exit(code=2) from exc
        except (httpx.TransportError, httpx.HTTPStatusError) as exc:
            if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code < 500:
                typer.secho(f"error: {_describe(exc)}", fg="red", err=True)
                raise typer.Exit(code=1) from exc
            if attempt < retries:
                delay = retry_backoff * (2**attempt)
                if not quiet:
                    typer.secho(
                        f"warning: {_describe(exc)} — retrying in {delay:g}s "
                        f"(retry {attempt + 1}/{retries})",
                        fg="yellow",
                        err=True,
                    )
                time.sleep(delay)
                continue
            suffix = f" (after {retries} retries)" if retries else ""
            typer.secho(f"error: {_describe(exc)}{suffix}", fg="red", err=True)
            raise typer.Exit(code=1) from exc
        except Exception as exc:
            typer.secho(f"error: {exc}", fg="red", err=True)
            raise typer.Exit(code=1) from exc
    raise RuntimeError("unreachable")  # pragma: no cover


@app.command()
def fetch(
    tsids: Annotated[list[str], typer.Argument(help="One or more CWMS tsids.")],
    start: WindowStart = None,
    end: WindowEnd = None,
    lookback: LookbackOpt = None,
    fmt: Annotated[
        OutputFormat,
        typer.Option(
            "--format",
            "-f",
            help="Output format. `json` is an alias for `ndjson` (newline-delimited).",
        ),
    ] = OutputFormat.csv,
    out: Annotated[
        Path | None,
        typer.Option("--out", "-o", help="Output file. Required for parquet."),
    ] = None,
    no_header: Annotated[
        bool,
        typer.Option(
            "--no-header",
            help="Omit the CSV header row. CSV output only.",
        ),
    ] = False,
    timeout: TimeoutOpt = 60.0,
    endpoint: EndpointOpt = None,
    retries: RetriesOpt = 2,
    retry_backoff: RetryBackoffOpt = 1.0,
    quiet: QuietOpt = False,
    verbose: Annotated[bool, typer.Option("-v", "--verbose", help="Debug logging.")] = False,
    fail_empty: Annotated[
        bool,
        typer.Option("--fail-empty", help="Exit 3 when the result is empty."),
    ] = False,
    strict: Annotated[
        bool,
        typer.Option(
            "--strict",
            hidden=True,
            help="(deprecated) Use --fail-empty instead.",
        ),
    ] = False,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="Keep only the most recent row per tsid."),
    ] = False,
) -> None:
    """Fetch observations for one or more tsids."""
    if fmt == OutputFormat.parquet and out is None:
        typer.secho("error: --format parquet requires --out PATH", fg="red", err=True)
        raise typer.Exit(code=2)

    if no_header and fmt != OutputFormat.csv:
        typer.secho(
            f"error: --no-header only applies to --format csv (got {fmt.value})",
            fg="red",
            err=True,
        )
        raise typer.Exit(code=2)

    lb = _resolve_window_args(start, end, lookback)

    if quiet:
        import warnings

        from .errors import UnknownTsidWarning

        warnings.simplefilter("ignore", UnknownTsidWarning)

    if verbose:
        import logging

        logging.basicConfig(level=logging.DEBUG)

    async def _do() -> pa.Table:
        async with _client(timeout=timeout, endpoint=endpoint) as client:
            return await client.fetch(tsids, start=start, end=end, lookback=lb)

    table = _run(_do, retries=retries, retry_backoff=retry_backoff, quiet=quiet)

    if latest:
        table = _latest_per_tsid(table)

    if strict and not quiet:
        typer.secho(
            "warning: --strict is deprecated; use --fail-empty instead.",
            fg="yellow",
            err=True,
        )
    if (fail_empty or strict) and table.num_rows == 0:
        raise typer.Exit(code=3)

    _write(table, fmt, out, include_header=not no_header)


def _latest_per_tsid(table: pa.Table) -> pa.Table:
    import pyarrow.compute as pc

    if table.num_rows == 0:
        return table
    order = pc.sort_indices(  # ty:ignore[unresolved-attribute]
        table,
        sort_keys=[("tsid", "ascending"), ("timestamp", "descending")],
    )
    sorted_tbl = table.take(order)
    tsids = sorted_tbl["tsid"].to_pylist()
    keep = [i for i in range(len(tsids)) if i == 0 or tsids[i] != tsids[i - 1]]
    return sorted_tbl.take(keep)


@app.command()
def describe(
    tsids: Annotated[list[str], typer.Argument(help="One or more CWMS tsids.")],
    start: WindowStart = None,
    end: WindowEnd = None,
    lookback: LookbackOpt = None,
    timeout: TimeoutOpt = 60.0,
    endpoint: EndpointOpt = None,
    retries: RetriesOpt = 2,
    retry_backoff: RetryBackoffOpt = 1.0,
    quiet: QuietOpt = False,
) -> None:
    """Emit location + tsid metadata as JSON (no values)."""
    lb = _resolve_window_args(start, end, lookback)

    if quiet:
        import warnings

        from .errors import UnknownTsidWarning

        warnings.simplefilter("ignore", UnknownTsidWarning)

    async def _do() -> dict:
        async with _client(timeout=timeout, endpoint=endpoint) as client:
            return await client.describe(tsids, start=start, end=end, lookback=lb)

    meta = _run(_do, retries=retries, retry_backoff=retry_backoff, quiet=quiet)

    typer.echo(json.dumps(meta, indent=2, default=str))


@app.command()
def raw(
    tsids: Annotated[list[str], typer.Argument(help="One or more CWMS tsids.")],
    start: WindowStart = None,
    end: WindowEnd = None,
    lookback: LookbackOpt = None,
    out: Annotated[
        Path | None,
        typer.Option("--out", "-o", help="Output file. Defaults to stdout."),
    ] = None,
    timeout: TimeoutOpt = 60.0,
    endpoint: EndpointOpt = None,
    retries: RetriesOpt = 2,
    retry_backoff: RetryBackoffOpt = 1.0,
    quiet: QuietOpt = False,
) -> None:
    """Print the raw upstream JSON payload for one or more tsids."""
    lb = _resolve_window_args(start, end, lookback)

    if quiet:
        import warnings

        from .errors import UnknownTsidWarning

        warnings.simplefilter("ignore", UnknownTsidWarning)

    async def _do() -> dict:
        async with _client(timeout=timeout, endpoint=endpoint) as client:
            return await client.fetch_raw(tsids, start=start, end=end, lookback=lb)

    payload = _run(_do, retries=retries, retry_backoff=retry_backoff, quiet=quiet)

    text = json.dumps(payload, indent=2, default=str)
    if out is not None:
        out.write_text(text + "\n")
    else:
        typer.echo(text)


def _resolve_window_args(
    start: datetime | None,
    end: datetime | None,
    lookback: str | None,
) -> timedelta | None:
    """Validate window args and parse `lookback` into a timedelta.

    Rejects the overspecified case (`--start`, `--end`, and `--lookback` all given)
    with `typer.Exit(code=2)`. Returns `None` when `lookback` is omitted so the
    library layer can apply its own default.
    """
    if start is not None and end is not None and lookback is not None:
        typer.secho(
            "error: --lookback cannot be combined with both --start and --end",
            fg="red",
            err=True,
        )
        raise typer.Exit(code=2)
    if start is not None and end is not None and start > end:
        typer.secho(
            f"error: --start ({start.isoformat()}) is after --end ({end.isoformat()})",
            fg="red",
            err=True,
        )
        raise typer.Exit(code=2)
    if lookback is None:
        return None
    try:
        return parse_duration(lookback)
    except ValueError as exc:
        typer.secho(f"error: {exc}", fg="red", err=True)
        raise typer.Exit(code=2) from exc


def _write(
    table: pa.Table,
    fmt: OutputFormat | str,
    out: Path | None,
    *,
    include_header: bool = True,
) -> None:
    if fmt == OutputFormat.csv:
        import pyarrow.csv as pa_csv

        write_options = pa_csv.WriteOptions(include_header=include_header)
        if out is not None:
            with out.open("wb") as f:
                pa_csv.write_csv(table, f, write_options)
        else:
            buf = io.BytesIO()
            pa_csv.write_csv(table, buf, write_options)
            sys.stdout.buffer.write(buf.getvalue())
    elif fmt in (OutputFormat.ndjson, OutputFormat.json):
        # NDJSON: one object per row. `json` is currently an alias.
        sink = out.open("w") if out is not None else sys.stdout
        try:
            for batch in table.to_batches():
                rows = batch.to_pylist()
                for row in rows:
                    # Normalize datetime objects for JSON
                    norm = {
                        k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()
                    }
                    sink.write(json.dumps(norm) + "\n")
        finally:
            if out is not None:
                sink.close()
    elif fmt == OutputFormat.parquet:
        import pyarrow.parquet as pa_pq

        if out is None:
            raise ValueError("parquet output requires a file path")
        pa_pq.write_table(table, out)
    else:
        typer.secho(f"error: unknown --format {fmt!r}", fg="red", err=True)
        raise typer.Exit(code=2)
