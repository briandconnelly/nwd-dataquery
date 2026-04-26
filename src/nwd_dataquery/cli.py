"""`nwd-dq` command-line interface."""

from __future__ import annotations

import asyncio
import io
import json
import re
import sys
from collections.abc import Callable, Coroutine
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import typer

from . import __version__
from .client import ENDPOINT, AsyncDataQueryClient
from .errors import DataQueryError

if TYPE_CHECKING:
    import pyarrow as pa

app = typer.Typer(
    name="nwd-dq",
    help="USACE NWD Dataquery 2.0 CLI.",
    add_completion=False,
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
TimezoneOpt = Annotated[str, typer.Option(help="Server timezone bucketing.")]
TimeoutOpt = Annotated[float, typer.Option(help="HTTP timeout (seconds).")]
EndpointOpt = Annotated[
    str | None,
    typer.Option(help="Override the Dataquery 2.0 endpoint URL."),
]


def parse_duration(text: str) -> timedelta:
    """Parse simple durations like ``7d``, ``48h``, ``10y``."""
    m = _DURATION_RE.match(text)
    if not m:
        raise ValueError(f"unparseable duration: {text!r}")
    n = int(m.group(1))
    unit = m.group(2)
    return _DURATION_UNITS[unit](n)


def _client(*, timeout: float, timezone: str, endpoint: str | None) -> AsyncDataQueryClient:
    return AsyncDataQueryClient(
        timeout=timeout,
        timezone=timezone,
        endpoint=endpoint or ENDPOINT,
    )


def _run[T](coro_factory: Callable[[], Coroutine[Any, Any, T]]) -> T:
    try:
        return asyncio.run(coro_factory())
    except DataQueryError as exc:
        typer.secho(f"server error: {exc}", fg="red", err=True)
        raise typer.Exit(code=2) from exc
    except Exception as exc:
        typer.secho(f"error: {exc}", fg="red", err=True)
        raise typer.Exit(code=1) from exc


@app.command()
def fetch(
    tsids: Annotated[list[str], typer.Argument(help="One or more CWMS tsids.")],
    start: WindowStart = None,
    end: WindowEnd = None,
    lookback: LookbackOpt = None,
    timezone: TimezoneOpt = "GMT",
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
    quiet: Annotated[bool, typer.Option("-q", "--quiet", help="Suppress warnings.")] = False,
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
        async with _client(timeout=timeout, timezone=timezone, endpoint=endpoint) as client:
            return await client.fetch(tsids, start=start, end=end, lookback=lb)

    table = _run(_do)

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
    start: Annotated[
        datetime | None,
        typer.Option(
            help="Window start (inclusive). ISO-8601; UTC if no offset. Defaults to (end - lookback).",
            formats=_DATETIME_FORMATS,
        ),
    ] = None,
    end: Annotated[
        datetime | None,
        typer.Option(
            help="Window end (inclusive). ISO-8601; UTC if no offset. Defaults to now.",
            formats=_DATETIME_FORMATS,
        ),
    ] = None,
    lookback: Annotated[
        str | None,
        typer.Option(
            help=(
                "Window length when --start and/or --end is omitted (e.g. 7d, 48h, 10y). "
                "Default: 7d. Rejected when both --start and --end are given."
            ),
        ),
    ] = None,
    timezone: Annotated[str, typer.Option()] = "GMT",
    timeout: Annotated[float, typer.Option()] = 60.0,
    endpoint: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Emit location + tsid metadata as JSON (no values)."""
    lb = _resolve_window_args(start, end, lookback)

    async def _run() -> dict:
        async with AsyncDataQueryClient(
            timeout=timeout,
            timezone=timezone,
            endpoint=endpoint or ENDPOINT,
        ) as client:
            return await client.describe(tsids, start=start, end=end, lookback=lb)

    try:
        meta = asyncio.run(_run())
    except Exception as exc:
        from .errors import DataQueryError

        if isinstance(exc, DataQueryError):
            typer.secho(f"server error: {exc}", fg="red", err=True)
            raise typer.Exit(code=2) from exc
        typer.secho(f"error: {exc}", fg="red", err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(json.dumps(meta, indent=2, default=str))


@app.command()
def raw(
    tsids: Annotated[list[str], typer.Argument(help="One or more CWMS tsids.")],
    start: Annotated[
        datetime | None,
        typer.Option(
            help="ISO-8601 start (UTC if no offset).",
            formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"],
        ),
    ] = None,
    end: Annotated[
        datetime | None,
        typer.Option(help="ISO-8601 end.", formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"]),
    ] = None,
    lookback: Annotated[
        str | None,
        typer.Option(
            help=(
                "Window length when --start and/or --end is omitted (e.g. 7d, 48h, 10y). "
                "Default: 7d. Rejected when both --start and --end are given."
            ),
        ),
    ] = None,
    timezone: Annotated[str, typer.Option(help="Server timezone bucketing.")] = "GMT",
    out: Annotated[
        Path | None,
        typer.Option("--out", "-o", help="Output file. Defaults to stdout."),
    ] = None,
    timeout: Annotated[float, typer.Option(help="HTTP timeout (seconds).")] = 60.0,
    endpoint: Annotated[
        str | None, typer.Option(help="Override the Dataquery 2.0 endpoint URL.")
    ] = None,
    quiet: Annotated[bool, typer.Option("-q", "--quiet", help="Suppress warnings.")] = False,
) -> None:
    """Print the raw upstream JSON payload for one or more tsids."""
    lb = _resolve_window_args(start, end, lookback)

    if quiet:
        import warnings

        from .errors import UnknownTsidWarning

        warnings.simplefilter("ignore", UnknownTsidWarning)

    async def _run() -> dict:
        async with AsyncDataQueryClient(
            timeout=timeout,
            timezone=timezone,
            endpoint=endpoint or ENDPOINT,
        ) as client:
            return await client.fetch_raw(tsids, start=start, end=end, lookback=lb)

    try:
        payload = asyncio.run(_run())
    except Exception as exc:
        from .errors import DataQueryError

        if isinstance(exc, DataQueryError):
            typer.secho(f"server error: {exc}", fg="red", err=True)
            raise typer.Exit(code=2) from exc
        typer.secho(f"error: {exc}", fg="red", err=True)
        raise typer.Exit(code=1) from exc

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
