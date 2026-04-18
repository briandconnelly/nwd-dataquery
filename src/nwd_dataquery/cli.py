"""`nwd-dq` command-line interface."""

from __future__ import annotations

import asyncio
import io
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pa_pq
import typer

from .client import AsyncDataQueryClient

app = typer.Typer(
    name="nwd-dq",
    help="USACE NWD Dataquery 2.0 CLI.",
    add_completion=False,
    no_args_is_help=True,
)

_DURATION_RE = re.compile(r"^\s*(\d+)\s*([yMwdhm])\s*$")
_DURATION_UNITS = {
    "y": lambda n: timedelta(days=365 * n),
    "M": lambda n: timedelta(days=30 * n),
    "w": lambda n: timedelta(weeks=n),
    "d": lambda n: timedelta(days=n),
    "h": lambda n: timedelta(hours=n),
    "m": lambda n: timedelta(minutes=n),
}


def parse_duration(text: str) -> timedelta:
    """Parse simple durations like ``7d``, ``48h``, ``10y``."""
    m = _DURATION_RE.match(text)
    if not m:
        raise ValueError(f"unparseable duration: {text!r}")
    n = int(m.group(1))
    unit = m.group(2)
    return _DURATION_UNITS[unit](n)


@app.command()
def fetch(
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
        str, typer.Option(help="Relative lookback (e.g. 7d, 10y).")
    ] = "7d",
    timezone: Annotated[str, typer.Option(help="Server timezone bucketing.")] = "GMT",
    fmt: Annotated[
        str, typer.Option("--format", "-f", help="csv | json | parquet")
    ] = "csv",
    out: Annotated[
        Path | None,
        typer.Option("--out", "-o", help="Output file. Required for parquet."),
    ] = None,
    timeout: Annotated[float, typer.Option(help="HTTP timeout (seconds).")] = 60.0,
    endpoint: Annotated[
        str | None, typer.Option(help="Override the Dataquery 2.0 endpoint URL.")
    ] = None,
    quiet: Annotated[
        bool, typer.Option("-q", "--quiet", help="Suppress warnings.")
    ] = False,
    verbose: Annotated[
        bool, typer.Option("-v", "--verbose", help="Debug logging.")
    ] = False,
    strict: Annotated[
        bool, typer.Option("--strict", help="Exit 3 on empty result.")
    ] = False,
) -> None:
    """Fetch observations for one or more tsids."""
    if fmt == "parquet" and out is None:
        typer.secho("error: --format parquet requires --out PATH", fg="red", err=True)
        raise typer.Exit(code=2)

    try:
        lb = parse_duration(lookback)
    except ValueError as exc:
        typer.secho(f"error: {exc}", fg="red", err=True)
        raise typer.Exit(code=2)

    if quiet:
        import warnings

        from .errors import UnknownTsidWarning

        warnings.simplefilter("ignore", UnknownTsidWarning)

    if verbose:
        import logging

        logging.basicConfig(level=logging.DEBUG)

    async def _run() -> pa.Table:
        kwargs: dict[str, object] = {"timeout": timeout, "timezone": timezone}
        if endpoint:
            kwargs["endpoint"] = endpoint
        async with AsyncDataQueryClient(**kwargs) as client:
            return await client.fetch(tsids, start=start, end=end, lookback=lb)

    try:
        table = asyncio.run(_run())
    except Exception as exc:  # transport/HTTP errors bubble up
        from .errors import DataQueryError

        if isinstance(exc, DataQueryError):
            typer.secho(f"server error: {exc}", fg="red", err=True)
            raise typer.Exit(code=2)
        typer.secho(f"error: {exc}", fg="red", err=True)
        raise typer.Exit(code=1)

    if strict and table.num_rows == 0:
        raise typer.Exit(code=3)

    _write(table, fmt, out)


@app.command()
def describe(
    tsids: Annotated[list[str], typer.Argument(help="One or more CWMS tsids.")],
    start: Annotated[datetime | None, typer.Option()] = None,
    end: Annotated[datetime | None, typer.Option()] = None,
    lookback: Annotated[str, typer.Option()] = "7d",
    timezone: Annotated[str, typer.Option()] = "GMT",
    timeout: Annotated[float, typer.Option()] = 60.0,
    endpoint: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Emit location + tsid metadata as JSON (no values)."""
    try:
        lb = parse_duration(lookback)
    except ValueError as exc:
        typer.secho(f"error: {exc}", fg="red", err=True)
        raise typer.Exit(code=2)

    async def _run() -> dict:
        kwargs: dict[str, object] = {"timeout": timeout, "timezone": timezone}
        if endpoint:
            kwargs["endpoint"] = endpoint
        async with AsyncDataQueryClient(**kwargs) as client:
            return await client.describe(tsids, start=start, end=end, lookback=lb)

    meta = asyncio.run(_run())
    typer.echo(json.dumps(meta, indent=2, default=str))


def _write(table: pa.Table, fmt: str, out: Path | None) -> None:
    if fmt == "csv":
        buf = io.BytesIO()
        pa_csv.write_csv(table, buf)
        sys.stdout.buffer.write(buf.getvalue())
    elif fmt == "json":
        # NDJSON: one object per row
        for batch in table.to_batches():
            rows = batch.to_pylist()
            for row in rows:
                # Normalize datetime objects for JSON
                norm = {
                    k: (v.isoformat() if hasattr(v, "isoformat") else v)
                    for k, v in row.items()
                }
                sys.stdout.write(json.dumps(norm) + "\n")
    elif fmt == "parquet":
        assert out is not None
        pa_pq.write_table(table, out)
    else:
        typer.secho(f"error: unknown --format {fmt!r}", fg="red", err=True)
        raise typer.Exit(code=2)
