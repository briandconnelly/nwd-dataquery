# nwd-dataquery

[![CI](https://github.com/briandconnelly/nwd-dataquery/actions/workflows/ci.yml/badge.svg)](https://github.com/briandconnelly/nwd-dataquery/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/nwd-dataquery.svg)](https://pypi.org/project/nwd-dataquery/)

Async Python client for the [USACE Northwestern Division](https://www.nwd.usace.army.mil/) Dataquery 2.0 hydrologic timeseries endpoint.

The underlying endpoint (`https://www.nwd-wc.usace.army.mil/dd/common/web_service/webexec/getjson`) is undocumented. This package was reverse-engineered from the Dataquery 2.0 UI and targets NWD-only data (e.g. Lake Washington Ship Canal, Howard Hanson, Mud Mountain). For districts that are migrated to the modern CWMS Data API, use `cwms-python` instead.

## Install

```bash
pip install nwd-dataquery            # core: pyarrow output + CLI
pip install nwd-dataquery[polars]    # adds polars frame support
pip install nwd-dataquery[pandas]    # adds pandas frame support
```

Python ≥3.12.

### A note on SSL

Importing `nwd_dataquery` calls `truststore.inject_into_ssl()` to use the OS trust store. USACE `.mil` domains often can't be validated with certifi's bundle on Python installs from `uv`/official installers. If you have strong reasons not to touch the global SSL stack, don't import this package.

## Quick start (Python)

```python
import asyncio
from datetime import datetime, timezone
from nwd_dataquery import AsyncDataQueryClient

async def main():
    async with AsyncDataQueryClient() as client:
        # Default: last 7 days, pyarrow.Table out
        table = await client.fetch("LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW")
        print(table.to_pandas().head())

        # Decade backfill in one request
        backfill = await client.fetch(
            "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW",
            start=datetime(2016, 1, 1, tzinfo=timezone.utc),
            end=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

        # Metadata only
        meta = await client.describe("LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW")

asyncio.run(main())
```

Switch frame types:

```python
tbl = await client.fetch(tsid)                  # pyarrow.Table (default)
df  = await client.fetch(tsid, backend="polars")  # requires nwd-dataquery[polars]
df  = await client.fetch(tsid, backend="pandas")  # requires nwd-dataquery[pandas]
```

## Quick start (CLI)

```bash
nwd-dq fetch LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW --lookback 30d
nwd-dq fetch LWSC.Flow-In.Ave.~1Day.1Day.CENWS-COMPUTED-RAW \
    --start 2016-01-01 --format parquet --out flows.pq
nwd-dq describe LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW | jq
```

Exit codes: `0` success, `1` transport error, `2` server/data-query error, `3` empty result with `--strict`.

## Output schema

`fetch()` returns a long-format frame with columns:

| column | type | meaning |
| --- | --- | --- |
| `timestamp` | `timestamp[us, tz=UTC]` | observation time |
| `value` | `float64` | measurement |
| `quality` | `int64` | server quality flag (may be null) |
| `tsid` | `string` | CWMS timeseries id |
| `location` | `string` | location code (`LWSC`, …) |
| `parameter` | `string` | parameter name (`Elev-Lake`, `Flow-In`, …) |
| `units` | `string` | server-reported units (`FT`, `CFS`, …) |

## Known tsids

| tsid | location | description | period of record |
| --- | --- | --- | --- |
| `LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW` | LWSC | Pool elevation — hourly average, NWS radio DCP, raw | 2001–present |
| `LWSC.Elev-Lake.Inst.1Hour.0.NWSRADIO-REV` | LWSC | Pool elevation — hourly instantaneous, NWS radio DCP, reviewed | — |
| `LWSC.Elev-Lake.Ave.1Hour.1Hour.IRIDIUM-REV` | LWSC | Pool elevation — hourly average, Iridium satellite, reviewed | — |
| `LWSC.Flow-In.Ave.~1Day.1Day.CENWS-COMPUTED-RAW` | LWSC | Daily average inflow — computed by Seattle District | — |

Extend this list by watching XHR requests in the Dataquery 2.0 UI.

## Gotchas

- **Empty payload is ambiguous.** The server returns `{}` for both "tsid doesn't exist" and "no data in window." The client always emits `UnknownTsidWarning` on empty payloads.
- **Errors arrive as HTTP 200.** Bad input gets `Content-Type: text/plain` with a JSON `{"error": "..."}` body. The client parses this and raises `DataQueryError`.
- **Wildcards don't work.** `["LWSC.*"]` returns `{}`. Query the UI to discover tsids.

## Development

```bash
uv sync --all-extras --group dev
prek install
uv run pytest              # unit + integration (cassettes)
uv run pytest -m live      # live smoke (requires network)
```

## License

MIT
