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

### Alternate endpoint

A public mirror reportedly exists at `public.crohms.org` (Columbia River Operational Hydromet Management System, a multi-agency partnership) using the same URL paths. Point to it via `--endpoint` / `AsyncDataQueryClient(endpoint=...)` if the primary host is unreachable. Unverified.

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

## TSID anatomy

The 6-part CWMS identifier: `LOC.PARAMETER.TYPE.INTERVAL.DURATION.VERSION`.

- **`TYPE`** — `Inst` (instantaneous) or `Ave` (interval-averaged).
- **`INTERVAL`** — `0`, `15Minutes`, `1Hour`, `~1Day`, `1Day`. A leading `~` marks irregular cadence.
- **`DURATION`** — `0` for point observations, or an interval for aggregations.
- **`VERSION`** — `SOURCE-QUALITY`. Sources observed: `NWSRADIO`, `IRIDIUM`, `GOES`, `USGS`, `USBR`, `CENWS-COMPUTED`, `CENWP-COMPUTED`, `CENWW-COMPUTED`, `CBT`, `RFC-NOS`, `NOAA`, `MIXED-COMPUTED`. Quality is `RAW` or `REV`. The special version `Best` is an alias for whichever source/quality is canonical for that series — prefer it for downstream consumption and keep the raw-version tsids for provenance.

## Known tsids

| tsid | location | description | period of record |
| --- | --- | --- | --- |
| `LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW` | LWSC | Pool elevation — hourly average, NWS radio DCP, raw | 2001–present |
| `LWSC.Elev-Lake.Inst.1Hour.0.NWSRADIO-REV` | LWSC | Pool elevation — hourly instantaneous, NWS radio DCP, reviewed | — |
| `LWSC.Elev-Lake.Ave.1Hour.1Hour.IRIDIUM-REV` | LWSC | Pool elevation — hourly average, Iridium satellite, reviewed | — |
| `LWSC.Flow-In.Ave.~1Day.1Day.CENWS-COMPUTED-RAW` | LWSC | Daily average inflow — computed by Seattle District | — |

Extend this list by watching XHR requests in the Dataquery 2.0 UI.

## Gotchas

- **Empty payload is ambiguous.** The server returns `{}` for "unknown tsid," "no data in the requested window," *and* for seasonal tsids that aren't currently deployed (e.g. temporary summer gauges). The client always emits `UnknownTsidWarning`; you can't distinguish the cases without out-of-band knowledge.
- **Everything comes back as `text/plain`.** Both successful responses and server-side errors use `Content-Type: text/plain; charset=UTF-8` and always respond HTTP 200. The client parses the body, checks for a top-level `"error"` key, and raises `DataQueryError` if present.
- **Wildcards don't work.** `["LWSC.*"]` returns `{}`. Query the UI to discover tsids.
- **Seasonal stations move in and out of the catalog.** Some station codes only appear in the Dataquery UI while the underlying gauge is physically deployed. Treat your tsid list as a moving target, not a fixed registry.

## Development

```bash
uv sync --all-extras --group dev
prek install
uv run pytest              # unit + integration (cassettes)
uv run pytest -m live      # live smoke (requires network)
```

## License

MIT
