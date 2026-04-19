"""Integration tests against recorded cassettes.

First run these with ``pytest --record-mode=once -k integration`` to record
cassettes against the live endpoint, then commit the cassette files. Every
subsequent run replays from disk with no network activity.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pytest

from nwd_dataquery import AsyncDataQueryClient
from nwd_dataquery._parse import SCHEMA


@pytest.mark.vcr
async def test_integration_fetch_lwsc_elevation():
    async with AsyncDataQueryClient() as client:
        table = await client.fetch(
            "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW",
            start=datetime(2026, 4, 10, tzinfo=timezone.utc),
            end=datetime(2026, 4, 11, tzinfo=timezone.utc),
        )
    assert isinstance(table, pa.Table)
    assert table.schema == SCHEMA
    assert table.num_rows > 0


@pytest.mark.vcr
async def test_integration_describe_returns_metadata():
    async with AsyncDataQueryClient() as client:
        meta = await client.describe(
            "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW",
            start=datetime(2026, 4, 18, tzinfo=timezone.utc),
            end=datetime(2026, 4, 19, tzinfo=timezone.utc),
        )
    assert "LWSC" in meta
    assert "timeseries" in meta["LWSC"]
