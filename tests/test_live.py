"""Live smoke tests against the real USACE endpoint.

Excluded from the default run via ``addopts = "-m 'not live'"`` in pyproject.toml.
Run explicitly with ``pytest -m live``. The nightly workflow runs these.
"""

from __future__ import annotations

from datetime import timedelta

import pyarrow as pa
import pytest

from nwd_dataquery import AsyncDataQueryClient


@pytest.mark.live
async def test_live_fetch_lwsc_elevation_nonempty():
    async with AsyncDataQueryClient() as client:
        result = await client.fetch(
            "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW",
            lookback=timedelta(days=14),
        )
    assert isinstance(result.table, pa.Table)
    assert result.table.num_rows > 0
    assert "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW" in result.table["tsid"].to_pylist()


@pytest.mark.live
async def test_live_describe_lwsc_returns_coordinates():
    async with AsyncDataQueryClient() as client:
        result = await client.describe(
            "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW",
            lookback=timedelta(days=1),
        )
    assert result.payload["LWSC"]["coordinates"]["latitude"] == pytest.approx(47.66, abs=0.5)  # ty:ignore[invalid-key]
