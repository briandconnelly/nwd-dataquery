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
        table = await client.fetch(
            "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW",
            lookback=timedelta(days=2),
        )
    assert isinstance(table, pa.Table)
    assert table.num_rows > 0
    assert "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW" in table["tsid"].to_pylist()


@pytest.mark.live
async def test_live_describe_lwsc_returns_coordinates():
    async with AsyncDataQueryClient() as client:
        meta = await client.describe(
            "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW",
            lookback=timedelta(days=1),
        )
    # `coordinates` is observed in upstream responses but is not part of
    # LocationEntry's narrow typed contract — see the design choice in PR #42.
    assert meta["LWSC"]["coordinates"]["latitude"] == pytest.approx(47.66, abs=0.5)  # ty:ignore[invalid-key]
