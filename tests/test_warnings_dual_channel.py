"""Verify UnknownTsidWarning fires via warnings.warn AND attaches to result.warnings."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from nwd_dataquery import AsyncDataQueryClient, UnknownTsidWarning


async def test_fetch_empty_payload_warns_and_attaches():
    """Empty payload triggers warnings.warn AND populates result.warnings.
    The same instance is in both places.
    """
    with (
        patch(
            "nwd_dataquery.client.AsyncDataQueryClient._request_payload",
            new=AsyncMock(return_value={}),
        ),
        pytest.warns(UnknownTsidWarning) as caught,
    ):
        async with AsyncDataQueryClient() as client:
            result = await client.fetch(
                "T",
                start=datetime(2026, 4, 1, tzinfo=UTC),
                end=datetime(2026, 4, 2, tzinfo=UTC),
            )

    assert len(result.warnings) == 1
    assert isinstance(result.warnings[0], UnknownTsidWarning)
    assert len(caught) == 1
    # Same instance in both channels (no copy).
    assert result.warnings[0] is caught[0].message


async def test_fetch_non_empty_payload_no_warning():
    """A non-empty payload emits no warning and result.warnings is empty."""
    payload = {
        "LWSC": {
            "name": "X",
            "timeseries": {
                "T": {
                    "parameter": "P",
                    "units": "U",
                    "values": [["2026-04-11T18:00:00", 21.66, 0]],
                }
            },
        }
    }
    with (
        patch(
            "nwd_dataquery.client.AsyncDataQueryClient._request_payload",
            new=AsyncMock(return_value=payload),
        ),
    ):
        async with AsyncDataQueryClient() as client:
            result = await client.fetch(
                "T",
                start=datetime(2026, 4, 1, tzinfo=UTC),
                end=datetime(2026, 4, 2, tzinfo=UTC),
            )
    assert result.warnings == ()


async def test_describe_empty_payload_warns_and_attaches():
    """Same dual-channel contract holds for describe()."""
    with (
        patch(
            "nwd_dataquery.client.AsyncDataQueryClient._request_payload",
            new=AsyncMock(return_value={}),
        ),
        pytest.warns(UnknownTsidWarning) as caught,
    ):
        async with AsyncDataQueryClient() as client:
            result = await client.describe(
                "T",
                start=datetime(2026, 4, 1, tzinfo=UTC),
                end=datetime(2026, 4, 2, tzinfo=UTC),
            )
    assert len(result.warnings) == 1
    assert isinstance(result.warnings[0], UnknownTsidWarning)
    assert len(caught) == 1
    assert result.warnings[0] is caught[0].message
