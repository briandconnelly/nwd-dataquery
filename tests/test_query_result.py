"""Tests for QueryResult."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pyarrow as pa

from nwd_dataquery._results import QueryResult
from nwd_dataquery.client import DataQueryPayload


def _empty_table() -> pa.Table:
    from nwd_dataquery._parse import SCHEMA

    return SCHEMA.empty_table()


def test_construct_with_all_fields():
    table = _empty_table()
    payload = cast(DataQueryPayload, {"LWSC": {"name": "X", "timeseries": {}}})
    start = datetime(2026, 4, 1, tzinfo=UTC)
    end = datetime(2026, 4, 8, tzinfo=UTC)
    result = QueryResult(
        table=table,
        payload=payload,
        requested_tsids=("T",),
        resolved_window=(start, end),
        endpoint="https://example.invalid",
        warnings=(),
    )
    assert result.table is table
    assert result.payload == payload
    assert result.requested_tsids == ("T",)
    assert result.resolved_window == (start, end)
    assert result.endpoint == "https://example.invalid"
    assert result.warnings == ()


def test_is_empty_true_for_zero_row_table():
    result = QueryResult(
        table=_empty_table(),
        payload={},
        requested_tsids=("T",),
        resolved_window=(datetime(2026, 4, 1, tzinfo=UTC), datetime(2026, 4, 2, tzinfo=UTC)),
        endpoint="x",
        warnings=(),
    )
    assert result.is_empty is True


def test_is_empty_false_for_populated_table():
    import pyarrow.compute as pc

    table = pa.table(
        {
            "timestamp": pc.assume_timezone(  # ty:ignore[unresolved-attribute]
                pa.array(["2026-04-11T18:00:00"], type=pa.string()).cast(pa.timestamp("us")),
                timezone="UTC",
            ),
            "value": pa.array([21.66], type=pa.float64()),
            "quality": pa.array([0], type=pa.int64()),
            "tsid": pa.array(["T"], type=pa.string()),
            "location": pa.array(["LWSC"], type=pa.string()),
            "parameter": pa.array(["Elev-Lake"], type=pa.string()),
            "units": pa.array(["FT"], type=pa.string()),
        }
    )
    result = QueryResult(
        table=table,
        payload={},
        requested_tsids=("T",),
        resolved_window=(datetime(2026, 4, 1, tzinfo=UTC), datetime(2026, 4, 2, tzinfo=UTC)),
        endpoint="x",
        warnings=(),
    )
    assert result.is_empty is False
