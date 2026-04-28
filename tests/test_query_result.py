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
