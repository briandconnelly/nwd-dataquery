"""Tests for QueryResult."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pyarrow as pa
import pytest

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


def _result_with(requested: tuple[str, ...], present_tsids: list[str]) -> QueryResult:
    """Build a QueryResult whose payload's `timeseries` keys cover `present_tsids`."""
    payload = cast(
        DataQueryPayload,
        {
            "LWSC": {
                "name": "X",
                "timeseries": {
                    t: {"parameter": "P", "units": "U", "values": []} for t in present_tsids
                },
            }
        },
    )
    return QueryResult(
        table=_empty_table(),
        payload=payload,
        requested_tsids=requested,
        resolved_window=(datetime(2026, 4, 1, tzinfo=UTC), datetime(2026, 4, 2, tzinfo=UTC)),
        endpoint="x",
        warnings=(),
    )


def test_unknown_tsids_empty_when_all_present():
    r = _result_with(("A", "B"), present_tsids=["A", "B"])
    assert r.unknown_tsids == ()


def test_unknown_tsids_lists_missing_in_request_order():
    r = _result_with(("A", "B", "C"), present_tsids=["B"])
    assert r.unknown_tsids == ("A", "C")


def test_unknown_tsids_dedupes_by_first_occurrence():
    r = _result_with(("A", "B", "A", "C", "B"), present_tsids=[])
    assert r.unknown_tsids == ("A", "B", "C")


def test_unknown_tsids_present_in_payload_never_unknown_even_if_duplicated():
    # 'A' appears twice in the request and IS in the payload; it must not appear in unknown_tsids.
    r = _result_with(("A", "B", "A"), present_tsids=["A"])
    assert r.unknown_tsids == ("B",)


def test_unknown_tsids_searches_all_locations():
    """A tsid is 'present' if it appears under ANY location's timeseries dict."""
    payload = cast(
        DataQueryPayload,
        {
            "LWSC": {"name": "X", "timeseries": {"A": {}}},
            "MUDM": {"name": "Y", "timeseries": {"B": {}}},
        },
    )
    r = QueryResult(
        table=_empty_table(),
        payload=payload,
        requested_tsids=("A", "B", "C"),
        resolved_window=(datetime(2026, 4, 1, tzinfo=UTC), datetime(2026, 4, 2, tzinfo=UTC)),
        endpoint="x",
        warnings=(),
    )
    assert r.unknown_tsids == ("C",)


def test_unknown_tsids_skips_non_dict_location_body():
    """Defensive: a non-dict location body in the payload is skipped, not crashed on."""
    payload = cast(DataQueryPayload, {"BAD": "not a dict", "LWSC": {"timeseries": {"A": {}}}})
    r = QueryResult(
        table=_empty_table(),
        payload=payload,
        requested_tsids=("A", "B"),
        resolved_window=(datetime(2026, 4, 1, tzinfo=UTC), datetime(2026, 4, 2, tzinfo=UTC)),
        endpoint="x",
        warnings=(),
    )
    assert r.unknown_tsids == ("B",)


def test_unknown_tsids_skips_non_dict_timeseries_value():
    """Defensive: a truthy non-dict `timeseries` value (e.g. a list) is skipped,
    not crashed on with AttributeError when .keys() is called.
    """
    payload = cast(
        DataQueryPayload,
        {"LWSC": {"timeseries": ["not", "a", "dict"]}, "MUDM": {"timeseries": {"A": {}}}},
    )
    r = QueryResult(
        table=_empty_table(),
        payload=payload,
        requested_tsids=("A", "B"),
        resolved_window=(datetime(2026, 4, 1, tzinfo=UTC), datetime(2026, 4, 2, tzinfo=UTC)),
        endpoint="x",
        warnings=(),
    )
    assert r.unknown_tsids == ("B",)


def test_query_result_is_frozen():
    """frozen=True must reject attribute assignment after construction."""
    r = QueryResult(
        table=_empty_table(),
        payload={},
        requested_tsids=("T",),
        resolved_window=(datetime(2026, 4, 1, tzinfo=UTC), datetime(2026, 4, 2, tzinfo=UTC)),
        endpoint="x",
        warnings=(),
    )
    with pytest.raises((AttributeError, TypeError)):
        r.endpoint = "y"  # type: ignore[misc]  # ty:ignore[invalid-assignment]
