"""Tests for MetadataResult."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pytest

from nwd_dataquery._results import MetadataResult
from nwd_dataquery.client import DataQueryPayload


def _result(requested: tuple[str, ...], present_tsids: list[str]) -> MetadataResult:
    payload = cast(
        DataQueryPayload,
        {
            "LWSC": {
                "name": "X",
                "timeseries": {t: {"parameter": "P", "units": "U"} for t in present_tsids},
            }
        },
    )
    return MetadataResult(
        payload=payload,
        requested_tsids=requested,
        resolved_window=(datetime(2026, 4, 1, tzinfo=UTC), datetime(2026, 4, 2, tzinfo=UTC)),
        endpoint="https://example.invalid",
        warnings=(),
    )


def test_construct_with_all_fields():
    r = _result(("T",), present_tsids=["T"])
    assert r.requested_tsids == ("T",)
    assert r.endpoint == "https://example.invalid"
    assert r.warnings == ()


def test_no_table_attribute():
    r = _result(("T",), present_tsids=["T"])
    assert not hasattr(r, "table")


def test_no_is_empty_attribute():
    r = _result(("T",), present_tsids=["T"])
    assert not hasattr(r, "is_empty")


def test_unknown_tsids_lists_missing_in_request_order():
    r = _result(("A", "B", "C"), present_tsids=["B"])
    assert r.unknown_tsids == ("A", "C")


def test_metadata_result_is_frozen():
    r = _result(("T",), present_tsids=["T"])
    with pytest.raises((AttributeError, TypeError)):
        r.endpoint = "y"  # type: ignore[misc]  # ty:ignore[invalid-assignment]
