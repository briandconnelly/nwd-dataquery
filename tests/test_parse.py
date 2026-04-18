import json
from pathlib import Path

import pyarrow as pa
import pytest  # noqa: F401

from nwd_dataquery._parse import SCHEMA, parse_payload

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def test_empty_payload_returns_empty_table_with_schema():
    table = parse_payload({})
    assert table.num_rows == 0
    assert table.schema == SCHEMA


def test_empty_json_file_same_as_empty_dict():
    table = parse_payload(_load("empty.json"))
    assert table.num_rows == 0
    assert table.schema == SCHEMA


def test_single_tsid_two_values():
    table = parse_payload(_load("single.json"))
    assert table.num_rows == 2
    assert table["location"].to_pylist() == ["LWSC", "LWSC"]
    assert table["tsid"].to_pylist() == [
        "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW",
        "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW",
    ]
    assert table["value"].to_pylist() == [21.66, 21.67]
    assert table["quality"].to_pylist() == [0, 0]
    assert table["parameter"].to_pylist() == ["Elev-Lake", "Elev-Lake"]
    assert table["units"].to_pylist() == ["FT", "FT"]


def test_single_tsid_timestamps_parsed_as_utc():
    table = parse_payload(_load("single.json"))
    ts_type = table.schema.field("timestamp").type
    assert pa.types.is_timestamp(ts_type)
    assert ts_type.tz == "UTC"
    # First row should be 2026-04-11T18:00:00Z
    first = table["timestamp"][0].as_py()
    assert first.isoformat() == "2026-04-11T18:00:00+00:00"


def test_multi_tsid_and_null_quality():
    table = parse_payload(_load("multi.json"))
    assert table.num_rows == 2
    tsids = set(table["tsid"].to_pylist())
    assert tsids == {
        "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW",
        "LWSC.Flow-In.Ave.~1Day.1Day.CENWS-COMPUTED-RAW",
    }
    # One of the rows has null quality
    quality = table["quality"].to_pylist()
    assert None in quality


def test_schema_has_expected_column_order():
    assert SCHEMA.names == [
        "timestamp",
        "value",
        "quality",
        "tsid",
        "location",
        "parameter",
        "units",
    ]
