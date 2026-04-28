import json
from pathlib import Path

import pyarrow as pa

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


# --- coverage gap tests ---


def test_non_dict_location_body_is_skipped():
    """_parse.py:42 — loc_body not a dict is skipped."""
    payload = {"BAD_LOCATION": "not a dict", "LWSC": {"timeseries": {}}}
    table = parse_payload(payload)
    assert table.num_rows == 0


def test_non_dict_ts_body_is_skipped():
    """_parse.py:45 — ts_body not a dict is skipped."""
    payload = {
        "LWSC": {
            "timeseries": {
                "BAD_TSID": "not a dict",
            }
        }
    }
    table = parse_payload(payload)
    assert table.num_rows == 0


def test_empty_value_row_is_skipped():
    """A row with no elements at all must not crash with IndexError; skip it."""
    payload = {
        "LWSC": {
            "timeseries": {
                "T": {
                    "parameter": "Elev-Lake",
                    "units": "FT",
                    "values": [
                        ["2026-04-11T18:00:00", 21.66, 0],
                        [],  # malformed row
                        ["2026-04-11T19:00:00", 21.67, 0],
                    ],
                }
            }
        }
    }
    table = parse_payload(payload)
    assert table.num_rows == 2
    assert table["value"].to_pylist() == [21.66, 21.67]


def test_malformed_timestamp_raises_parse_error():
    """An unparseable timestamp must surface as DataQueryParseError with context,
    not a bare ArrowInvalid.
    """
    import pytest

    from nwd_dataquery.errors import DataQueryParseError

    payload = {
        "LWSC": {
            "timeseries": {
                "T": {
                    "parameter": "Elev-Lake",
                    "units": "FT",
                    "values": [["not-a-timestamp", 21.66, 0]],
                }
            }
        }
    }
    with pytest.raises(DataQueryParseError) as exc_info:
        parse_payload(payload)
    msg = str(exc_info.value)
    assert "not-a-timestamp" in msg
    assert "T" in msg  # tsid included for context
