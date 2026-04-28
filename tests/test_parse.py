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


def test_find_first_bad_timestamp_returns_index_of_first_bad():
    from nwd_dataquery._parse import _find_first_bad_timestamp

    assert (
        _find_first_bad_timestamp(
            [
                "2026-04-11T18:00:00",
                "2026-04-11T19:00:00",
                "garbage",
                "2026-04-11T20:00:00",
            ]
        )
        == 2
    )


def test_find_first_bad_timestamp_returns_none_when_all_parse():
    """The defensive None-return path: every entry parses in Python, so the
    Arrow failure (if any) cannot be attributed to a single row. The caller
    must not claim an offending row in this case.
    """
    from nwd_dataquery._parse import _find_first_bad_timestamp

    assert _find_first_bad_timestamp(["2026-04-11T18:00:00", "2026-04-11T19:00:00"]) is None


def test_arrow_failure_with_no_pinpointable_row_drops_offending_claim(monkeypatch):
    """When Arrow fails but the Python fallback can't identify a single bad
    row, the error must surface the underlying parse failure without falsely
    claiming a specific 'offending row'.
    """
    import pyarrow as pa
    import pytest

    from nwd_dataquery import _parse
    from nwd_dataquery.errors import DataQueryParseError

    def _raise_arrow_invalid(*args, **kwargs):
        raise pa.ArrowInvalid("simulated arrow failure")

    monkeypatch.setattr(_parse.pc, "strptime", _raise_arrow_invalid)

    payload = {
        "LWSC": {
            "timeseries": {
                "T": {
                    "parameter": "Elev-Lake",
                    "units": "FT",
                    "values": [["2026-04-11T18:00:00", 21.66, 0]],
                }
            }
        }
    }
    with pytest.raises(DataQueryParseError) as exc_info:
        parse_payload(payload)
    msg = str(exc_info.value)
    assert "simulated arrow failure" in msg
    assert "offending row" not in msg
    assert "tsid=" not in msg


def test_malformed_timestamp_identifies_actual_offending_row():
    """The error message must name the row that actually failed to parse,
    not the first row collected. Asserts the bad timestamp appears AND an
    earlier good timestamp does NOT, so the test fails if the implementation
    falls back to reporting row 0.
    """
    import pytest

    from nwd_dataquery.errors import DataQueryParseError

    payload = {
        "LWSC": {
            "timeseries": {
                "T": {
                    "parameter": "Elev-Lake",
                    "units": "FT",
                    "values": [
                        ["2026-04-11T18:00:00", 21.66, 0],
                        ["2026-04-11T19:00:00", 21.67, 0],
                        ["bad-timestamp-row-2", 21.68, 0],
                    ],
                }
            }
        }
    }
    with pytest.raises(DataQueryParseError) as exc_info:
        parse_payload(payload)
    msg = str(exc_info.value)
    assert "bad-timestamp-row-2" in msg
    assert "2026-04-11T18:00:00" not in msg
    assert "2026-04-11T19:00:00" not in msg
