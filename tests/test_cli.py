import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pyarrow as pa
import pytest
from typer.testing import CliRunner

from nwd_dataquery.cli import app, parse_duration

runner = CliRunner()


@pytest.fixture
def sample_table() -> pa.Table:
    import pyarrow.compute as pc

    return pa.table(
        {
            "timestamp": pc.assume_timezone(
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


def test_version_flag_prints_version():
    from nwd_dataquery import __version__

    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0, result.stderr
    assert result.stdout.strip() == f"nwd-dq {__version__}"


def test_parse_duration_days():
    from datetime import timedelta

    assert parse_duration("7d") == timedelta(days=7)
    assert parse_duration("48h") == timedelta(hours=48)
    assert parse_duration("30m") == timedelta(minutes=30)
    assert parse_duration("10y") == timedelta(days=3650)


def test_parse_duration_rejects_garbage():
    with pytest.raises(ValueError):
        parse_duration("lol")


def test_fetch_csv_to_stdout(sample_table):
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
            new=AsyncMock(return_value=sample_table),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["fetch", "T", "--lookback", "1d"])
    assert result.exit_code == 0, result.stderr
    assert "timestamp" in result.stdout  # CSV header
    assert "21.66" in result.stdout


@pytest.mark.parametrize("fmt", ["json", "ndjson"])
def test_fetch_ndjson_aliases_to_stdout(sample_table, fmt):
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
            new=AsyncMock(return_value=sample_table),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["fetch", "T", "--format", fmt])
    assert result.exit_code == 0
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["value"] == 21.66
    assert record["tsid"] == "T"


def test_fetch_invalid_format_exits_before_request():
    """`--format jsn` must exit code 2 without constructing the client."""
    with patch("nwd_dataquery.cli.AsyncDataQueryClient") as client_cls:
        result = runner.invoke(app, ["fetch", "T", "--format", "jsn"])
    assert result.exit_code == 2
    client_cls.assert_not_called()


def test_fetch_parquet_requires_out(sample_table):
    with patch(
        "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
        new=AsyncMock(return_value=sample_table),
    ):
        result = runner.invoke(app, ["fetch", "T", "--format", "parquet"])
    assert result.exit_code != 0
    assert "--out" in result.stderr


def test_fetch_parquet_to_file(sample_table, tmp_path: Path):
    out = tmp_path / "out.pq"
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
            new=AsyncMock(return_value=sample_table),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["fetch", "T", "--format", "parquet", "--out", str(out)])
    assert result.exit_code == 0
    assert out.exists()
    import pyarrow.parquet as pq

    round_trip = pq.read_table(out)
    assert round_trip.num_rows == 1


def test_fetch_csv_to_file(sample_table, tmp_path: Path):
    out = tmp_path / "out.csv"
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
            new=AsyncMock(return_value=sample_table),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["fetch", "T", "--out", str(out)])
    assert result.exit_code == 0, result.stderr
    assert out.exists()
    text = out.read_text()
    assert "timestamp" in text  # CSV header
    assert "21.66" in text
    assert "21.66" not in result.stdout  # did not also print to stdout


def test_fetch_json_to_file(sample_table, tmp_path: Path):
    out = tmp_path / "out.ndjson"
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
            new=AsyncMock(return_value=sample_table),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["fetch", "T", "--format", "json", "--out", str(out)])
    assert result.exit_code == 0, result.stderr
    assert out.exists()
    lines = [line for line in out.read_text().splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["value"] == 21.66
    assert "21.66" not in result.stdout  # did not also print to stdout


def test_fetch_strict_exits_3_on_empty():
    empty = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("value", pa.float64()),
            pa.field("quality", pa.int64()),
            pa.field("tsid", pa.string()),
            pa.field("location", pa.string()),
            pa.field("parameter", pa.string()),
            pa.field("units", pa.string()),
        ]
    ).empty_table()
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
            new=AsyncMock(return_value=empty),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["fetch", "T", "--strict"])
    assert result.exit_code == 3


def test_raw_emits_pretty_json_to_stdout():
    payload = {"LWSC": {"name": "Lake Washington", "timeseries": {"T": {"parameter": "P"}}}}
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch_raw",
            new=AsyncMock(return_value=payload),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["raw", "T"])
    assert result.exit_code == 0, result.stderr
    assert json.loads(result.stdout) == payload
    # pretty-printed: contains newlines and indentation
    assert "\n  " in result.stdout


def test_raw_writes_to_out_file(tmp_path: Path):
    payload = {"LWSC": {"name": "Lake Washington"}}
    out = tmp_path / "raw.json"
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch_raw",
            new=AsyncMock(return_value=payload),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["raw", "T", "--out", str(out)])
    assert result.exit_code == 0, result.stderr
    assert out.exists()
    assert json.loads(out.read_text()) == payload
    assert "Lake Washington" not in result.stdout


def test_raw_data_query_error_exits_2():
    from nwd_dataquery.errors import DataQueryError

    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch_raw",
            new=AsyncMock(side_effect=DataQueryError("Malformed query")),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["raw", "T"])
    assert result.exit_code == 2
    assert "server error" in result.stderr.lower()


def test_raw_runtime_error_exits_1():
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch_raw",
            new=AsyncMock(side_effect=RuntimeError("network down")),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["raw", "T"])
    assert result.exit_code == 1
    assert "error" in result.stderr.lower()


def test_raw_bad_lookback_exits_2():
    result = runner.invoke(app, ["raw", "T", "--lookback", "garbage"])
    assert result.exit_code == 2
    assert "error" in result.stderr.lower()


def test_raw_quiet_suppresses_warnings():
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch_raw",
            new=AsyncMock(return_value={}),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["raw", "T", "--quiet"])
    assert result.exit_code == 0


def test_raw_endpoint_override():
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch_raw",
            new=AsyncMock(return_value={}),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["raw", "T", "--endpoint", "http://example.invalid"])
    assert result.exit_code == 0


def test_describe_emits_json():
    meta = {"LWSC": {"name": "X", "timeseries": {"T": {"parameter": "P"}}}}
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.describe",
            new=AsyncMock(return_value=meta),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["describe", "T"])
    assert result.exit_code == 0
    assert json.loads(result.stdout) == meta


# --- coverage gap tests ---


def test_fetch_bad_lookback_exits_2():
    """cli.py:85-87 — parse_duration ValueError → exit 2."""
    result = runner.invoke(app, ["fetch", "T", "--lookback", "garbage"])
    assert result.exit_code == 2
    assert "error" in result.stderr.lower()


def test_fetch_quiet_suppresses_warnings(sample_table):
    """cli.py:89-94 — --quiet branch."""
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
            new=AsyncMock(return_value=sample_table),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["fetch", "T", "--quiet"])
    assert result.exit_code == 0


def test_fetch_verbose_enables_logging(sample_table):
    """cli.py:96-99 — --verbose branch."""
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
            new=AsyncMock(return_value=sample_table),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["fetch", "T", "--verbose"])
    assert result.exit_code == 0


def test_fetch_endpoint_override(sample_table):
    """cli.py:103-104 — --endpoint override sets kwargs."""
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
            new=AsyncMock(return_value=sample_table),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["fetch", "T", "--endpoint", "http://example.invalid"])
    assert result.exit_code == 0


def test_fetch_data_query_error_exits_2():
    """cli.py:110-115 — DataQueryError → exit 2."""
    from nwd_dataquery.errors import DataQueryError

    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
            new=AsyncMock(side_effect=DataQueryError("boom")),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["fetch", "T"])
    assert result.exit_code == 2
    assert "server error" in result.stderr.lower()


def test_fetch_runtime_error_exits_1():
    """cli.py:116-117 — generic RuntimeError → exit 1."""
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
            new=AsyncMock(side_effect=RuntimeError("network down")),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["fetch", "T"])
    assert result.exit_code == 1
    assert "error" in result.stderr.lower()


def test_describe_bad_lookback_exits_2():
    """cli.py:138-140 — parse_duration ValueError in describe → exit 2."""
    result = runner.invoke(app, ["describe", "T", "--lookback", "nope"])
    assert result.exit_code == 2
    assert "error" in result.stderr.lower()


def test_describe_endpoint_override():
    """cli.py:144-145 — --endpoint override in describe."""
    meta = {"LWSC": {"timeseries": {}}}
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.describe",
            new=AsyncMock(return_value=meta),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["describe", "T", "--endpoint", "http://example.invalid"])
    assert result.exit_code == 0


def test_describe_data_query_error_exits_2():
    """cli.py:151-156 — DataQueryError in describe → exit 2."""
    from nwd_dataquery.errors import DataQueryError

    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.describe",
            new=AsyncMock(side_effect=DataQueryError("bad")),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["describe", "T"])
    assert result.exit_code == 2
    assert "server error" in result.stderr.lower()


def test_describe_runtime_error_exits_1():
    """cli.py:157-158 — generic RuntimeError in describe → exit 1."""
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.describe",
            new=AsyncMock(side_effect=RuntimeError("oops")),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["describe", "T"])
    assert result.exit_code == 1
    assert "error" in result.stderr.lower()


def test_write_parquet_none_out_raises(sample_table):
    """cli.py:179-180 — _write parquet with out=None raises ValueError."""
    from nwd_dataquery.cli import _write

    with pytest.raises(ValueError, match="file path"):
        _write(sample_table, "parquet", None)


def test_write_unknown_format_raises(sample_table):
    """cli.py:182-184 — _write unknown format → typer.Exit(code=2)."""
    import typer

    from nwd_dataquery.cli import _write

    with pytest.raises(typer.Exit) as exc_info:
        _write(sample_table, "xml", None)
    assert exc_info.value.exit_code == 2


def test_write_csv_no_header_to_buffer(sample_table):
    """_write with include_header=False omits the CSV header row."""
    import io

    from nwd_dataquery.cli import OutputFormat, _write

    buf = io.BytesIO()
    with patch("sys.stdout") as mock_stdout:
        mock_stdout.buffer = buf
        _write(sample_table, OutputFormat.csv, None, include_header=False)

    text = buf.getvalue().decode()
    assert "timestamp" not in text  # no header row
    assert "21.66" in text


def _table_from_rows(rows: list[tuple[str, str, float]]) -> pa.Table:
    import pyarrow.compute as pc

    ts = pc.assume_timezone(
        pa.array([r[0] for r in rows], type=pa.string()).cast(pa.timestamp("us")),
        timezone="UTC",
    )
    return pa.table(
        {
            "timestamp": ts,
            "value": pa.array([r[2] for r in rows], type=pa.float64()),
            "quality": pa.array([0] * len(rows), type=pa.int64()),
            "tsid": pa.array([r[1] for r in rows], type=pa.string()),
            "location": pa.array(["LWSC"] * len(rows), type=pa.string()),
            "parameter": pa.array(["Elev-Lake"] * len(rows), type=pa.string()),
            "units": pa.array(["FT"] * len(rows), type=pa.string()),
        }
    )


def test_latest_per_tsid_picks_most_recent_row():
    from nwd_dataquery.cli import _latest_per_tsid

    tbl = _table_from_rows(
        [
            ("2026-04-10T12:00:00", "A", 1.0),
            ("2026-04-11T18:00:00", "A", 2.0),
            ("2026-04-09T00:00:00", "B", 10.0),
            ("2026-04-11T06:00:00", "B", 20.0),
            ("2026-04-10T00:00:00", "B", 15.0),
        ]
    )
    latest = _latest_per_tsid(tbl)
    rows = {r["tsid"]: r["value"] for r in latest.to_pylist()}
    assert rows == {"A": 2.0, "B": 20.0}
    assert latest.num_rows == 2


def test_latest_per_tsid_on_empty_table():
    from nwd_dataquery.cli import _latest_per_tsid

    empty = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("value", pa.float64()),
            pa.field("quality", pa.int64()),
            pa.field("tsid", pa.string()),
            pa.field("location", pa.string()),
            pa.field("parameter", pa.string()),
            pa.field("units", pa.string()),
        ]
    ).empty_table()
    assert _latest_per_tsid(empty).num_rows == 0


def test_fetch_latest_flag_reduces_output():
    tbl = _table_from_rows(
        [
            ("2026-04-10T12:00:00", "A", 1.0),
            ("2026-04-11T18:00:00", "A", 2.0),
            ("2026-04-11T06:00:00", "B", 20.0),
        ]
    )
    with (
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.fetch",
            new=AsyncMock(return_value=tbl),
        ),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(app, ["fetch", "A", "B", "--latest", "--format", "json"])
    assert result.exit_code == 0, result.stderr
    lines = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) == 2
    by_tsid = {r["tsid"]: r["value"] for r in lines}
    assert by_tsid == {"A": 2.0, "B": 20.0}
