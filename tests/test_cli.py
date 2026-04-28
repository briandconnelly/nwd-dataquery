import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
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


def test_fetch_ndjson_to_stdout(sample_table):
    """--format ndjson emits one JSON object per line."""
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
        result = runner.invoke(app, ["fetch", "T", "--format", "ndjson"])
    assert result.exit_code == 0
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["value"] == 21.66
    assert record["tsid"] == "T"


def test_fetch_json_to_stdout(sample_table):
    """--format json emits a single JSON document (array of row objects)."""
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
        result = runner.invoke(app, ["fetch", "T", "--format", "json"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert isinstance(payload, list)
    assert len(payload) == 1
    assert payload[0]["value"] == 21.66
    assert payload[0]["tsid"] == "T"


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
    """--format json --out PATH writes a single JSON array document."""
    out = tmp_path / "out.json"
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
    payload = json.loads(out.read_text())
    assert isinstance(payload, list)
    assert len(payload) == 1
    assert payload[0]["value"] == 21.66
    assert "21.66" not in result.stdout  # did not also print to stdout


def test_fetch_ndjson_to_file(sample_table, tmp_path: Path):
    """--format ndjson --out PATH writes one JSON object per line."""
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
        result = runner.invoke(app, ["fetch", "T", "--format", "ndjson", "--out", str(out)])
    assert result.exit_code == 0, result.stderr
    assert out.exists()
    lines = [line for line in out.read_text().splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["value"] == 21.66
    assert "21.66" not in result.stdout


def test_fetch_json_empty_result_emits_array():
    """--format json with no rows emits `[]` (no trailing newline). Exit 0."""
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
        result = runner.invoke(app, ["fetch", "T", "--format", "json"])
    assert result.exit_code == 0
    assert json.loads(result.stdout) == []
    assert result.stdout == "[]"  # no trailing newline


def test_fetch_ndjson_empty_result_is_empty():
    """--format ndjson with no rows emits empty stdout. Exit 0. Locks the
    contrast with --format json which emits `[]`.
    """
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
        result = runner.invoke(app, ["fetch", "T", "--format", "ndjson"])
    assert result.exit_code == 0
    assert result.stdout == ""


def test_fetch_fail_empty_exits_3_on_empty():
    """--fail-empty is the new canonical name for the empty-result exit-3 contract."""
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
        result = runner.invoke(app, ["fetch", "T", "--fail-empty"])
    assert result.exit_code == 3


def test_fetch_strict_quiet_suppresses_deprecation_warning():
    """--quiet ('Suppress warnings') silences the --strict deprecation message."""
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
        result = runner.invoke(app, ["fetch", "T", "--strict", "--quiet"])
    assert result.exit_code == 3  # exit-3 contract still honored
    assert "deprecated" not in result.stderr.lower()
    assert result.stderr == ""


def test_fetch_strict_still_works_but_warns():
    """--strict is preserved for one release; emits a deprecation warning to stderr."""
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
    assert "deprecated" in result.stderr.lower()
    assert "--fail-empty" in result.stderr


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


def test_raw_lookback_with_both_endpoints_exits_2():
    """Explicit --lookback alongside both --start and --end is an argument error for `raw` too."""
    with patch("nwd_dataquery.cli.AsyncDataQueryClient") as client_cls:
        result = runner.invoke(
            app,
            [
                "raw",
                "T",
                "--start",
                "2026-04-01",
                "--end",
                "2026-04-08",
                "--lookback",
                "30d",
            ],
        )
    assert result.exit_code == 2
    assert "--lookback" in result.stderr
    client_cls.assert_not_called()


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


def test_fetch_csv_no_header_to_stdout(sample_table):
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
        result = runner.invoke(app, ["fetch", "T", "--no-header"])
    assert result.exit_code == 0, result.stderr
    assert "timestamp" not in result.stdout  # header suppressed
    assert "21.66" in result.stdout


def test_fetch_csv_no_header_to_file(sample_table, tmp_path: Path):
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
        result = runner.invoke(app, ["fetch", "T", "--no-header", "--out", str(out)])
    assert result.exit_code == 0, result.stderr
    text = out.read_text()
    assert "timestamp" not in text
    assert "21.66" in text


@pytest.mark.parametrize("fmt", ["ndjson", "json", "parquet"])
def test_fetch_no_header_rejected_with_non_csv(fmt, tmp_path: Path):
    """--no-header only makes sense for CSV; combining with another format exits 2."""
    args = ["fetch", "T", "--no-header", "--format", fmt]
    if fmt == "parquet":
        args += ["--out", str(tmp_path / "x.pq")]
    with patch("nwd_dataquery.cli.AsyncDataQueryClient") as client_cls:
        result = runner.invoke(app, args)
    assert result.exit_code == 2
    assert "--no-header" in result.stderr
    client_cls.assert_not_called()


def test_fetch_lookback_with_both_endpoints_exits_2():
    """Explicit --lookback alongside both --start and --end is an argument error."""
    with patch("nwd_dataquery.cli.AsyncDataQueryClient") as client_cls:
        result = runner.invoke(
            app,
            [
                "fetch",
                "T",
                "--start",
                "2026-04-01",
                "--end",
                "2026-04-08",
                "--lookback",
                "30d",
            ],
        )
    assert result.exit_code == 2
    assert "--lookback" in result.stderr
    client_cls.assert_not_called()


def test_fetch_start_after_end_exits_2_before_request():
    """--start later than --end is a CLI argument error; client must not be constructed."""
    with patch("nwd_dataquery.cli.AsyncDataQueryClient") as client_cls:
        result = runner.invoke(
            app,
            ["fetch", "T", "--start", "2026-04-25", "--end", "2026-04-01"],
        )
    assert result.exit_code == 2
    assert "--start" in result.stderr
    assert "--end" in result.stderr
    client_cls.assert_not_called()


def test_describe_start_after_end_exits_2_before_request():
    """--start later than --end is now rejected for `describe` too (via the shared helper)."""
    with patch("nwd_dataquery.cli.AsyncDataQueryClient") as client_cls:
        result = runner.invoke(
            app,
            ["describe", "T", "--start", "2026-04-25", "--end", "2026-04-01"],
        )
    assert result.exit_code == 2
    assert "--start" in result.stderr
    assert "--end" in result.stderr
    client_cls.assert_not_called()


def test_raw_start_after_end_exits_2_before_request():
    """--start later than --end is now rejected for `raw` too (via the shared helper)."""
    with patch("nwd_dataquery.cli.AsyncDataQueryClient") as client_cls:
        result = runner.invoke(
            app,
            ["raw", "T", "--start", "2026-04-25", "--end", "2026-04-01"],
        )
    assert result.exit_code == 2
    assert "--start" in result.stderr
    assert "--end" in result.stderr
    client_cls.assert_not_called()


def test_fetch_accepts_iso_with_z_suffix(sample_table):
    """The --start/--end help text mentions ISO-8601 with offset; verify Z and +00:00 actually parse."""
    captured = {}

    async def fake_fetch(self, tsids, *, start=None, end=None, lookback=None):
        captured["start"] = start
        captured["end"] = end
        return sample_table

    with (
        patch("nwd_dataquery.cli.AsyncDataQueryClient.fetch", new=fake_fetch),
        patch(
            "nwd_dataquery.cli.AsyncDataQueryClient.aclose",
            new=AsyncMock(return_value=None),
        ),
    ):
        result = runner.invoke(
            app,
            [
                "fetch",
                "T",
                "--start",
                "2026-04-01T00:00:00Z",
                "--end",
                "2026-04-08T00:00:00+00:00",
            ],
        )
    assert result.exit_code == 0, result.stderr
    assert captured["start"] is not None
    assert captured["end"] is not None
    assert captured["start"].tzinfo is not None
    assert captured["end"].tzinfo is not None


def test_fetch_mixed_naive_aware_inverted_window_exits_2(sample_table):
    """Mixed naive --end and aware --start where start > end in UTC must exit 2
    cleanly, not raise TypeError from the naive/aware comparison.
    """
    with patch("nwd_dataquery.cli.AsyncDataQueryClient") as client_cls:
        result = runner.invoke(
            app,
            [
                "fetch",
                "T",
                "--start",
                "2026-04-25T00:00:00+00:00",  # aware
                "--end",
                "2026-04-01",  # naive (treated as UTC)
            ],
        )
    assert result.exit_code == 2, result.stderr
    assert "--start" in result.stderr
    assert "--end" in result.stderr
    client_cls.assert_not_called()


def test_fetch_mixed_naive_aware_valid_window_succeeds(sample_table):
    """Mixed naive --start and aware --end with a valid window must succeed,
    not raise TypeError from the naive/aware comparison.
    """
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
        result = runner.invoke(
            app,
            [
                "fetch",
                "T",
                "--start",
                "2026-04-01",  # naive (treated as UTC)
                "--end",
                "2026-04-08T00:00:00+00:00",  # aware
            ],
        )
    assert result.exit_code == 0, result.stderr


def test_fetch_no_lookback_with_both_endpoints_is_fine(sample_table):
    """Without explicit --lookback, both --start and --end is the canonical happy path."""
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
        result = runner.invoke(
            app,
            ["fetch", "T", "--start", "2026-04-01", "--end", "2026-04-08"],
        )
    assert result.exit_code == 0, result.stderr


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

    ts = pc.assume_timezone(  # ty:ignore[unresolved-attribute]
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
    payload = json.loads(result.stdout)
    assert isinstance(payload, list)
    assert len(payload) == 2
    by_tsid = {r["tsid"]: r["value"] for r in payload}
    assert by_tsid == {"A": 2.0, "B": 20.0}


def test_describe_for_http_status_error_5xx_with_body():
    from nwd_dataquery.cli import _describe

    req = httpx.Request("GET", "https://example.com/foo")
    resp = httpx.Response(503, json={"error": "Service unavailable"}, request=req)
    err = httpx.HTTPStatusError("503", request=req, response=resp)
    assert _describe(err) == "server returned 503 from example.com: Service unavailable"


def test_describe_for_http_status_error_4xx_no_body():
    from nwd_dataquery.cli import _describe

    req = httpx.Request("GET", "https://example.com/foo")
    resp = httpx.Response(404, content=b"not found", request=req)
    err = httpx.HTTPStatusError("404", request=req, response=resp)
    assert _describe(err) == "server returned 404 from example.com"


def test_describe_for_connect_timeout():
    from nwd_dataquery.cli import _describe

    req = httpx.Request("GET", "https://example.com/foo")
    assert (
        _describe(httpx.ConnectTimeout("timed out", request=req))
        == "connect timeout to example.com"
    )


def test_describe_for_read_timeout():
    from nwd_dataquery.cli import _describe

    req = httpx.Request("GET", "https://example.com/foo")
    assert (
        _describe(httpx.ReadTimeout("read timeout", request=req)) == "read timeout from example.com"
    )


def test_describe_for_connect_error():
    from nwd_dataquery.cli import _describe

    req = httpx.Request("GET", "https://example.com/foo")
    msg = _describe(httpx.ConnectError("conn refused", request=req))
    assert msg.startswith("connect failed to example.com: ")


def test_describe_for_other_transport_error():
    from nwd_dataquery.cli import _describe

    req = httpx.Request("GET", "https://example.com/foo")
    msg = _describe(httpx.NetworkError("net broke", request=req))
    assert msg == "transport error: net broke"


# --- Retry behavior helpers ---


def _failing_then_succeeds(failures, exception_factory):
    """Returns (handler, counter). Handler raises `exception_factory(request)`
    `failures` times, then returns 200 with empty payload.
    """
    counter = {"calls": 0}

    def handler(request):
        counter["calls"] += 1
        if counter["calls"] <= failures:
            raise exception_factory(request)
        return httpx.Response(200, json={})

    return handler, counter


def _always_fails(exception_factory):
    """Returns (handler, counter). Handler always raises `exception_factory(request)`."""
    counter = {"calls": 0}

    def handler(request):
        counter["calls"] += 1
        raise exception_factory(request)

    return handler, counter


def _patch_cli_client(monkeypatch, handler):
    """Monkeypatch nwd_dataquery.cli._client to return a mock-backed AsyncDataQueryClient.

    Returns nothing — the patching has the side effect.
    """
    from nwd_dataquery import client as client_module

    transport = httpx.MockTransport(handler)
    session = httpx.AsyncClient(transport=transport)
    monkeypatch.setattr(
        "nwd_dataquery.cli._client",
        lambda *, timeout, endpoint: client_module.AsyncDataQueryClient(session=session),
    )


def test_fetch_retries_then_succeeds_on_transport_error(monkeypatch):
    """ConnectError twice, then 200. Sleeps 1.0s, 2.0s. Exit 0."""
    handler, counter = _failing_then_succeeds(
        failures=2,
        exception_factory=lambda req: httpx.ConnectError("conn refused", request=req),
    )
    sleeps: list[float] = []
    monkeypatch.setattr("nwd_dataquery.cli.time.sleep", sleeps.append)
    _patch_cli_client(monkeypatch, handler)

    result = runner.invoke(app, ["fetch", "T", "--retries", "2", "--retry-backoff", "1.0"])
    assert result.exit_code == 0, result.stderr
    assert counter["calls"] == 3
    assert sleeps == [1.0, 2.0]


def test_fetch_retries_exhausted_exits_1_with_count(monkeypatch):
    """ReadTimeout 3 times with --retries 2. Three attempts, exit 1, message names retry count."""
    handler, counter = _always_fails(
        exception_factory=lambda req: httpx.ReadTimeout("slow", request=req),
    )
    monkeypatch.setattr("nwd_dataquery.cli.time.sleep", lambda s: None)
    _patch_cli_client(monkeypatch, handler)

    result = runner.invoke(app, ["fetch", "T", "--retries", "2", "--retry-backoff", "0.01"])
    assert result.exit_code == 1
    assert counter["calls"] == 3  # initial + 2 retries
    assert "(after 2 retries)" in result.stderr
    assert "read timeout" in result.stderr


def test_fetch_no_retry_on_4xx(monkeypatch):
    """404 → single attempt, exit 1, message names status code."""
    counter = {"calls": 0}

    def handler(request):
        counter["calls"] += 1
        return httpx.Response(404, content=b"not found")

    _patch_cli_client(monkeypatch, handler)

    result = runner.invoke(app, ["fetch", "T", "--retries", "5"])
    assert result.exit_code == 1
    assert counter["calls"] == 1
    assert "404" in result.stderr
    assert "(after" not in result.stderr  # no retries-suffix


def test_fetch_retries_on_5xx(monkeypatch):
    """503 once, then 200. Two attempts, exit 0."""
    counter = {"calls": 0}

    def handler(request):
        counter["calls"] += 1
        if counter["calls"] == 1:
            return httpx.Response(503, content=b"unavailable")
        return httpx.Response(200, json={})

    monkeypatch.setattr("nwd_dataquery.cli.time.sleep", lambda s: None)
    _patch_cli_client(monkeypatch, handler)

    result = runner.invoke(app, ["fetch", "T", "--retries", "2"])
    assert result.exit_code == 0, result.stderr
    assert counter["calls"] == 2


def test_fetch_no_retry_on_data_query_error(monkeypatch):
    """200 with {'error': '...'} → single attempt, exit 2."""
    counter = {"calls": 0}

    def handler(request):
        counter["calls"] += 1
        return httpx.Response(200, json={"error": "bad tsid"})

    _patch_cli_client(monkeypatch, handler)

    result = runner.invoke(app, ["fetch", "T", "--retries", "5"])
    assert result.exit_code == 2
    assert counter["calls"] == 1
    assert "bad tsid" in result.stderr


def test_fetch_retries_zero_snap_fails(monkeypatch):
    """ConnectError with --retries 0 → single attempt, exit 1, no warnings, no sleeps."""
    handler, counter = _always_fails(
        exception_factory=lambda req: httpx.ConnectError("conn refused", request=req),
    )
    sleeps: list[float] = []
    monkeypatch.setattr("nwd_dataquery.cli.time.sleep", sleeps.append)
    _patch_cli_client(monkeypatch, handler)

    result = runner.invoke(app, ["fetch", "T", "--retries", "0"])
    assert result.exit_code == 1
    assert counter["calls"] == 1
    assert sleeps == []
    assert "warning:" not in result.stderr
    assert "(after" not in result.stderr  # no retries-suffix when retries=0


def test_fetch_quiet_suppresses_retry_warnings(monkeypatch):
    """Same retry-then-succeed scenario plus --quiet. Exit 0, no warning: lines on stderr."""
    handler, counter = _failing_then_succeeds(
        failures=2,
        exception_factory=lambda req: httpx.ConnectError("conn refused", request=req),
    )
    monkeypatch.setattr("nwd_dataquery.cli.time.sleep", lambda s: None)
    _patch_cli_client(monkeypatch, handler)

    result = runner.invoke(
        app, ["fetch", "T", "--retries", "2", "--retry-backoff", "0.01", "--quiet"]
    )
    assert result.exit_code == 0, result.stderr
    assert counter["calls"] == 3
    assert "warning:" not in result.stderr


def test_fetch_rejects_negative_retries():
    """--retries -1 exits 2 from Typer's min= validator before any HTTP call."""
    result = runner.invoke(app, ["fetch", "T", "--retries", "-1"])
    assert result.exit_code == 2


def test_fetch_rejects_negative_retry_backoff():
    """--retry-backoff -1 exits 2 from Typer's min= validator before any HTTP call."""
    result = runner.invoke(app, ["fetch", "T", "--retry-backoff", "-1"])
    assert result.exit_code == 2


def test_fetch_rejects_nan_retry_backoff():
    """--retry-backoff nan exits 2 via the _require_finite callback. Typer's
    min=0.0 alone does NOT reject nan; the explicit callback prevents
    time.sleep(nan) from raising ValueError inside the retry handler.
    """
    result = runner.invoke(app, ["fetch", "T", "--retry-backoff", "nan"])
    assert result.exit_code == 2


def test_fetch_rejects_inf_retry_backoff():
    """--retry-backoff inf exits 2 via the _require_finite callback. inf would
    pass min=0.0 (inf >= 0 is True) but cause time.sleep(inf) to hang forever.
    """
    result = runner.invoke(app, ["fetch", "T", "--retry-backoff", "inf"])
    assert result.exit_code == 2


def test_describe_quiet_suppresses_warnings(monkeypatch):
    """--quiet on describe registers the UnknownTsidWarning suppression filter
    (covers the new `if quiet:` block added when describe gained --quiet).
    """
    meta = {"LWSC": {"name": "X", "timeseries": {"T": {}}}}
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
        result = runner.invoke(app, ["describe", "T", "--quiet"])
    assert result.exit_code == 0, result.stderr
    assert json.loads(result.stdout) == meta
