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
                pa.array(["2026-04-11T18:00:00"], type=pa.string()).cast(
                    pa.timestamp("us")
                ),
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


def test_fetch_json_ndjson_to_stdout(sample_table):
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
    # One JSON object per line
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["value"] == 21.66
    assert record["tsid"] == "T"


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
        result = runner.invoke(
            app, ["fetch", "T", "--format", "parquet", "--out", str(out)]
        )
    assert result.exit_code == 0
    assert out.exists()
    import pyarrow.parquet as pq

    round_trip = pq.read_table(out)
    assert round_trip.num_rows == 1


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
