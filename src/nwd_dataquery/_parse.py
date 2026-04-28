"""Flatten a Dataquery 2.0 JSON response into a pyarrow.Table.

The server returns a two-level nested shape:
    {location: {..., timeseries: {tsid: {values, units, parameter, ...}}}}

Values are [timestamp_str, value, quality] triples. Timestamps are naive ISO
strings that represent UTC when the server was asked with timezone=GMT.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from .errors import DataQueryParseError

SCHEMA = pa.schema(
    [
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("value", pa.float64()),
        pa.field("quality", pa.int64()),
        pa.field("tsid", pa.string()),
        pa.field("location", pa.string()),
        pa.field("parameter", pa.string()),
        pa.field("units", pa.string()),
    ]
)


def parse_payload(payload: dict[str, Any]) -> pa.Table:
    """Flatten a Dataquery 2.0 response into a long-format pyarrow.Table."""
    ts_raw: list[str] = []
    vals: list[float | None] = []
    quals: list[int | None] = []
    ids: list[str] = []
    locs: list[str] = []
    params: list[str | None] = []
    units_col: list[str | None] = []

    for location, loc_body in payload.items():
        if not isinstance(loc_body, dict):
            continue
        for tsid, ts_body in (loc_body.get("timeseries") or {}).items():
            if not isinstance(ts_body, dict):
                continue
            units = ts_body.get("units")
            parameter = ts_body.get("parameter")
            for row in ts_body.get("values") or []:
                if not row:
                    continue
                ts_raw.append(row[0])
                vals.append(row[1] if len(row) > 1 else None)
                quals.append(row[2] if len(row) > 2 else None)
                ids.append(tsid)
                locs.append(location)
                params.append(parameter)
                units_col.append(units)

    if not ts_raw:
        return SCHEMA.empty_table()

    try:
        parsed = pc.strptime(pa.array(ts_raw), format="%Y-%m-%dT%H:%M:%S", unit="us")  # ty:ignore[unresolved-attribute]
        parsed = pc.assume_timezone(parsed, "UTC")  # ty:ignore[unresolved-attribute]
    except pa.ArrowInvalid as exc:
        raise DataQueryParseError(
            f"could not parse timestamp(s) in payload: {exc}; "
            f"first offending row: tsid={ids[0]!r}, timestamp={ts_raw[0]!r}"
        ) from exc

    return pa.table(
        {
            "timestamp": parsed,
            "value": pa.array(vals, type=pa.float64()),
            "quality": pa.array(quals, type=pa.int64()),
            "tsid": pa.array(ids, type=pa.string()),
            "location": pa.array(locs, type=pa.string()),
            "parameter": pa.array(params, type=pa.string()),
            "units": pa.array(units_col, type=pa.string()),
        },
        schema=SCHEMA,
    )
