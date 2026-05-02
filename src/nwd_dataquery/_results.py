"""Result objects returned by AsyncDataQueryClient."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

    from .client import DataQueryPayload


def _compute_unknown_tsids(
    requested: tuple[str, ...],
    payload: DataQueryPayload,
) -> tuple[str, ...]:
    """Return tsids in `requested` not present under any payload location's
    `timeseries` dict. Order matches `requested`; duplicates collapsed by
    first occurrence; tsids that appear in the payload are never returned.
    """
    present: set[str] = set()
    for loc_body in payload.values():
        if not isinstance(loc_body, dict):
            continue
        ts = loc_body.get("timeseries")
        if not isinstance(ts, dict):
            continue
        present.update(ts.keys())
    seen: set[str] = set()
    out: list[str] = []
    for t in requested:
        if t in seen:
            continue
        seen.add(t)
        if t not in present:
            out.append(t)
    return tuple(out)


@dataclass(frozen=True, slots=True)
class QueryResult:
    """Result of a `fetch()` call: table + raw payload + request context."""

    table: pa.Table
    payload: DataQueryPayload
    requested_tsids: tuple[str, ...]
    resolved_window: tuple[datetime, datetime]
    endpoint: str
    warnings: tuple[Warning, ...]

    @property
    def is_empty(self) -> bool:
        return self.table.num_rows == 0

    @property
    def unknown_tsids(self) -> tuple[str, ...]:
        return _compute_unknown_tsids(self.requested_tsids, self.payload)


@dataclass(frozen=True, slots=True)
class MetadataResult:
    """Result of a `describe()` call: metadata-only (no table)."""

    payload: DataQueryPayload
    requested_tsids: tuple[str, ...]
    resolved_window: tuple[datetime, datetime]
    endpoint: str
    warnings: tuple[Warning, ...]

    @property
    def unknown_tsids(self) -> tuple[str, ...]:
        return _compute_unknown_tsids(self.requested_tsids, self.payload)
