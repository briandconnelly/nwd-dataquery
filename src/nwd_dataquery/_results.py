"""Result objects returned by AsyncDataQueryClient."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

    from .client import DataQueryPayload


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
        present: set[str] = set()
        for loc_body in self.payload.values():
            if not isinstance(loc_body, dict):
                continue
            ts = loc_body.get("timeseries") or {}
            present.update(ts.keys())
        seen: set[str] = set()
        out: list[str] = []
        for t in self.requested_tsids:
            if t in seen:
                continue
            seen.add(t)
            if t not in present:
                out.append(t)
        return tuple(out)


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
        present: set[str] = set()
        for loc_body in self.payload.values():
            if not isinstance(loc_body, dict):
                continue
            ts = loc_body.get("timeseries") or {}
            present.update(ts.keys())
        seen: set[str] = set()
        out: list[str] = []
        for t in self.requested_tsids:
            if t in seen:
                continue
            seen.add(t)
            if t not in present:
                out.append(t)
        return tuple(out)
