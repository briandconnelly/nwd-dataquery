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
