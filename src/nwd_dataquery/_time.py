"""Shared datetime helpers."""

from __future__ import annotations

from datetime import UTC, datetime


def to_utc(dt: datetime) -> datetime:
    """Treat a naive datetime as UTC; return aware datetimes unchanged."""
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt
