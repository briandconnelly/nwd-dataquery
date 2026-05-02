"""Shared datetime helpers."""

from __future__ import annotations

from datetime import UTC, datetime


def to_utc(dt: datetime) -> datetime:
    """Treat a naive datetime as UTC; return aware datetimes unchanged."""
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt


def is_window_inverted(start: datetime, end: datetime) -> bool:
    """True iff ``start`` is strictly after ``end``, normalizing naive
    datetimes as UTC so naive/aware mixes compare safely.

    The footgun this hides: comparing a naive datetime to an aware one
    raises TypeError. Both sides are normalized through :func:`to_utc`
    before comparison.
    """
    return to_utc(start) > to_utc(end)
