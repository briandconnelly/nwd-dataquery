"""Exception and warning types for nwd-dataquery."""

from __future__ import annotations


class DataQueryError(RuntimeError):
    """Server returned an error response (HTTP 200 with text/plain error body)."""


class UnknownTsidWarning(UserWarning):
    """Empty payload — the tsid doesn't exist or has no data in the window.

    The Dataquery 2.0 server cannot distinguish these two cases; this warning
    fires whenever the payload is empty so callers can notice silently-empty
    results.
    """
