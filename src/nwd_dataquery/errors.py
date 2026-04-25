"""Exception and warning types for nwd-dataquery."""

from __future__ import annotations


class DataQueryError(RuntimeError):
    """Server returned an error response or a malformed payload.

    Raised when the upstream body contains a top-level ``"error"`` key
    (regardless of ``Content-Type``) or when the decoded JSON is not a
    JSON object.
    """


class UnknownTsidWarning(UserWarning):
    """Empty payload — the tsid doesn't exist or has no data in the window.

    The Dataquery 2.0 server cannot distinguish these two cases; this warning
    fires whenever the payload is empty so callers can notice silently-empty
    results.
    """
