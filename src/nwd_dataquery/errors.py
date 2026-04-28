"""Exception and warning types for nwd-dataquery."""

from __future__ import annotations


class DataQueryError(RuntimeError):
    """Server returned an error response or a malformed payload.

    Raised when the upstream body contains a top-level ``"error"`` key
    (regardless of ``Content-Type``) or when the decoded JSON is not a
    JSON object.
    """


class DataQueryParseError(RuntimeError):
    """The upstream payload could not be parsed into a tabular result.

    Raised when row-level shape or timestamp parsing fails. Sibling of
    ``DataQueryError`` so callers can distinguish server-reported errors
    from local parse failures.
    """


class UnknownTsidWarning(UserWarning):
    """Empty payload — the tsid doesn't exist or has no data in the window.

    The Dataquery 2.0 server cannot distinguish these two cases; this warning
    fires whenever the payload is empty so callers can notice silently-empty
    results.
    """
