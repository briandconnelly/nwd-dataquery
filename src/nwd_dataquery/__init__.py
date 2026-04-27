"""Async Python client for USACE NWD Dataquery 2.0."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

from .client import (
    AsyncDataQueryClient,
    DataQueryPayload,
    LocationEntry,
    TimeseriesEntry,
)
from .errors import DataQueryError, UnknownTsidWarning

try:
    __version__ = _pkg_version("nwd-dataquery")
except PackageNotFoundError:
    # Source checkout without installed distribution metadata (e.g. PYTHONPATH=src).
    __version__ = "0+unknown"
__all__ = [
    "AsyncDataQueryClient",
    "DataQueryError",
    "DataQueryPayload",
    "LocationEntry",
    "TimeseriesEntry",
    "UnknownTsidWarning",
    "__version__",
]
