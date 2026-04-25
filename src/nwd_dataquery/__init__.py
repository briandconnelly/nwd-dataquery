"""Async Python client for USACE NWD Dataquery 2.0."""

from __future__ import annotations

from importlib.metadata import version as _pkg_version

from .client import AsyncDataQueryClient
from .errors import DataQueryError, UnknownTsidWarning

__version__ = _pkg_version("nwd-dataquery")
__all__ = [
    "AsyncDataQueryClient",
    "DataQueryError",
    "UnknownTsidWarning",
    "__version__",
]
