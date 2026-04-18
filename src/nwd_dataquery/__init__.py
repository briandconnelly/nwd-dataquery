"""Async Python client for USACE NWD Dataquery 2.0.

Importing this package injects the system trust store into Python's SSL stack
via ``truststore.inject_into_ssl()``. USACE ``.mil`` domains often require the
OS trust store rather than certifi's bundle; this makes the package work out
of the box on macOS and Linux.
"""

from __future__ import annotations

import truststore

truststore.inject_into_ssl()

from .client import AsyncDataQueryClient  # noqa: E402
from .errors import DataQueryError, UnknownTsidWarning  # noqa: E402

__version__ = "0.1.0"
__all__ = [
    "AsyncDataQueryClient",
    "DataQueryError",
    "UnknownTsidWarning",
    "__version__",
]
