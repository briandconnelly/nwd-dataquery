from importlib.metadata import version as _pkg_version

import nwd_dataquery
from nwd_dataquery import (
    AsyncDataQueryClient,
    DataQueryError,
    DataQueryParseError,
    MetadataResult,
    QueryResult,
    UnknownTsidWarning,
    __version__,
)


def test_public_exports_are_available():
    assert AsyncDataQueryClient is nwd_dataquery.AsyncDataQueryClient
    assert DataQueryError is nwd_dataquery.DataQueryError
    assert DataQueryParseError is nwd_dataquery.DataQueryParseError
    assert MetadataResult is nwd_dataquery.MetadataResult
    assert QueryResult is nwd_dataquery.QueryResult
    assert UnknownTsidWarning is nwd_dataquery.UnknownTsidWarning


def test_version_is_defined():
    assert isinstance(__version__, str)
    assert __version__ == _pkg_version("nwd-dataquery")


def test_all_contains_expected_names():
    assert set(nwd_dataquery.__all__) == {
        "AsyncDataQueryClient",
        "DataQueryError",
        "DataQueryParseError",
        "DataQueryPayload",
        "LocationEntry",
        "MetadataResult",
        "QueryResult",
        "TimeseriesEntry",
        "UnknownTsidWarning",
        "__version__",
    }


def test_version_falls_back_when_distribution_missing(monkeypatch):
    """Source checkouts without dist metadata still get a usable __version__."""
    import importlib
    import importlib.metadata

    def _raise(name):
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", _raise)
    reloaded = importlib.reload(nwd_dataquery)
    try:
        assert reloaded.__version__ == "0+unknown"
    finally:
        # Restore real metadata + module so later tests see a valid version.
        monkeypatch.undo()
        importlib.reload(nwd_dataquery)
