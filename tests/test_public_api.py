from importlib.metadata import version as _pkg_version

import nwd_dataquery
from nwd_dataquery import (
    AsyncDataQueryClient,
    DataQueryError,
    UnknownTsidWarning,
    __version__,
)


def test_public_exports_are_available():
    assert AsyncDataQueryClient is nwd_dataquery.AsyncDataQueryClient
    assert DataQueryError is nwd_dataquery.DataQueryError
    assert UnknownTsidWarning is nwd_dataquery.UnknownTsidWarning


def test_version_is_defined():
    assert isinstance(__version__, str)
    assert __version__ == _pkg_version("nwd-dataquery")


def test_all_contains_expected_names():
    assert set(nwd_dataquery.__all__) == {
        "AsyncDataQueryClient",
        "DataQueryError",
        "UnknownTsidWarning",
        "__version__",
    }
