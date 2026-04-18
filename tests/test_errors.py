import warnings

import pytest

from nwd_dataquery.errors import DataQueryError, UnknownTsidWarning


def test_data_query_error_is_runtime_error():
    assert issubclass(DataQueryError, RuntimeError)
    with pytest.raises(DataQueryError, match="boom"):
        raise DataQueryError("boom")


def test_unknown_tsid_warning_is_user_warning():
    assert issubclass(UnknownTsidWarning, UserWarning)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warnings.warn("empty", UnknownTsidWarning)
    assert len(caught) == 1
    assert issubclass(caught[0].category, UnknownTsidWarning)
