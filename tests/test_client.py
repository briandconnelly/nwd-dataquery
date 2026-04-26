import json
from datetime import UTC, datetime, timedelta

import httpx
import pyarrow as pa
import pytest

from nwd_dataquery.client import ENDPOINT, AsyncDataQueryClient
from nwd_dataquery.errors import DataQueryError, UnknownTsidWarning


def _mock_client(handler) -> AsyncDataQueryClient:
    transport = httpx.MockTransport(handler)
    session = httpx.AsyncClient(transport=transport)
    return AsyncDataQueryClient(session=session)


async def test_fetch_raw_builds_expected_query_params():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={})

    client = _mock_client(handler)
    with pytest.warns(UnknownTsidWarning):
        await client.fetch_raw(
            "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW",
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 1, 8, tzinfo=UTC),
        )
    await client.aclose()

    assert captured["url"].startswith(ENDPOINT)
    assert captured["params"]["timezone"] == "GMT"
    assert captured["params"]["query"] == json.dumps(["LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW"])
    assert captured["params"]["startdate"] == "2026-01-01T00:00:00Z"
    assert captured["params"]["enddate"] == "2026-01-08T00:00:00Z"


async def test_fetch_raw_default_window_is_last_7_days():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={})

    client = _mock_client(handler)
    with pytest.warns(UnknownTsidWarning):
        await client.fetch_raw("T")
    await client.aclose()

    start = datetime.fromisoformat(captured["params"]["startdate"].replace("Z", "+00:00"))
    end = datetime.fromisoformat(captured["params"]["enddate"].replace("Z", "+00:00"))
    delta = end - start
    assert timedelta(days=6, hours=23) <= delta <= timedelta(days=7, hours=1)


async def test_fetch_raw_accepts_list_of_tsids():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={})

    client = _mock_client(handler)
    with pytest.warns(UnknownTsidWarning):
        await client.fetch_raw(["A", "B"])
    await client.aclose()

    assert captured["params"]["query"] == json.dumps(["A", "B"])


async def test_fetch_raw_raises_on_empty_tsid_list():
    client = _mock_client(lambda req: httpx.Response(200, json={}))
    with pytest.raises(ValueError, match="at least one tsid"):
        await client.fetch_raw([])
    await client.aclose()


async def test_fetch_raw_raises_data_query_error_on_text_plain_error_body():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=json.dumps({"error": "Malformed query"}),
            headers={"content-type": "text/plain"},
        )

    client = _mock_client(handler)
    with pytest.raises(DataQueryError, match="Malformed query"):
        await client.fetch_raw("T")
    await client.aclose()


async def test_fetch_raw_warns_on_empty_payload():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    client = _mock_client(handler)
    with pytest.warns(UnknownTsidWarning, match="Empty response"):
        payload = await client.fetch_raw("T")
    await client.aclose()

    assert payload == {}


async def test_fetch_raw_returns_payload_on_success():
    data = {"LWSC": {"name": "X", "timeseries": {}}}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=data)

    client = _mock_client(handler)
    # empty timeseries dict still triggers the empty-payload warning because
    # the parser will produce nothing — but fetch_raw's emptiness check is
    # strictly about the top-level payload, which is NOT empty here.
    payload = await client.fetch_raw("T")
    await client.aclose()

    assert payload == data


async def test_async_context_manager_does_not_build_session_eagerly(monkeypatch):
    """Context entry must not trigger session creation (and therefore AIA fetch)."""
    from nwd_dataquery import client as client_mod

    def _explode(url: str) -> object:
        raise AssertionError("session must not be built on context entry")

    monkeypatch.setattr(client_mod, "_ssl_context_for", _explode)

    async with AsyncDataQueryClient() as client:
        assert client._session is None  # type: ignore[attr-defined]


async def test_async_context_manager_closes_session_after_use():
    called = {"closed": False}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    async def fake_close():
        called["closed"] = True

    transport = httpx.MockTransport(handler)
    session = httpx.AsyncClient(transport=transport)
    session.aclose = fake_close  # type: ignore[method-assign]

    # Inject the session so the context manager owns and closes it.
    client = AsyncDataQueryClient()
    client._session = session  # type: ignore[attr-defined]
    client._owns_session = True  # type: ignore[attr-defined]
    async with client:
        with pytest.warns(UnknownTsidWarning):
            await client.fetch_raw("T")
    assert called["closed"]


async def test_aclose_without_request_is_noop():
    """Exiting the context without making a request must not error."""
    async with AsyncDataQueryClient():
        pass  # no requests made, no session built


def _success_handler(payload):
    def handler(request):
        return httpx.Response(200, json=payload)

    return handler


async def test_fetch_returns_pyarrow_table_by_default():
    payload = {
        "LWSC": {
            "name": "Lake Washington",
            "timeseries": {
                "LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW": {
                    "parameter": "Elev-Lake",
                    "units": "FT",
                    "values": [["2026-04-11T18:00:00", 21.66, 0]],
                }
            },
        }
    }
    client = _mock_client(_success_handler(payload))
    table = await client.fetch("LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW")
    await client.aclose()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table["value"][0].as_py() == 21.66


async def test_fetch_backend_polars_returns_polars_frame():
    pl = pytest.importorskip("polars")
    payload = {
        "LWSC": {
            "name": "X",
            "timeseries": {
                "T": {
                    "parameter": "P",
                    "units": "U",
                    "values": [["2026-04-11T18:00:00", 1.0, 0]],
                }
            },
        }
    }
    client = _mock_client(_success_handler(payload))
    df = await client.fetch("T", backend="polars")
    await client.aclose()
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (1, 7)


async def test_fetch_backend_pandas_returns_pandas_frame():
    pd = pytest.importorskip("pandas")
    payload = {
        "LWSC": {
            "name": "X",
            "timeseries": {
                "T": {
                    "parameter": "P",
                    "units": "U",
                    "values": [["2026-04-11T18:00:00", 1.0, 0]],
                }
            },
        }
    }
    client = _mock_client(_success_handler(payload))
    df = await client.fetch("T", backend="pandas")
    await client.aclose()
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 7)


async def test_fetch_rejects_unknown_backend():
    client = _mock_client(_success_handler({}))
    with pytest.warns(UnknownTsidWarning), pytest.raises(ValueError, match="unknown backend"):
        await client.fetch("T", backend="duckdb")  # type: ignore[arg-type]
    await client.aclose()


async def test_describe_returns_metadata_without_values():
    payload = {
        "LWSC": {
            "name": "Lake Washington",
            "coordinates": {"latitude": 47.66, "longitude": -122.39},
            "timeseries": {
                "T": {
                    "parameter": "Elev-Lake",
                    "units": "FT",
                    "notes": "(2001-2026)",
                    "values": [["2026-04-11T18:00:00", 21.66, 0]],
                }
            },
        }
    }
    client = _mock_client(_success_handler(payload))
    meta = await client.describe("T")
    await client.aclose()

    assert meta["LWSC"]["name"] == "Lake Washington"
    assert "values" not in meta["LWSC"]["timeseries"]["T"]
    assert meta["LWSC"]["timeseries"]["T"]["parameter"] == "Elev-Lake"
    assert meta["LWSC"]["timeseries"]["T"]["units"] == "FT"


# --- coverage gap tests ---


async def test_fetch_raw_rejects_overspecified_window():
    """fetch_raw raises ValueError when start, end, AND lookback are all explicitly given."""
    client = _mock_client(lambda req: httpx.Response(200, json={}))
    with pytest.raises(ValueError, match="lookback"):
        await client.fetch_raw(
            "T",
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 1, 8, tzinfo=UTC),
            lookback=timedelta(days=3),
        )
    await client.aclose()


async def test_fetch_raw_start_none_end_provided():
    """client.py:80 — start=None, end provided → start = end - lookback."""
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={})

    client = _mock_client(handler)
    end = datetime(2026, 4, 18, tzinfo=UTC)
    lb = timedelta(days=3)
    with pytest.warns(UnknownTsidWarning):
        await client.fetch_raw("T", end=end, lookback=lb)
    await client.aclose()

    start_str = captured["params"]["startdate"]
    end_str = captured["params"]["enddate"]
    assert start_str == "2026-04-15T00:00:00Z"
    assert end_str == "2026-04-18T00:00:00Z"


async def test_fetch_raw_start_provided_end_none_fills_end_with_now():
    """client.py: start provided, end=None → end is filled with now() so a closed window is sent."""
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["params"] = dict(request.url.params)
        return httpx.Response(200, json={})

    client = _mock_client(handler)
    start = datetime(2026, 4, 1, tzinfo=UTC)
    before = datetime.now(UTC)
    with pytest.warns(UnknownTsidWarning):
        await client.fetch_raw("T", start=start)
    after = datetime.now(UTC)
    await client.aclose()

    assert captured["params"]["startdate"] == "2026-04-01T00:00:00Z"
    assert "enddate" in captured["params"]  # previously omitted
    end_str = captured["params"]["enddate"]
    end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
    # end was filled with now(); allow a small window for clock motion during the call
    assert before - timedelta(seconds=2) <= end_dt <= after + timedelta(seconds=2)


async def test_fetch_raw_non_json_body_raises_value_error():
    """A 200 OK response whose body isn't JSON propagates a ValueError."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"not json at all",
            headers={"content-type": "text/plain"},
        )

    client = _mock_client(handler)
    with pytest.raises(ValueError):
        await client.fetch_raw("T")
    await client.aclose()


async def test_fetch_raw_raises_data_query_error_on_application_json_error_body():
    """A JSON error body served as application/json must still surface as DataQueryError."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"error": "Malformed query"},
            headers={"content-type": "application/json"},
        )

    client = _mock_client(handler)
    with pytest.raises(DataQueryError, match="Malformed query"):
        await client.fetch_raw("T")
    await client.aclose()


async def test_fetch_raw_raises_data_query_error_when_content_type_missing():
    """No Content-Type header, JSON error body — still raises DataQueryError."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=json.dumps({"error": "boom"}).encode())

    client = _mock_client(handler)
    with pytest.raises(DataQueryError, match="boom"):
        await client.fetch_raw("T")
    await client.aclose()


@pytest.mark.parametrize(
    ("body_bytes", "type_name"),
    [
        (b"[1, 2, 3]", "list"),
        (b'"oops"', "str"),
        (b"null", "NoneType"),
        (b"42", "int"),
    ],
)
async def test_fetch_raw_raises_on_non_dict_payload(body_bytes, type_name):
    """Non-object JSON payloads violate the function contract — raise clearly."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body_bytes)

    client = _mock_client(handler)
    with pytest.raises(DataQueryError, match=f"got {type_name}"):
        await client.fetch_raw("T")
    await client.aclose()


async def test_fetch_raw_http_error_takes_precedence_over_json_decode_error():
    """5xx with non-JSON body should surface the HTTP error, not a JSON decode error."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"not json")

    client = _mock_client(handler)
    with pytest.raises(httpx.HTTPStatusError):
        await client.fetch_raw("T")
    await client.aclose()


async def test_fetch_raw_http_error_takes_precedence_over_shape_violation():
    """5xx with a non-dict JSON body raises HTTPStatusError, not DataQueryError."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"[1, 2, 3]")

    client = _mock_client(handler)
    with pytest.raises(httpx.HTTPStatusError):
        await client.fetch_raw("T")
    await client.aclose()


async def test_fetch_raw_5xx_with_error_body_still_surfaces_data_query_error():
    """A non-2xx response carrying {"error": ...} keeps the actionable message."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"error": "Service unavailable"})

    client = _mock_client(handler)
    with pytest.raises(DataQueryError, match="Service unavailable"):
        await client.fetch_raw("T")
    await client.aclose()


async def test_iso_naive_datetime_treated_as_utc():
    """client.py:166 — _iso() with naive datetime prepends UTC offset."""
    from nwd_dataquery.client import _iso

    naive = datetime(2026, 1, 1)  # no tzinfo
    result = _iso(naive)
    assert result == "2026-01-01T00:00:00Z"


async def test_https_endpoint_passes_aia_context_to_session(monkeypatch):
    """HTTPS endpoints build an AIA-aware SSLContext and pass it to httpx.AsyncClient."""
    from nwd_dataquery import client as client_mod

    sentinel = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(client_mod, "_ssl_context_for", lambda url: sentinel)

    class _FakeAsyncClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        async def aclose(self) -> None:  # called by aclose()
            pass

    monkeypatch.setattr(client_mod.httpx, "AsyncClient", _FakeAsyncClient)

    client = AsyncDataQueryClient(endpoint="https://example.com/x")
    client._get_or_build_session()
    assert captured["verify"] is sentinel


async def test_non_https_endpoint_skips_aia_context(monkeypatch):
    """Non-HTTPS endpoints must not trigger AIA fetching (would block on bogus URLs)."""
    from nwd_dataquery import client as client_mod

    aia_calls: list[str] = []
    captured: dict[str, object] = {}

    def _stub(url: str) -> object:
        aia_calls.append(url)
        raise AssertionError("AIA fetch should not be invoked for non-HTTPS endpoints")

    monkeypatch.setattr(client_mod, "_ssl_context_for", _stub)

    class _FakeAsyncClient:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        async def aclose(self) -> None:
            pass

    monkeypatch.setattr(client_mod.httpx, "AsyncClient", _FakeAsyncClient)

    client = AsyncDataQueryClient(endpoint="http://example.invalid/x")
    client._get_or_build_session()
    assert aia_calls == []
    assert "verify" not in captured


async def test_ssl_context_cached_per_origin_not_full_url(monkeypatch):
    """Different paths on the same host share one SSLContext (cache is by origin)."""
    from nwd_dataquery import client as client_mod

    fetched_urls: list[str] = []

    class _FakeChaser:
        def make_ssl_context_for_url(self, url: str) -> object:
            fetched_urls.append(url)
            return object()

    monkeypatch.setattr(client_mod, "AiaChaser", _FakeChaser)
    client_mod._build_ssl_context.cache_clear()

    a = client_mod._ssl_context_for("https://example.com/path/a")
    b = client_mod._ssl_context_for("https://example.com/path/b?q=1")
    c = client_mod._ssl_context_for("https://other.example.com/")

    assert a is b  # same origin → same cached context
    assert a is not c  # different origin → distinct context
    assert fetched_urls == ["https://example.com", "https://other.example.com"]
