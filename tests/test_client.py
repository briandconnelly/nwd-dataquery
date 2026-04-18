import json
from datetime import datetime, timedelta, timezone

import httpx
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
            start=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end=datetime(2026, 1, 8, tzinfo=timezone.utc),
        )
    await client.aclose()

    assert captured["url"].startswith(ENDPOINT)
    assert captured["params"]["timezone"] == "GMT"
    assert captured["params"]["query"] == json.dumps(
        ["LWSC.Elev-Lake.Ave.1Hour.0.NWSRADIO-RAW"]
    )
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

    start = datetime.fromisoformat(
        captured["params"]["startdate"].replace("Z", "+00:00")
    )
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


async def test_async_context_manager_closes_session():
    called = {"closed": False}

    async def fake_close():
        called["closed"] = True

    async with AsyncDataQueryClient() as client:
        assert client._session is not None  # type: ignore[attr-defined]
        client._session.aclose = fake_close  # type: ignore[method-assign]
    assert called["closed"]
