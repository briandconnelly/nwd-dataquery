"""Test fixtures shared across the suite."""

from __future__ import annotations

import ssl

import pytest


@pytest.fixture(autouse=True)
def _stub_aia_chaser(request, monkeypatch):
    """Replace `AiaChaser` with a fake that returns a default SSLContext.

    Without this, the first build of a session for an HTTPS endpoint
    triggers a real AIA fetch — which breaks cassette replay (VCR has
    no recording of the `cacerts.digicert.com` request) and adds a
    hidden network dependency to the unit suite. Live tests opt out
    via the `live` marker.
    """
    if request.node.get_closest_marker("live"):
        return

    from nwd_dataquery import client as client_mod

    class _FakeChaser:
        def make_ssl_context_for_url(self, _url: str) -> ssl.SSLContext:
            return ssl.create_default_context()

    monkeypatch.setattr(client_mod, "AiaChaser", _FakeChaser)
    client_mod._build_ssl_context.cache_clear()
    yield
    client_mod._build_ssl_context.cache_clear()
