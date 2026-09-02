"""The LD proxy: validation at the edge, passthrough in the middle, 502 for the upstream.

Why this endpoint exists at all is in app/services/ld_service.py — the sandbox has no DNS and
no internet egress, so a script cannot reach the LD server and results-api stands in.

Nothing here touches the network: the router tests replace the service, and the service tests
replace its aiohttp session. The properties under test are the ones a caller can be hurt by —
that a caller-supplied string cannot reach the outbound query unshaped, that an upstream
failure is not reported as the caller's fault, and that the upstream's body never reaches a
model-authored script.

The ASGI app is driven directly and the coroutines are run with asyncio.run: httpx (so
TestClient) and pytest-asyncio are both absent here, the same constraint test_response_caps.py
works under.
"""

import asyncio
import json

import aiohttp
import pytest
from fastapi import FastAPI

from app.dependencies import get_ld_service
from app.routers import ld as ld_router
from app.services.ld_service import LD_ENTRY_FIELDS, LDService, LDUpstreamError

ENTRY = {
    "variation1": "12:49048170:A:C",
    "variation2": "12:49048999:G:T",
    "r2": 0.91,
    "d_prime": 0.98,
}


class StubService:
    def __init__(self, entries=None, raises=None):
        self.entries = entries if entries is not None else [ENTRY]
        self.raises = raises
        self.calls = []

    async def variants_in_ld(self, variant, *, window, r2_threshold, panel):
        self.calls.append(
            {"variant": variant, "window": window, "r2_threshold": r2_threshold, "panel": panel}
        )
        if self.raises:
            raise self.raises
        return self.entries


class _Response:
    def __init__(self, status, body):
        self.status_code, self.content = status, body

    def json(self):
        return json.loads(self.content)


def call(service, path, query=""):
    """Drive the router's ASGI app for one GET and collect the response."""
    api = FastAPI()
    api.include_router(ld_router.router, prefix="/api/v1")
    api.dependency_overrides[get_ld_service] = lambda: service

    messages = []
    delivered = []

    async def receive():
        if not delivered:
            delivered.append(True)
            return {"type": "http.request", "body": b"", "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message):
        messages.append(message)

    asyncio.run(
        api(
            {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.3"},
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": path,
                "raw_path": path.encode(),
                "query_string": query.encode(),
                "root_path": "",
                "headers": [],
                "client": ("testclient", 50000),
                "server": ("testserver", 80),
                "state": {},
            },
            receive,
            send,
        )
    )
    start = next(m for m in messages if m["type"] == "http.response.start")
    body = b"".join(
        bytes(m.get("body", b"")) for m in messages if m["type"] == "http.response.body"
    )
    return _Response(start["status"], body)


def test_a_query_variant_returns_the_upstream_entries_under_a_named_envelope():
    service = StubService()
    response = call(
        service, "/api/v1/ld/12:49048170:A:C", "window=250000&r2_threshold=0.2"
    )
    assert response.status_code == 200
    body = response.json()
    assert body["variant"] == "12:49048170:A:C"
    assert body["window"] == 250000
    assert body["r2_threshold"] == 0.2
    assert body["panel"] == "sisu42"
    assert body["ld"] == [ENTRY]
    assert service.calls == [
        {
            "variant": "12:49048170:A:C",
            "window": 250000,
            "r2_threshold": 0.2,
            "panel": "sisu42",
        }
    ]


def test_nothing_above_the_threshold_is_an_answer_and_not_an_error():
    response = call(StubService(entries=[]), "/api/v1/ld/12:49048170:A:C")
    assert response.status_code == 200
    assert response.json()["ld"] == []


@pytest.mark.parametrize(
    "variant",
    [
        "not-a-variant",
        "12:49048170:A",            # three fields
        "12:abc:A:C",               # non-numeric position
        "12:49048170:A:C extra",
        "$(curl evil)",
        "12:49048170:A:C&panel=x",  # a second query parameter smuggled through the path
    ],
)
def test_a_variant_that_is_not_chr_pos_ref_alt_is_refused_before_the_upstream(variant):
    service = StubService()
    response = call(service, f"/api/v1/ld/{variant}")
    assert response.status_code == 422
    assert service.calls == [], "the upstream was called with an unvalidated variant"


@pytest.mark.parametrize("variant", ["12:49048170:A:C", "chr12:49048170:A:C", "X:1:A:T"])
def test_the_spellings_the_suite_actually_uses_are_accepted(variant):
    assert call(StubService(), f"/api/v1/ld/{variant}").status_code == 200


def test_a_panel_that_is_not_a_name_is_refused_but_an_unknown_name_is_not():
    service = StubService()
    refused = call(service, "/api/v1/ld/12:49048170:A:C", "panel=a+b%26c%3Dd")
    assert refused.status_code == 422
    assert service.calls == []

    # membership belongs to the upstream: an unknown-but-well-formed name goes through, so
    # this service has no panel list to go stale
    assert call(service, "/api/v1/ld/12:49048170:A:C", "panel=sisu99").status_code == 200


@pytest.mark.parametrize("window", ["0", "-1", "11000001"])
def test_a_window_outside_the_bound_is_refused_rather_than_clamped(window):
    """Clamping returns fewer variants and reads as a sparse locus rather than as a limit."""
    service = StubService()
    response = call(service, "/api/v1/ld/12:49048170:A:C", f"window={window}")
    assert response.status_code == 422
    assert service.calls == []


@pytest.mark.parametrize("threshold", ["-0.1", "1.1"])
def test_an_r2_threshold_outside_zero_to_one_is_refused(threshold):
    response = call(StubService(), "/api/v1/ld/12:49048170:A:C", f"r2_threshold={threshold}")
    assert response.status_code == 422


def test_an_upstream_failure_is_502_and_not_the_callers_fault():
    response = call(
        StubService(raises=LDUpstreamError("the LD server could not be reached")),
        "/api/v1/ld/12:49048170:A:C",
    )
    assert response.status_code == 502
    assert "could not be reached" in response.json()["detail"]


# --------------------------------------------------------------------------- the service


class FakeResponse:
    def __init__(self, status=200, payload=None, text="", raises=None):
        self.status = status
        self._payload = payload
        self._text = text
        self._raises = raises

    async def __aenter__(self):
        if self._raises:
            raise self._raises
        return self

    async def __aexit__(self, *_exc):
        return False

    async def json(self, content_type=None):
        return self._payload

    async def text(self):
        return self._text


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params))
        return self.response


def service_with(response):
    service = LDService()
    session = FakeSession(response)
    service._ensure_upstream_session = lambda: session
    return service, session


def fetch(service, **over):
    kwargs = {"window": 1000, "r2_threshold": 0.1, "panel": "sisu42"}
    kwargs.update(over)
    return asyncio.run(service.variants_in_ld("12:49048170:A:C", **kwargs))


def test_the_service_trims_entries_to_the_documented_fields():
    """A field not listed is not forwarded, so an upstream that grows one does not start
    leaking it through a proxy nobody re-reviewed."""
    service, _session = service_with(
        FakeResponse(payload={"ld": [{**ENTRY, "internal_debug": "leak me"}]})
    )
    entries = fetch(service)
    assert entries == [ENTRY]
    assert set(entries[0]) == set(LD_ENTRY_FIELDS)


def test_the_service_passes_the_query_the_upstream_expects():
    service, session = service_with(FakeResponse(payload={"ld": []}))
    fetch(service, window=250000, r2_threshold=0.6)
    _url, params = session.calls[0]
    assert params == {
        "variant": "12:49048170:A:C",
        "window": "250000",
        "panel": "sisu42",
        "r2_thresh": "0.6",
    }


@pytest.mark.parametrize(
    "payload,expected",
    [({"ld": None}, []), ({}, []), ({"ld": [ENTRY, "not a dict"]}, [ENTRY])],
)
def test_the_service_tolerates_the_shapes_an_empty_answer_arrives_in(payload, expected):
    service, _session = service_with(FakeResponse(payload=payload))
    assert fetch(service) == expected


@pytest.mark.parametrize("payload", [["not", "an", "object"], {"ld": {"not": "a list"}}])
def test_a_body_that_is_not_the_contract_raises_rather_than_returning_nothing(payload):
    """Silently returning [] for a malformed body is indistinguishable from 'no LD here',
    which is exactly the wrong answer to hand a plot."""
    service, _session = service_with(FakeResponse(payload=payload))
    with pytest.raises(LDUpstreamError):
        fetch(service)


def test_the_upstreams_body_is_not_forwarded_to_the_caller():
    service, _session = service_with(
        FakeResponse(status=500, text="upstream stack trace with internal hostnames")
    )
    with pytest.raises(LDUpstreamError) as caught:
        fetch(service)
    assert caught.value.status == 500
    assert "stack trace" not in str(caught.value)
    assert "HTTP 500" in str(caught.value)


@pytest.mark.parametrize("raised", [aiohttp.ClientError("dns"), TimeoutError()])
def test_unreachable_and_timed_out_both_surface_as_upstream_errors(raised):
    service, _session = service_with(FakeResponse(raises=raised))
    with pytest.raises(LDUpstreamError):
        fetch(service)


def test_cleanup_never_opens_a_session():
    """Shutdown runs cleanup on every registered service, including ones that never served."""
    service = LDService()
    asyncio.run(service.cleanup())
    assert service._upstream_session is None
