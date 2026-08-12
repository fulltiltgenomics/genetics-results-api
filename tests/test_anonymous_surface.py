"""The anonymous surface — routes servable with no principal resolved at all.

genetics-results-suite-0lf: the four per-execution counters in `app/core/sandbox_budget.py` are
admitted from the `Authorization` header, so a request that carries none is counted against
nothing. The sandbox's NetworkPolicy egress reaches `results-api:4000` directly, bypassing
auth-gateway, so any route that answers 200 anonymously is a per-execution bound a script can
shed by simply not sending the header.

These tests are the enforcement half. The control lives in `app.dependencies.is_public_endpoint`;
without a test that reads the LIVE route table, a future `@is_public` decorator reopens the hole
silently and nothing in the repo notices — `scripts/test-network-policies.py` cannot see route
decorators, and the network layer cannot take results-api:4000 away from the sandbox because the
SDK genuinely needs it.

**What this closes is the no-credential path only.** A caller presenting `INTERNAL_API_SECRET`
still reaches every handler with no accounting whatsoever — `_sandbox_principal` accepts an HS256
sandbox token and nothing else, so `admit` is not called for it. The sandbox is handed that secret
today and the SDK attaches it to every request, so the residual hole closes only when
genetics-results-suite-4h6.7 stops giving the sandbox the secret and genetics-results-suite-4h6.14
makes the SDK send the per-execution token instead.
`test_an_internal_secret_caller_is_served_but_not_accounted` pins that residue.
"""

import asyncio

import pytest
from fastapi import Request

import app.config.common as config
from app.core import sandbox_budget
from app.dependencies import (
    ALWAYS_ANONYMOUS_PATHS,
    is_public_endpoint,
    public_route_paths,
)
from app.server import app as fastapi_app


def _request_for(route) -> Request:
    return Request({"type": "http", "method": "GET", "headers": [], "route": route})


def _drive(path: str, headers=()) -> tuple[int, bytes]:
    """Run one request through the real ASGI app. httpx (and so TestClient) is not installed."""
    messages: list[dict] = []
    delivered = []

    async def receive():
        if not delivered:
            delivered.append(True)
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Event().wait()

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "state": {},
    }
    asyncio.run(fastapi_app(scope, receive, send))

    start = next(m for m in messages if m["type"] == "http.response.start")
    body = b"".join(
        bytes(m.get("body", b"")) for m in messages if m["type"] == "http.response.body"
    )
    return start["status"], body


def _public_routes():
    return [
        route
        for route in fastapi_app.routes
        if getattr(getattr(route, "endpoint", None), "is_public", False)
    ]


# The set as it stands. This is a tripwire, not a specification: if a route is added to or
# removed from it, decide deliberately whether it belongs in ALWAYS_ANONYMOUS_PATHS and update
# `docs/code-execution-security.md` section 4 in genetics-results-suite, then update this list.
EXPECTED_PUBLIC_PATHS = {
    "/api/v1",
    "/healthz",
    "/api/v1/auth",
    "/api/v1/variant_sets",
    "/api/v1/variant_sets/{name}",
    "/api/v1/rsid/variants",
}


def test_the_public_route_set_is_what_the_docs_claim():
    assert public_route_paths() == EXPECTED_PUBLIC_PATHS


def test_every_always_anonymous_path_is_a_real_public_route():
    # a typo here would silently make the path require auth rather than exempt it, which is the
    # fail-CLOSED direction on /healthz: the kubelet would start failing the liveness probe.
    assert ALWAYS_ANONYMOUS_PATHS <= public_route_paths()


def test_without_the_sandbox_every_public_route_stays_anonymous(monkeypatch):
    monkeypatch.setattr(config, "sandbox_enabled", False)
    for route in _public_routes():
        assert is_public_endpoint(_request_for(route)), route.path


def test_with_the_sandbox_the_anonymous_surface_is_exactly_healthz(monkeypatch):
    # pinned against the literal path, NOT against ALWAYS_ANONYMOUS_PATHS: comparing the computed
    # surface to the constant that produced it is tautological and passes unchanged for any
    # widening of that constant, which is the direction that matters here.
    assert set(ALWAYS_ANONYMOUS_PATHS) == {"/healthz"}
    monkeypatch.setattr(config, "sandbox_enabled", True)
    anonymous = {
        route.path for route in _public_routes() if is_public_endpoint(_request_for(route))
    }
    assert anonymous == {"/healthz"}


def test_an_internal_secret_caller_is_served_but_not_accounted(monkeypatch):
    """The residual hole, pinned as it actually behaves rather than as the docs once claimed.

    Shrinking the anonymous surface closes the NO-CREDENTIAL path. It does not close the
    INTERNAL_API_SECRET path: that secret satisfies `is_internal_caller`, so `get_verified_user`
    resolves `mcp-tool` and the request enters the handler, while `_sandbox_principal` accepts an
    HS256 sandbox token only — so `admit` is never called and the per-execution counter map stays
    empty. The sandbox is handed that secret today and the SDK attaches it to every request, so
    on the day `SANDBOX_ENABLED` flips a script can shed all four counters by sending the internal
    secret instead of sending nothing.

    EXPECTED TO CHANGE when genetics-results-suite-4h6.7 (stop giving the sandbox the secret) and
    genetics-results-suite-4h6.14 (make the SDK send the per-execution token) land: this test
    should then fail, and that failure is the signal that the hole is closed, not a regression.
    """
    monkeypatch.setattr(config, "sandbox_enabled", True)
    monkeypatch.setattr(config, "require_auth", True)
    monkeypatch.setattr(config, "internal_api_secret", "test-internal-secret")
    sandbox_budget.reset()

    anonymous_status, _ = _drive("/api/v1")
    assert anonymous_status == 401, "the no-credential path is the half that is closed"

    status, _ = _drive("/api/v1", headers=[("authorization", "Bearer test-internal-secret")])
    assert status == 200, "an internal-secret caller is still served on a formerly public route"
    assert sandbox_budget._executions == {}, (
        "served with no per-execution accounting: admit() was never called"
    )


def test_the_rsid_route_the_sdk_uses_is_not_anonymous_with_the_sandbox(monkeypatch):
    # named separately because it is the one public route the SDK actually calls
    # (genetics_mcp_server.sdk.client.search(rsids=...) -> ToolExecutor.lookup_variants_by_rsid
    # -> GET {results-api}/v1/rsid/variants). Removing results-api:4000 from the sandbox's egress
    # allow-list would have broken it, which is why option (b) of the bead was rejected.
    monkeypatch.setattr(config, "sandbox_enabled", True)
    rsid = [r for r in _public_routes() if r.path == "/api/v1/rsid/variants"]
    assert rsid, "the SDK's rsid lookup route disappeared; re-derive the SDK's endpoint list"
    for route in rsid:
        assert not is_public_endpoint(_request_for(route))


@pytest.mark.parametrize("sandbox_enabled", [False, True])
def test_a_non_public_route_is_never_anonymous(monkeypatch, sandbox_enabled):
    monkeypatch.setattr(config, "sandbox_enabled", sandbox_enabled)
    non_public = [
        route
        for route in fastapi_app.routes
        if getattr(route, "endpoint", None) is not None
        and not getattr(route.endpoint, "is_public", False)
    ]
    assert non_public
    for route in non_public:
        assert not is_public_endpoint(_request_for(route))


def test_a_route_with_no_route_object_is_not_anonymous(monkeypatch):
    # an unmatched path 404s out of the router with no `route` in scope
    monkeypatch.setattr(config, "sandbox_enabled", True)
    assert not is_public_endpoint(Request({"type": "http", "method": "GET", "headers": []}))
