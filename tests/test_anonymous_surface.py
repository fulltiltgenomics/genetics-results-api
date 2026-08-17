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
sandbox token and nothing else, so `admit` is not called for it. What changed with
genetics-results-suite-4h6.44 is who can reach that path: the SDK no longer attaches the shared
secret at all. It reads the supervisor's per-execution token file and sends the token bound to
this destination (`aud: results-api`), which is the credential `admit` is keyed on.
`test_the_sdk_s_per_execution_token_is_accounted` measures that a request carrying it lands in the
execution map, and `test_the_internal_secret_path_survives_but_the_sdk_no_longer_takes_it` keeps
the residue pinned as what it now is — a property of this service, not a route out of the sandbox.
"""

import asyncio
import importlib
import logging
import os
import time

import jwt
import pytest
from fastapi import Request

import app.config.common as config
from app.core import sandbox_budget, sandbox_token
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


def _anonymous_surface() -> set[str]:
    return {route.path for route in _public_routes() if is_public_endpoint(_request_for(route))}


def test_the_minimal_surface_is_the_default(monkeypatch):
    # the shipped default, read from the environment the way the process reads it. An empty
    # environment must produce the narrow surface, so forgetting the variable is safe.
    monkeypatch.delenv("ANONYMOUS_SURFACE_MINIMAL", raising=False)
    assert config._parse_anonymous_surface_minimal() is True
    # and only an explicit false-y value turns it off; a typo does not
    for value, expected in (
        ("false", False),
        ("FALSE", False),
        ("0", False),
        ("no", False),
        ("true", True),
        ("flase", True),
        ("", True),
    ):
        monkeypatch.setenv("ANONYMOUS_SURFACE_MINIMAL", value)
        assert config._parse_anonymous_surface_minimal() is expected, value


def test_with_the_surface_widened_every_public_route_is_anonymous(monkeypatch):
    monkeypatch.setattr(config, "sandbox_enabled", False)
    monkeypatch.setattr(config, "anonymous_surface_minimal", False)
    for route in _public_routes():
        assert is_public_endpoint(_request_for(route)), route.path


def test_the_minimal_anonymous_surface_is_exactly_healthz(monkeypatch):
    # pinned against the literal path, NOT against ALWAYS_ANONYMOUS_PATHS: comparing the computed
    # surface to the constant that produced it is tautological and passes unchanged for any
    # widening of that constant, which is the direction that matters here.
    assert set(ALWAYS_ANONYMOUS_PATHS) == {"/healthz"}
    monkeypatch.setattr(config, "sandbox_enabled", False)
    monkeypatch.setattr(config, "anonymous_surface_minimal", True)
    assert _anonymous_surface() == {"/healthz"}


def test_the_sandbox_forces_the_minimal_surface_even_if_the_flag_is_off(monkeypatch):
    """genetics-results-suite-rhh: the two levers are independent in ONE direction only.

    `ANONYMOUS_SURFACE_MINIMAL=false` is an escape hatch for the no-sandbox deployment. It must
    not be a way to hand a running sandbox six routes it can call with no header, so
    `SANDBOX_ENABLED` overrides it.
    """
    monkeypatch.setattr(config, "sandbox_enabled", True)
    monkeypatch.setattr(config, "anonymous_surface_minimal", False)
    assert _anonymous_surface() == {"/healthz"}


def test_disabling_the_sandbox_does_not_re_open_the_surface(monkeypatch):
    """The defect this bead is: `SANDBOX_ENABLED=false` is an INCIDENT action, and it used to be
    a security widening as well. With the sandbox off and the flag at its default, the surface
    must stay minimal — otherwise killing the sandbox under pressure re-opens six routes."""
    monkeypatch.setattr(config, "sandbox_enabled", True)
    monkeypatch.setattr(config, "anonymous_surface_minimal", config._parse_anonymous_surface_minimal())
    assert _anonymous_surface() == {"/healthz"}
    monkeypatch.setattr(config, "sandbox_enabled", False)
    assert _anonymous_surface() == {"/healthz"}, (
        "disabling the sandbox re-opened the anonymous surface"
    )


def _sandbox_bearer(key: str, jti: str = "exec-4h6-44", ttl: int = 300) -> str:
    """A token shaped exactly as chat-backend mints it (genetics-mcp-server sandbox_token.py).

    Built here rather than imported: this repo is the VERIFIER, and a test that reuses the
    minter's own helper cannot catch the two drifting apart. That argument only holds for
    values written as LITERALS — `iss`, `aud` and `alg` used to be read off the verifier's
    own constants, which made those three tautological and left drift in exactly the fields
    a validator rejects on. They are spelled out here, as `test_a_db_api_token_is_refused_here`
    already spells out its crossed audience.
    """
    now = int(time.time())
    return jwt.encode(
        {
            "iss": "chat-backend",
            "aud": "results-api",
            "sub": "someone@example.org",
            "sid": "session-1",
            "jti": jti,
            "iat": now,
            "exp": now + ttl,
            "scope": "genetics:read",
        },
        key,
        algorithm="HS256",
    )


def test_the_sdk_s_per_execution_token_is_accounted(monkeypatch):
    """THE acceptance for genetics-results-suite-4h6.44, and it is a measurement of the counter
    map rather than of a status code.

    A 200 proves nothing here: an INTERNAL_API_SECRET caller is served 200 too, as `mcp-tool`,
    with `_executions == {}` — that shape of check is exactly what let genetics-results-suite-0lf
    through. `admit` runs in `app/middleware.py` off `_sandbox_principal`, so the only credential
    that reaches it is an HS256 token whose `aud` is this service, which is what the SDK now
    attaches per destination (genetics-mcp-server tools/executor.py `_SandboxTokenAuth`).
    """
    key = "test-signing-key-0123456789abcdef"
    monkeypatch.setattr(config, "sandbox_enabled", True)
    monkeypatch.setattr(config, "require_auth", True)
    monkeypatch.setattr(config, "sandbox_token_signing_key", key)
    sandbox_budget.reset()

    status, _ = _drive(
        "/api/v1", headers=[("authorization", f"Bearer {_sandbox_bearer(key)}")]
    )

    assert status == 200
    assert "exec-4h6-44" in sandbox_budget._executions, (
        "the SDK's request did not appear in the per-execution counter map: admit() never ran, "
        "so every quota control above it is inert"
    )
    entry = sandbox_budget._executions["exec-4h6-44"]
    assert entry.requests == 1
    assert entry.in_flight == 0, "the middleware's finally-release did not run"
    sandbox_budget.reset()


def test_a_db_api_token_is_refused_here(monkeypatch):
    """Audience binding is the reason the SDK picks by destination rather than holding one
    bearer. PyJWT treats a list `aud` as membership, so this is asserted against the validator
    and not against the minter."""
    key = "test-signing-key-0123456789abcdef"
    monkeypatch.setattr(config, "sandbox_enabled", True)
    monkeypatch.setattr(config, "require_auth", True)
    monkeypatch.setattr(config, "sandbox_token_signing_key", key)
    sandbox_budget.reset()

    now = int(time.time())
    crossed = jwt.encode(
        {
            "iss": sandbox_token.ISSUER,
            "aud": "db-api",
            "sub": "someone@example.org",
            "sid": "session-1",
            "jti": "exec-crossed",
            "iat": now,
            "exp": now + 300,
            "scope": "genetics:read",
        },
        key,
        algorithm=sandbox_token.ALGORITHM,
    )
    status, _ = _drive("/api/v1", headers=[("authorization", f"Bearer {crossed}")])

    assert status == 401
    assert sandbox_budget._executions == {}
    sandbox_budget.reset()


def test_the_internal_secret_path_survives_but_the_sdk_no_longer_takes_it(monkeypatch):
    """The residue, pinned as what it now is.

    This service still serves an INTERNAL_API_SECRET caller with no accounting: the secret
    satisfies `is_internal_caller`, `get_verified_user` resolves `mcp-tool`, and
    `_sandbox_principal` accepts an HS256 sandbox token only, so `admit` is never called. That
    remains true and is deliberately not "fixed" here — the browser's BFF and the MCP service are
    legitimate secret-bearing callers and are not per-execution anything.

    What changed (genetics-results-suite-4h6.44) is that the sandbox is no longer one of them.
    The SDK reads the supervisor's per-execution token file and attaches the audience-bound token
    instead of the shared secret; genetics-mcp-server
    tests/test_sandbox_sdk_credential.py::test_the_internal_secret_is_not_attached_when_a_token_file_exists
    is the assertion on that side, and it is the one that fails if this route back out reopens.
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


def _reload_config_with(value: str | None):
    """Re-execute app/config/common.py with ANONYMOUS_SURFACE_MINIMAL set (or not) as given."""
    saved = os.environ.get("ANONYMOUS_SURFACE_MINIMAL")
    if value is None:
        os.environ.pop("ANONYMOUS_SURFACE_MINIMAL", None)
    else:
        os.environ["ANONYMOUS_SURFACE_MINIMAL"] = value
    try:
        importlib.reload(config)
        return config.anonymous_surface_minimal
    finally:
        if saved is None:
            os.environ.pop("ANONYMOUS_SURFACE_MINIMAL", None)
        else:
            os.environ["ANONYMOUS_SURFACE_MINIMAL"] = saved
        importlib.reload(config)


def test_the_shipped_default_is_the_attribute_the_code_actually_reads():
    """Pins the module ATTRIBUTE, not the parse function.

    `test_the_minimal_surface_is_the_default` exercises `_parse_anonymous_surface_minimal()` in
    isolation and every other test here monkeypatches `config.anonymous_surface_minimal`
    explicitly, so nothing tied the function to the attribute `is_public_endpoint` reads:
    mutating the module line to `anonymous_surface_minimal = False` left the whole offline lane
    green. This re-executes the config module with the variable absent — the shipped
    deployment's state — and asserts both the attribute and its effect on a real public route.
    """
    assert _reload_config_with(None) is True

    # and it is load-bearing: with the module in that state, a data-path public route is not
    # anonymous. sandbox_enabled is forced off so the assertion can only be satisfied by the
    # attribute above.
    saved = os.environ.get("ANONYMOUS_SURFACE_MINIMAL")
    os.environ.pop("ANONYMOUS_SURFACE_MINIMAL", None)
    try:
        importlib.reload(config)
        config.sandbox_enabled = False
        data_path = [r for r in _public_routes() if r.path not in ALWAYS_ANONYMOUS_PATHS]
        assert data_path
        for route in data_path:
            assert not is_public_endpoint(_request_for(route)), route.path
    finally:
        if saved is not None:
            os.environ["ANONYMOUS_SURFACE_MINIMAL"] = saved
        importlib.reload(config)


@pytest.mark.parametrize(
    "value,expected",
    [
        # the ordinary falsey spellings an operator reaches for. `off` is the one that matters:
        # the manifest calls this a break-glass, and `ANONYMOUS_SURFACE_MINIMAL=off` used to
        # leave the surface narrow while looking like it had been widened.
        ("off", False),
        ("OFF", False),
        ("disabled", False),
        ("n", False),
        ("F", False),
        ("0", False),
        ("false", False),
        ("no", False),
        # whitespace is what the .strip() exists for
        (" false ", False),
        ("0 ", False),
        ("\ttrue\n", True),
        # truthy and unrecognised both keep the fail-safe default
        ("on", True),
        ("enabled", True),
        ("flase", True),
        ("None", True),
        ("", True),
    ],
)
def test_the_break_glass_accepts_the_spellings_an_operator_types(monkeypatch, value, expected):
    monkeypatch.setenv("ANONYMOUS_SURFACE_MINIMAL", value)
    assert config._parse_anonymous_surface_minimal() is expected, value


def test_an_unrecognised_value_is_logged_rather_than_silently_assumed(monkeypatch, caplog):
    monkeypatch.setenv("ANONYMOUS_SURFACE_MINIMAL", "flase")
    with caplog.at_level(logging.WARNING, logger="app.config.common"):
        assert config._parse_anonymous_surface_minimal() is True
    assert any(
        "flase" in r.getMessage() and "ANONYMOUS_SURFACE_MINIMAL" in r.getMessage()
        for r in caplog.records
    ), "an unrecognised value must name itself and what was assumed"
