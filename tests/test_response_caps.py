"""Unit tests for the per-credential response cap on results-api.

Design of record: docs/code-execution-security.md section 4 in genetics-results-suite, and
the rule as it is actually implemented is documented in app/core/limits.py.

The properties under test:
  * a sandbox execution gets the tight byte cap, and gets a **429** rather than a truncated
    body when it exceeds it — and the producer behind a stream is torn down, not left running
    with its chunks discarded;
  * there is no row cap here, deliberately: see app/core/limits.py;
  * a verified non-sandbox principal gets the operator-configured behaviour — and that means
    *any* verified one (shared secret, Google id_token, per-user API token), not just the
    shared secret, because auth-gateway routes real users straight here with their own token;
  * an ``@is_public`` route keeps serving unlimited responses to the browser, which reaches it
    with no credential at all — but a sandbox token presented *to a public route* is still
    capped, so nothing is gained by shedding the credential;
  * ``REQUIRE_AUTH=false`` (local dev) does not silently inherit the tight caps, and a sandbox
    token is capped there anyway so the caps stay testable locally.
"""

import asyncio
import json
import time
from urllib.parse import urlencode

import jwt
import pytest
from fastapi import Depends, FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, StreamingResponse

import app.config.common as config
from app.core import limits
from app.dependencies import auth_required, is_public
from app.middleware import SandboxResponseCapMiddleware, setup_middleware

INTERNAL_SECRET = "test-internal-secret"
SIGNING_KEY = "test-sandbox-signing-key-that-is-32-bytes+"

# every chunk the /stream endpoint's generator produces, so a test can tell "the caller stopped
# receiving" apart from "the service stopped producing"
_CHUNKS_PRODUCED: list[int] = []


def _mint(audience="results-api", **over):
    iat = int(time.time())
    claims = {
        "iss": "chat-backend",
        "aud": audience,
        "sub": "user@finngen.fi",
        "sid": "session-abc",
        "jti": "exec-123",
        "iat": iat,
        "exp": iat + 300,
        "scope": "query:views",
    }
    claims.update(over)
    return jwt.encode(claims, SIGNING_KEY, algorithm="HS256")


@pytest.fixture(autouse=True)
def sandbox_config(monkeypatch):
    monkeypatch.setattr(config, "internal_api_secret", INTERNAL_SECRET)
    monkeypatch.setattr(config, "sandbox_token_signing_key", SIGNING_KEY)
    monkeypatch.setattr(config, "allowed_email_domains", {"finngen.fi"})
    monkeypatch.setattr(config, "allowed_emails", set())
    monkeypatch.setattr(config, "require_auth", True)


@pytest.fixture(autouse=True)
def clean_chunk_log():
    _CHUNKS_PRODUCED.clear()
    yield
    _CHUNKS_PRODUCED.clear()


class _Response:
    """The ASGI app is driven directly: httpx (and so TestClient) is not installed here."""

    def __init__(self, status: int, body: bytes, headers: list, messages: list):
        self.status_code, self.content, self.headers = status, body, headers
        self.messages = messages

    def json(self):
        return json.loads(self.content)


def _call(api, path, query="", headers=(), spec_version="2.3") -> _Response:
    messages = []
    received = []

    async def receive():
        if not received:
            received.append(True)
            return {"type": "http.request", "body": b"", "more_body": False}
        # a real client does not disconnect here; blocking forever is what lets
        # StreamingResponse's listen_for_disconnect task behave as it does in production
        await asyncio.Event().wait()

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": spec_version},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": query.encode(),
        "root_path": "",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "state": {},
    }
    asyncio.run(api(scope, receive, send))

    start = next(m for m in messages if m["type"] == "http.response.start")
    body = b"".join(
        bytes(m.get("body", b"")) for m in messages if m["type"] == "http.response.body"
    )
    return _Response(start["status"], body, start["headers"], messages)


@pytest.fixture
def client():
    api = FastAPI(dependencies=[Depends(auth_required)])
    api.add_middleware(SandboxResponseCapMiddleware)

    @api.get("/rows")
    async def rows(n: int = 10):
        return JSONResponse([{"i": i} for i in range(n)])

    @api.get("/blob")
    async def blob(n: int = 10):
        return JSONResponse({"payload": "x" * n})

    @api.get("/stream")
    async def stream(chunks: int = 10, size: int = 1000):
        async def produce():
            for i in range(chunks):
                _CHUNKS_PRODUCED.append(i)
                yield b"x" * size

        # TSV is the default format of every bulk range endpoint, and the shape nearly every
        # real endpoint here uses
        return StreamingResponse(produce(), media_type="text/tab-separated-values")

    @api.get("/public/rows")
    @is_public
    async def public_rows(n: int = 10):
        return JSONResponse([{"i": i} for i in range(n)])

    @api.get("/boom")
    async def boom():
        return JSONResponse({"payload": "x" * 50_000}, status_code=404)

    return api


def _get(client, path, token=None, spec_version="2.3", **params):
    headers = [("Authorization", f"Bearer {token}")] if token else []
    return _call(client, path, urlencode(params), headers, spec_version=spec_version)


# --- a sandbox execution gets the tight cap -------------------------------------------------


def test_sandbox_over_the_byte_cap_gets_429_not_a_truncated_body(client, monkeypatch):
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 1000)
    resp = _get(client, "/blob", _mint(), n=5000)

    assert resp.status_code == 429
    assert "byte limit" in resp.json()["detail"]


def test_sandbox_under_the_byte_cap_is_served_in_full(client, monkeypatch):
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 100_000)
    resp = _get(client, "/rows", _mint(), n=1000)

    assert resp.status_code == 200
    assert len(resp.json()) == 1000


def test_there_is_no_row_cap_only_a_byte_cap(client, monkeypatch):
    """Removed deliberately: counting rows meant json.loads over the whole buffered body on the
    event loop — a memory amplifier only a sandbox caller could trigger — and it never bound
    TSV, the default format of the bulk endpoints it was written for. See app/core/limits.py."""
    assert not hasattr(limits.sandbox_caps(), "max_rows")
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 16 * 1024**2)
    resp = _get(client, "/rows", _mint(), n=30_000)

    assert resp.status_code == 200
    assert len(resp.json()) == 30_000


def test_an_error_response_keeps_its_status_even_over_the_cap(client, monkeypatch):
    """The cap must not rewrite a 404 into a 429 and lose the real answer."""
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 1000)
    assert _get(client, "/boom", _mint()).status_code == 404


def test_the_default_is_the_documented_number():
    assert limits.sandbox_caps().max_response_bytes == 16 * 1024**2


# --- streaming: the shape nearly every real endpoint uses -----------------------------------


def test_a_relaxed_stream_is_passed_through_chunk_by_chunk(client):
    """The relaxed path must not buffer: a range download has to start arriving immediately."""
    resp = _get(client, "/stream", INTERNAL_SECRET, chunks=5, size=10)

    assert resp.status_code == 200
    assert resp.content == b"x" * 50
    assert _CHUNKS_PRODUCED == list(range(5))
    body_messages = [m for m in resp.messages if m["type"] == "http.response.body"]
    assert len(body_messages) == 6, "5 chunks plus the terminating empty body, uncoalesced"


def test_a_capped_stream_under_the_cap_is_served_in_full(client, monkeypatch):
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 100_000)
    resp = _get(client, "/stream", _mint(), chunks=5, size=10)

    assert resp.status_code == 200
    assert resp.content == b"x" * 50
    assert _CHUNKS_PRODUCED == list(range(5))
    body_messages = [m for m in resp.messages if m["type"] == "http.response.body"]
    assert len(body_messages) == 1, "buffered, so it goes out as one body message"


@pytest.mark.parametrize("spec_version", ["2.3", "2.4"])
def test_a_capped_stream_tears_the_producer_down_rather_than_dropping_chunks(
    client, monkeypatch, spec_version
):
    """Dropping later messages bounds what the caller *receives* but not what this service
    *spends*: on the real endpoints the generator is GCS range reads plus the tabix filter
    pool, and it used to run to completion with every chunk discarded. Both ASGI spec versions
    are covered because StreamingResponse takes a different path on each (uvicorn sends 2.3)."""
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 10_000)
    resp = _get(client, "/stream", _mint(), spec_version=spec_version, chunks=100, size=1000)

    assert resp.status_code == 429, "the client still gets its answer"
    # 11 chunks are what it takes to cross a 10 000-byte cap at 1 000 bytes each
    assert _CHUNKS_PRODUCED == list(range(11)), "the generator must stop at the rejection"


# --- verified non-sandbox principals are relaxed --------------------------------------------


def test_shared_secret_is_not_capped(client, monkeypatch):
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 100)
    resp = _get(client, "/rows", INTERNAL_SECRET, n=5000)

    assert resp.status_code == 200
    assert len(resp.json()) == 5000


def test_a_verified_per_user_api_token_is_not_capped(client, monkeypatch):
    """auth-gateway's @api_bearer location sends these straight here with no shared secret; an
    hmac-only relax rule would put verified humans on the sandbox caps."""
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 100)
    monkeypatch.setattr(
        "app.core.auth._validate_user_api_token", lambda token: "user@finngen.fi"
    )
    resp = _get(client, "/rows", "opaque-user-token", n=5000)

    assert resp.status_code == 200
    assert len(resp.json()) == 5000


def test_a_verified_google_id_token_is_not_capped(client, monkeypatch):
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 100)
    monkeypatch.setattr(
        "google.oauth2.id_token.verify_oauth2_token",
        lambda token, request, *a, **kw: {
            "email": "user@finngen.fi",
            "email_verified": True,
        },
    )
    google_shaped = jwt.encode({"email": "user@finngen.fi"}, "irrelevant", algorithm="HS384")
    resp = _get(client, "/rows", google_shaped, n=5000)

    assert resp.status_code == 200
    assert len(resp.json()) == 5000


# --- public routes and dev ------------------------------------------------------------------


def test_a_public_route_with_no_credential_is_not_capped(client, monkeypatch):
    """Pinned deliberately. What makes this safe is not the cap but that every public route
    bounds its own response for every caller — including POST /rsid/variants, which is where
    the invariant used to break. See app/core/limits.py."""
    # the wide anonymous surface, so that a credential-less request reaches the handler at all.
    # It is not the default any more (genetics-results-suite-rhh); this test is about what the
    # CAP does once the request is in, not about who may get in.
    monkeypatch.setattr(config, "anonymous_surface_minimal", False)
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 100)
    resp = _get(client, "/public/rows", n=5000)

    assert resp.status_code == 200
    assert len(resp.json()) == 5000


def test_a_public_route_with_a_sandbox_token_is_still_capped(client, monkeypatch):
    """The other half of the rule above: the sandbox principal is resolved before the
    public-route short circuit, so a script cannot pick a public route to escape the cap."""
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 100)
    assert _get(client, "/public/rows", _mint(), n=5000).status_code == 429


def test_require_auth_false_relaxes_everything_except_a_sandbox_token(client, monkeypatch):
    monkeypatch.setattr(config, "require_auth", False)
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 100)

    assert _get(client, "/rows", n=5000).status_code == 200
    assert _get(client, "/rows", _mint(), n=5000).status_code == 429


def test_an_invalid_sandbox_token_is_401_on_a_public_route(client):
    """It must not fall through to the uncapped path: an unverified credential is not a
    missing one."""
    forged = jwt.encode({"aud": "results-api"}, "wrong-key", algorithm="HS256")
    assert _get(client, "/public/rows", forged).status_code == 401


# --- wiring ---------------------------------------------------------------------------------


def test_the_cap_middleware_is_registered_inside_gzip(monkeypatch):
    """It must measure the payload the caller decodes, not its compressed size."""
    monkeypatch.setattr(config, "usage_logging_enabled", False)
    api = FastAPI()
    setup_middleware(api)

    classes = [m.cls for m in api.user_middleware]
    # user_middleware is outermost-first, so "inside gzip" means "later in this list"
    assert classes.index(SandboxResponseCapMiddleware) > classes.index(GZipMiddleware)


def test_a_relaxed_response_is_not_buffered_or_rewritten(client):
    """The no-op path must leave the response byte-identical, headers included."""
    resp = _get(client, "/rows", INTERNAL_SECRET, n=3)
    assert resp.content == b'[{"i":0},{"i":1},{"i":2}]'
    assert (b"content-length", b"25") in [(k.lower(), v) for k, v in resp.headers]
