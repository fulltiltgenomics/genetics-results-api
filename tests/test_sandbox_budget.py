"""Unit tests for the per-execution (`jti`) limits on results-api.

Design of record: docs/code-execution-security.md section 4 in genetics-results-suite; the rule
as implemented is documented in app/core/sandbox_budget.py.

The response cap in tests/test_response_caps.py bounds **one** response. These bound the
execution: how many bytes it may be sent in total, how many requests it may issue, and how many
it may have in flight at once. The properties under test:

The module implements **four** counters — aggregate response bytes, request count, concurrency
per execution and concurrency pod-wide — plus a bound on the counter map itself, and emits
**five** rejection codes. The properties under test:

  * the aggregate byte budget is charged from what was actually sent, and answers 429 once
    spent — including partway through a loop that is individually inside the per-response cap;
  * every status is charged and capped, not only 2xx: an error body is caller-controlled;
  * the request-count limit answers 429 on its own, before any handler work;
  * concurrency is *rejected*, not queued, per execution and per pod;
  * a slot is released by the **middleware**, after the last byte and for requests that never
    matched a route, so nothing leaks;
  * the counter map is bounded, and its cleanup can never evict a live execution — an evicted
    counter is a reset budget, which is the fail-open direction;
  * a non-sandbox request is untouched by any of them, even with every limit set to 1.

Not covered here, and deliberately: a request that presents **no** `Authorization` header is
never admitted at all, so none of these limits applies to it. See the limitations section of
`app/core/sandbox_budget.py`.
"""

import asyncio
import json
import time
from urllib.parse import urlencode

import jwt
import pytest
from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse, StreamingResponse

import app.config.common as config
from app.core import limits, sandbox_budget, sandbox_token
from app.dependencies import auth_required
from app.middleware import SandboxResponseCapMiddleware

INTERNAL_SECRET = "test-internal-secret"
SIGNING_KEY = "test-sandbox-signing-key-that-is-32-bytes+"

_HANDLER_CALLS: list[str] = []
_IN_FLIGHT_DURING_STREAM: list[int] = []


def _mint(jti="exec-123", ttl=300, **over):
    iat = int(time.time())
    claims = {
        "iss": "chat-backend",
        "aud": "results-api",
        "sub": "user@finngen.fi",
        "sid": "session-abc",
        "jti": jti,
        "iat": iat,
        "exp": iat + ttl,
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
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 16 * 1024**2)


@pytest.fixture(autouse=True)
def clean_state():
    sandbox_budget.reset()
    _HANDLER_CALLS.clear()
    _IN_FLIGHT_DURING_STREAM.clear()
    yield
    sandbox_budget.reset()
    _HANDLER_CALLS.clear()
    _IN_FLIGHT_DURING_STREAM.clear()


# --- harness --------------------------------------------------------------------------------


class _Response:
    """The ASGI app is driven directly: httpx (and so TestClient) is not installed here."""

    def __init__(self, status: int, body: bytes):
        self.status_code, self.content = status, body

    def json(self):
        return json.loads(self.content)


def _scope(path: str, query: str, headers):
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
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


async def _acall(api, path, query="", headers=()) -> _Response:
    messages = []
    received = []

    async def receive():
        if not received:
            received.append(True)
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Event().wait()

    async def send(message):
        messages.append(message)

    await api(_scope(path, query, headers), receive, send)
    start = next(m for m in messages if m["type"] == "http.response.start")
    body = b"".join(
        bytes(m.get("body", b"")) for m in messages if m["type"] == "http.response.body"
    )
    return _Response(start["status"], body)


@pytest.fixture
def client():
    api = FastAPI(dependencies=[Depends(auth_required)])
    api.add_middleware(SandboxResponseCapMiddleware)

    gate = asyncio.Event()
    entered = asyncio.Event()

    @api.get("/blob")
    async def blob(n: int = 10):
        _HANDLER_CALLS.append("blob")
        return JSONResponse({"payload": "x" * n})

    @api.get("/slow")
    async def slow():
        _HANDLER_CALLS.append("slow")
        entered.set()
        await gate.wait()
        return JSONResponse({"ok": True})

    @api.get("/boom")
    async def boom(n: int = 10):
        _HANDLER_CALLS.append("boom")
        return JSONResponse({"payload": "x" * n}, status_code=404)

    @api.get("/stream")
    async def stream(chunks: int = 5, size: int = 100, observe: str = ""):
        async def produce():
            for _ in range(chunks):
                # sampled from inside the response body, i.e. after the endpoint function has
                # returned and after every dependency's teardown has run
                if observe:
                    entry = sandbox_budget.snapshot(observe)
                    _IN_FLIGHT_DURING_STREAM.append(-1 if entry is None else entry.in_flight)
                yield b"x" * size

        return StreamingResponse(produce(), media_type="text/tab-separated-values")

    api.state.gate, api.state.entered = gate, entered
    return api


def _get(client, path, token=None, **params):
    headers = [("Authorization", f"Bearer {token}")] if token else []
    return asyncio.run(_acall(client, path, urlencode(params), headers))


# --- 1. the aggregate response-byte budget --------------------------------------------------


def test_the_aggregate_budget_is_charged_from_what_was_actually_sent(client, monkeypatch):
    """The charge must agree with SandboxResponseCapMiddleware's own buffer, not re-measure."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET", 10_000)
    resp = _get(client, "/blob", _mint(), n=500)

    assert resp.status_code == 200
    assert sandbox_budget.snapshot("exec-123").bytes_sent == len(resp.content)


def test_a_loop_of_in_cap_responses_exhausts_the_budget_and_gets_429(client, monkeypatch):
    """Each response is far inside the 16 MiB per-response cap; the loop is what the aggregate
    budget exists to bound, and the 4h6.28 teardown fix does not bound it."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET", 5_000)
    token = _mint()

    statuses = [_get(client, "/blob", token, n=1000).status_code for _ in range(10)]

    assert statuses.count(200) > 1, "the first requests are served in full, not truncated"
    assert 429 in statuses, "the budget must stop the loop"
    # once spent it stays spent, and it is a 429 rather than a short body
    assert statuses[-1] == 429
    assert statuses == sorted(statuses), "no request is served after the budget is spent"


def test_the_budget_429_names_the_limit_and_the_observed_value(client, monkeypatch):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET", 100)
    token = _mint()
    _get(client, "/blob", token, n=500)
    resp = _get(client, "/blob", token, n=500)

    assert resp.status_code == 429
    payload = resp.json()
    assert payload["code"] == "sandbox_aggregate_bytes"
    assert payload["limit"] == 100
    assert payload["observed"] >= 500


def test_a_rejection_costs_no_handler_work(client, monkeypatch):
    """Admitted before the handler runs, so a spent execution costs no GCS read."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET", 100)
    token = _mint()
    _get(client, "/blob", token, n=500)
    _HANDLER_CALLS.clear()

    assert _get(client, "/blob", token, n=500).status_code == 429
    assert _HANDLER_CALLS == []


def test_a_response_rejected_by_the_per_response_cap_charges_nothing(client, monkeypatch):
    """It was produced but never sent, and "sent" is the unit the budget counts. The loop it
    permits is bounded by the request-count limit instead."""
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 200)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET", 10_000)

    resp = _get(client, "/blob", _mint(), n=5000)

    assert resp.status_code == 429
    assert resp.json()["code"] == "sandbox_response_bytes"
    entry = sandbox_budget.snapshot("exec-123")
    assert entry.bytes_sent == 0
    assert entry.requests == 1, "it still consumes a request slot"


def test_a_stream_is_charged_and_releases_its_slot(client, monkeypatch):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET", 10_000)
    resp = _get(client, "/stream", _mint(), chunks=5, size=100)

    assert resp.status_code == 200
    entry = sandbox_budget.snapshot("exec-123")
    assert entry.bytes_sent == 500
    assert entry.in_flight == 0, "the terminal state; where the release happens is pinned below"


def test_the_slot_is_still_held_while_the_streaming_body_is_produced(client, monkeypatch):
    """The slot must survive the endpoint function returning.

    The terminal `in_flight == 0` above is satisfied by *any* placement and observes nothing
    about where the release happens. This samples from inside the `StreamingResponse` generator
    — after the endpoint function has returned — so it fails for every placement that frees the
    slot on the handler's return, on `http.response.start`, or from a `BackgroundTask`. It does
    **not** by itself rule out a dependency teardown; `test_an_unmatched_route_releases_its_slot`
    is what does that, for the reason recorded there.
    """
    monkeypatch.setattr(sandbox_budget, "SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET", 10_000)
    resp = _get(client, "/stream", _mint(), chunks=3, size=100, observe="exec-123")

    assert resp.status_code == 200
    assert _IN_FLIGHT_DURING_STREAM == [1, 1, 1], (
        "the concurrency slot must still be held while the body is on the wire"
    )
    assert sandbox_budget.snapshot("exec-123").in_flight == 0


def test_an_unmatched_route_releases_its_slot(client, monkeypatch):
    """This is the assertion that pins release to the **middleware**, not a dependency teardown.

    `admit` runs in the middleware, which sees every request; a FastAPI dependency — and so its
    teardown — is solved per *matched route*, and an unmatched path raises 404 out of the router
    before any dependency is entered. A teardown placement therefore never releases here, and
    the leak is permanent in both directions: the pod-wide slot is gone, and the entry can never
    be swept either, because `_sweep_locked` refuses to evict anything with `in_flight > 0`.

    Measured against a mutant that moves `release` into a `yield` dependency's teardown: this
    test fails (`in_flight == 1`) while every other test in this file still passes — including
    the streaming one above, because in FastAPI 0.136.1 that teardown runs *after* the response
    body, not before it.
    """
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 1)
    token = _mint()

    assert _get(client, "/no-such-route", token).status_code == 404
    entry = sandbox_budget.snapshot("exec-123")
    assert entry.in_flight == 0, "an unmatched route must not strand a concurrency slot"
    assert _get(client, "/blob", token, n=1).status_code == 200, "and the slot is reusable"


def test_an_in_cap_non_2xx_body_is_charged_to_the_aggregate_budget(client, monkeypatch):
    """"An error body is small" is false: FastAPI's own 422 handler echoes the offending input,
    so a non-2xx body is as caller-controlled as a 200 and must be charged like one."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET", 100_000)
    resp = _get(client, "/boom", _mint(), n=1000)

    assert resp.status_code == 404
    assert len(resp.content) > 1000
    assert sandbox_budget.snapshot("exec-123").bytes_sent == len(resp.content)


def test_a_loop_of_error_responses_exhausts_the_aggregate_budget(client, monkeypatch):
    """Uncharged error bodies made the real egress bound `MAX_REQUESTS_PER_EXECUTION` x
    (whatever fits in a URI or a body), not the aggregate budget."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET", 5_000)
    token = _mint()

    statuses = [_get(client, "/boom", token, n=1000).status_code for _ in range(10)]

    assert statuses.count(404) > 1, "the first error bodies are delivered in full"
    assert 429 in statuses, "the budget must stop the loop"
    assert statuses[-1] == 429


def test_an_over_cap_non_2xx_body_is_capped_but_keeps_its_status(client, monkeypatch):
    """The per-response cap applies to every status. It cannot answer 429 here — a 404 rewritten
    into a 429 loses the real answer and invites a pointless retry (pinned by
    tests/test_response_caps.py) — so the status survives and the oversized body does not."""
    monkeypatch.setattr(limits, "SANDBOX_MAX_RESPONSE_BYTES", 500)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET", 10_000_000)

    resp = _get(client, "/boom", _mint(), n=200_000)

    assert resp.status_code == 404, "the status is the answer and must survive"
    assert len(resp.content) <= 500, "200 014 bytes were delivered under a 500 byte cap"
    assert resp.json()["code"] == "sandbox_response_bytes"
    entry = sandbox_budget.snapshot("exec-123")
    assert entry.bytes_sent == 0, "an over-cap response sends a bounded stub, not the payload"
    assert entry.requests == 1, "it still consumes a request slot"


# --- 2. the request-count limit -------------------------------------------------------------


def test_the_request_count_limit_stops_a_loop_of_tiny_responses(client, monkeypatch):
    """The byte budget does not bound a loop of small responses, and every request still costs
    a tabix seek or a GCS range read."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 3)
    token = _mint()

    statuses = [_get(client, "/blob", token, n=1).status_code for _ in range(5)]

    assert statuses == [200, 200, 200, 429, 429]


def test_the_request_count_429_names_its_limit(client, monkeypatch):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1)
    token = _mint()
    _get(client, "/blob", token, n=1)
    payload = _get(client, "/blob", token, n=1).json()

    assert payload["code"] == "sandbox_request_count"
    assert (payload["limit"], payload["observed"]) == (1, 1)


def test_the_count_is_per_execution_not_per_pod(client, monkeypatch):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1)
    assert _get(client, "/blob", _mint(jti="exec-a"), n=1).status_code == 200
    assert _get(client, "/blob", _mint(jti="exec-a"), n=1).status_code == 429
    assert _get(client, "/blob", _mint(jti="exec-b"), n=1).status_code == 200


# --- 3. concurrency: rejected, never queued -------------------------------------------------


def _concurrent(client, first_token, second_token):
    """Hold one request inside its handler, then issue a second and read its answer."""

    async def scenario():
        headers = [("Authorization", f"Bearer {first_token}")]
        held = asyncio.create_task(_acall(client, "/slow", "", headers))
        await asyncio.wait_for(client.state.entered.wait(), timeout=5)
        try:
            second = await _acall(
                client, "/blob", "n=1", [("Authorization", f"Bearer {second_token}")]
            )
        finally:
            client.state.gate.set()
        return second, await asyncio.wait_for(held, timeout=5)

    return asyncio.run(scenario())


def test_a_second_concurrent_request_is_rejected_not_queued(client, monkeypatch):
    """Queueing would burn the sandbox's ~120s wall clock on a wait the script cannot see; the
    held request must still complete normally."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 1)
    token = _mint()

    second, held = _concurrent(client, token, token)

    assert second.status_code == 429
    payload = second.json()
    assert payload["code"] == "sandbox_concurrency"
    assert (payload["limit"], payload["observed"]) == (1, 1)
    assert held.status_code == 200
    assert sandbox_budget.snapshot("exec-123").in_flight == 0


def test_the_pod_wide_concurrency_limit_covers_distinct_executions(client, monkeypatch):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 10)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL", 1)

    second, held = _concurrent(client, _mint(jti="exec-a"), _mint(jti="exec-b"))

    assert second.status_code == 429
    assert second.json()["code"] == "sandbox_concurrency_pod"
    assert held.status_code == 200


def test_the_slot_is_released_so_the_next_request_succeeds(client, monkeypatch):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 1)
    token = _mint()

    for _ in range(3):
        assert _get(client, "/blob", token, n=1).status_code == 200


# --- 4. cleanup must never evict a live execution -------------------------------------------


def test_cleanup_drops_only_executions_whose_token_can_no_longer_authenticate(client):
    live, expired = _mint(jti="live"), _mint(jti="expired", ttl=1)
    assert _get(client, "/blob", live, n=100).status_code == 200
    assert _get(client, "/blob", expired, n=100).status_code == 200

    # far enough past `exp` that verify_sandbox_token's own leeway cannot still accept it
    now = time.time() + 1 + sandbox_token.LEEWAY_SECONDS + 1
    with sandbox_budget._lock:
        sandbox_budget._sweep_locked(now)

    assert sandbox_budget.snapshot("expired") is None
    assert sandbox_budget.snapshot("live").bytes_sent > 0, (
        "evicting a live execution silently resets its budget — the fail-open direction"
    )


def test_cleanup_never_evicts_an_execution_with_a_request_in_flight(client, monkeypatch):
    """A stream can outlive its own token, so expiry alone is not enough to declare an entry
    dead: the sweep also requires nothing in flight."""

    async def scenario():
        headers = [("Authorization", f"Bearer {_mint(jti='inflight', ttl=1)}")]
        held = asyncio.create_task(_acall(client, "/slow", "", headers))
        await asyncio.wait_for(client.state.entered.wait(), timeout=5)
        with sandbox_budget._lock:
            sandbox_budget._sweep_locked(time.time() + 3600)
        survived = sandbox_budget.snapshot("inflight")
        client.state.gate.set()
        await asyncio.wait_for(held, timeout=5)
        return survived

    survived = asyncio.run(scenario())
    assert survived is not None and survived.in_flight == 1


def test_a_full_tracker_refuses_a_new_execution_rather_than_evicting_a_live_one(
    client, monkeypatch
):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_TRACKED_EXECUTIONS", 1)
    assert _get(client, "/blob", _mint(jti="exec-a"), n=100).status_code == 200

    resp = _get(client, "/blob", _mint(jti="exec-b"), n=100)

    assert resp.status_code == 429
    assert resp.json()["code"] == "sandbox_execution_tracker_full"
    assert sandbox_budget.snapshot("exec-a").bytes_sent > 0

    # and the slot frees itself once the incumbent's token expires, with no operator action.
    # This must use a *new* jti: re-using "exec-a" takes admit's `entry is not None` path and
    # touches neither the sweep nor the tracker-full check, so it would demonstrate nothing.
    with sandbox_budget._lock:
        sandbox_budget._executions["exec-a"].expires_at = int(time.time()) - 3600
    assert _get(client, "/blob", _mint(jti="exec-c"), n=100).status_code == 200
    assert sandbox_budget.snapshot("exec-a") is None, "the expired incumbent was swept"


# --- 5. non-sandbox traffic is untouched ----------------------------------------------------


def test_a_verified_non_sandbox_caller_is_bound_by_none_of_them(client, monkeypatch):
    """Browser and BFF traffic reaches this middleware too; with every limit at 1 it must still
    see no accounting at all."""
    monkeypatch.setattr(sandbox_budget, "SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET", 1)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS", 1)
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL", 1)

    for _ in range(5):
        resp = _get(client, "/blob", INTERNAL_SECRET, n=5000)
        assert resp.status_code == 200
        assert len(resp.json()["payload"]) == 5000

    second, held = _concurrent(client, INTERNAL_SECRET, INTERNAL_SECRET)
    assert (second.status_code, held.status_code) == (200, 200)
    assert sandbox_budget._executions == {}, "no entry is created for a non-sandbox caller"


def test_an_unauthenticated_request_creates_no_accounting(client, monkeypatch):
    monkeypatch.setattr(sandbox_budget, "SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1)
    assert _get(client, "/blob", n=10).status_code == 401
    assert sandbox_budget._executions == {}


def test_an_invalid_sandbox_token_is_401_and_creates_no_accounting(client):
    """The middleware's own resolver must not 401 or account for it; auth_required owns that."""
    forged = jwt.encode({"aud": "results-api"}, "wrong-key", algorithm="HS256")
    assert _get(client, "/blob", forged, n=10).status_code == 401
    assert sandbox_budget._executions == {}


# --- 6. the shipped defaults ----------------------------------------------------------------


def test_the_defaults_are_the_documented_numbers():
    """Documented in app/core/sandbox_budget.py and in docs/code-execution-security.md § 4."""
    assert sandbox_budget.SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET == 1024**3
    assert sandbox_budget.SANDBOX_MAX_REQUESTS_PER_EXECUTION == 1000
    assert sandbox_budget.SANDBOX_MAX_CONCURRENT_REQUESTS == 4
    assert sandbox_budget.SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL == 8
    assert sandbox_budget.SANDBOX_MAX_TRACKED_EXECUTIONS == 4096


@pytest.mark.parametrize("raw", ["0", "-1", "", "not-a-number", "4.5"])
def test_a_nonsense_limit_fails_loudly_rather_than_rejecting_everything(monkeypatch, raw):
    """Every limit is a ceiling compared with `>=`, so 0 or a negative silently turns it into
    "reject every sandbox request" — a total outage of the sandbox data path that no health
    check would attribute to a typo in a manifest. It must stop the pod from starting."""
    monkeypatch.setenv("SANDBOX_MAX_REQUESTS_PER_EXECUTION", raw)
    with pytest.raises(ValueError, match="positive integer"):
        sandbox_budget._env_int("SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1000)


def test_the_pod_wide_concurrency_bound_is_never_tighter_than_one_executions():
    """Inverted, an execution could never reach its own allowance and the per-execution number
    in the manifest would be a lie. Asserted at import; this pins the shipped pair."""
    assert (
        sandbox_budget.SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL
        >= sandbox_budget.SANDBOX_MAX_CONCURRENT_REQUESTS
    )
