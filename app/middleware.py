import asyncio
import json
import logging

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send

import app.config.common as config
from app.core import limits, sandbox_budget, sandbox_token

logger = logging.getLogger(__name__)


class _ResponseCapExceeded(Exception):
    """Raised out of ``send`` once the 429 is on the wire, to tear the producer down.

    Dropping later messages on the floor bounds what the caller *receives* but not what this
    service *spends*: the handler's generator keeps running to completion — GCS range reads and
    the tabix filter pool on the endpoints that matter — with every chunk discarded. Raising
    instead breaks ``StreamingResponse.stream_response``'s ``async for`` at its next ``send``,
    so the iterator is abandoned and closed. Caught in ``__call__`` below; it passes through
    Starlette's ``ExceptionMiddleware`` (which handles only ``HTTPException``) untouched, and
    never reaches ``ServerErrorMiddleware``, so no 500 is attempted over the sent 429.
    """


class SandboxResponseCapMiddleware:
    """Enforce the sandbox response-byte cap (see app/core/limits.py).

    Pure ASGI and a strict no-op for every request that is not sandbox-capped: relaxed callers
    — which is all browser and BFF traffic — are never buffered and never inspected.

    A capped response *is* buffered, which is what makes the answer a 429 rather than a
    truncated body: a stream cannot be un-sent once its first chunk is on the wire, and the
    design of record requires "over budget -> 429, not a silent truncation". The buffer is
    bounded by the byte cap itself — it is abandoned, and the producer torn down, the moment it
    exceeds it.

    **Bytes only, no row count.** Counting rows meant ``json.loads`` over the whole buffer on
    the event loop, whose object graph is several times the byte size: on a ``replicas: 1`` pod
    the row cap was a memory amplifier that only a sandbox caller could trigger, so presenting
    the token hurt the pod more than omitting it. It also never bound the endpoints it was
    written for — TSV, the default ``format`` of every bulk range endpoint, parses as nothing —
    and `app.core.limits` already records the byte cap as the one that binds here.

    Registered innermost, so it measures the payload the caller's script actually decodes
    rather than its gzipped size.

    **It also holds the per-execution limits** (`app/core/sandbox_budget.py`): the aggregate
    response-byte budget, the request count and the concurrency slot are all admitted here,
    before the handler runs, and released in a `finally` that the ASGI contract puts after the
    last byte of the response — a `StreamingResponse` included, since its generator is driven
    from inside this `__call__`.

    A FastAPI dependency's teardown is the placement to avoid, for a reason that is *not* the
    streaming one an earlier draft of this docstring gave: measured on FastAPI 0.136.1, a
    `yield` dependency's exit code runs after the response body, so for a matched route the two
    placements are indistinguishable. What separates them is that `admit` runs for **every**
    request while a dependency is solved only for a **matched route** — an unmatched path 404s
    out of the router with no dependency ever entered, so a teardown placement would strand
    that slot forever, and `_sweep_locked` cannot reclaim an entry with `in_flight > 0` either.
    `tests/test_sandbox_budget.py::test_an_unmatched_route_releases_its_slot` is the test that
    fails if the release moves. This is also the only layer that knows the exact byte count that
    went on the wire.

    **It also arms the sandbox request deadline** (`SANDBOX_REQUEST_TIMEOUT_SECONDS`), for the
    same reason and in the same `try`. Nothing else was bounding how long one request may hold a
    slot: a request wedged in a GCS read held a per-execution slot, a pod-wide slot **and** its
    counter-map entry for as long as the socket stayed open, since `_sweep_locked` refuses to
    evict an entry with `in_flight > 0` — so the rule that closes the fail-open direction had no
    counterpart bounding how long an entry may stay unevictable, and a handful of hung requests
    reached the pod-wide bound with no attacker (`genetics-results-suite-yv4`).

    The two rejected placements: **uvicorn has no per-request timeout to set** — `timeout_keep_alive`
    bounds an idle connection between requests and `timeout_graceful_shutdown` a shutdown, neither
    of which touches a request in progress — and an **outer ASGI middleware** would cancel this
    `__call__` from outside, which still runs the `finally` but makes the deadline and the
    release two separately-ordered layers that a later `setup_middleware` edit could reorder
    without any test noticing. Armed here, the `TimeoutError` unwinds through the very `finally`
    that calls `release`, so the release on the timeout path is the same line as on the happy
    path rather than a second one that has to be kept in step.

    Armed only for a request that carries an execution token: a browser or BFF request holds no
    slot, pins no counter entry, and a deadline on it would be a new bound on traffic this bead
    is not about (`asyncio.timeout(None)` is the no-op for those).
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        principal = _sandbox_principal(scope)

        # `sent` is what actually reached the caller, and is what the aggregate budget is
        # charged; it stays 0 for a response this middleware did not put on the wire itself.
        # `wrote` is the weaker, separate fact that *something* has been handed to `send` —
        # see the deadline handler for why the two are not interchangeable.
        state: dict = {
            "capped": None, "start": None, "body": bytearray(), "sent": 0, "wrote": False
        }

        async def send_out(message):
            """The single door to the wire. Everything this middleware sends goes through it,
            so `wrote` is exactly "the response has begun" and no later branch has to
            reconstruct that from the buffering state."""
            state["wrote"] = True
            await send(message)

        async def send_wrapper(message):
            if state["capped"] is False:
                await send_out(message)
                return

            if message["type"] == "http.response.start":
                caps = limits.caps_for_scope(scope)
                # every status is capped and charged, not only 2xx. "An error body is small" was
                # false: FastAPI's 422 handler echoes the offending input, so a 100 000-char
                # query param produces a 100 KB error body — caller-controlled, exactly like a
                # 200. While non-2xx passed through unbuffered it was neither capped nor charged,
                # which made the real egress bound SANDBOX_MAX_REQUESTS_PER_EXECUTION x (whatever
                # fits in a URI or a body) rather than the aggregate budget. What the status
                # changes is only the *rejection*: see `_reject`.
                state["capped"] = caps.enforced
                if not state["capped"]:
                    await send_out(message)
                    return
                state["caps"] = caps
                state["start"] = message
                return

            if message["type"] != "http.response.body":
                await send_out(message)
                return

            caps = state["caps"]
            body = state["body"]
            body += message.get("body", b"")
            if len(body) > caps.max_response_bytes:
                await _reject(send_out, scope, caps, len(body), state["start"]["status"])
                raise _ResponseCapExceeded
            if message.get("more_body"):
                return

            headers = [
                (k, v)
                for k, v in state["start"]["headers"]
                if k.lower() != b"content-length"
            ]
            headers.append((b"content-length", str(len(body)).encode()))
            await send_out({**state["start"], "headers": headers})
            # the bytearray goes out as-is: a bytes() copy here doubled the peak for no gain,
            # and nothing downstream mutates or retains it
            await send_out({"type": "http.response.body", "body": body, "more_body": False})
            state["sent"] = len(body)

        deadline = asyncio.timeout(
            sandbox_budget.SANDBOX_REQUEST_TIMEOUT_SECONDS if principal is not None else None
        )
        # `admit` reserves three things at once, and the reservation is only undone by the
        # `finally` below. Anything raising between the reservation and the `try` would strand a
        # per-execution slot, a pod-wide slot and a map entry `_sweep_locked` may never evict —
        # permanently, since no deadline is armed yet either. So the admission happens *inside*
        # the try, and the flag is set the instant it returns.
        admitted = False
        try:
            if principal is not None:
                rejection = sandbox_budget.admit(principal)
                if rejection is not None:
                    sandbox_budget.log_rejection(rejection, scope.get("path"), principal)
                    await _send_429(
                        send_out,
                        {
                            "detail": rejection.detail,
                            "code": rejection.code,
                            "limit": rejection.limit,
                            "observed": rejection.observed,
                        },
                    )
                    return
                admitted = True
                # after the flag, never before: this is the admission signal an operator reads,
                # and `admit` deliberately no longer emits it, because anything raising between
                # its counter increment and its return strands the reservation for good
                sandbox_budget.log_admission(principal)

            async with deadline:
                await self.app(scope, receive, send_wrapper)
        except _ResponseCapExceeded:
            pass
        except TimeoutError:
            # a TimeoutError the *handler* raised is its own 500 to answer, not ours; only a
            # cancellation this deadline caused is ours to convert
            if not deadline.expired():
                raise
            if state["wrote"]:
                # The deadline fired *after* the response had begun. Measured: a `yield`
                # dependency's teardown runs after the body, so a slow teardown produces exactly
                # this — the caller already has its answer and the request succeeded. Sending
                # the 504 here put a second `http.response.start` on a completed response (an
                # ASGI protocol violation) and counted a success as `sandbox_request_timeout`,
                # which made the counters lie. So it gets its own code and no 504 — **not**
                # silence: the slot was still pinned for the full deadline, which is the cheapest
                # slot-pinning primitive in the module, and swallowing it would remove the only
                # signal that it happened.
                #
                # `state["sent"] == 0` is NOT the right test for this. It is 0 both when nothing
                # has been sent and while the *body* message is still in flight — the deadline
                # can fire between this middleware's two `send`s, or inside uvicorn's `drain()`
                # against a slow-reading client, and the start message is on the wire in both.
                # `wrote` is set by `send_out` and so covers those too. (The teardown variant is
                # measured. The drain variant is reproduced only against a blocking recorder, not
                # a real uvicorn socket under backpressure: the control flow is the same one, but
                # that `drain()` suspends exactly where assumed is unconfirmed.)
                sandbox_budget.log_request_timeout(
                    scope.get("path"), principal, response_started=True
                )
            else:
                sandbox_budget.log_request_timeout(scope.get("path"), principal)
                await _send_json(send_out, 504, {
                    "detail": (
                        "This request exceeded the sandbox request deadline "
                        f"({sandbox_budget.SANDBOX_REQUEST_TIMEOUT_SECONDS}s) and was "
                        "abandoned. Narrow the request and retry."
                    ),
                    "code": "sandbox_request_timeout",
                    "limit": sandbox_budget.SANDBOX_REQUEST_TIMEOUT_SECONDS,
                    "observed": sandbox_budget.SANDBOX_REQUEST_TIMEOUT_SECONDS,
                })
        finally:
            if admitted:
                sandbox_budget.release(principal.execution_id, state["sent"])


def _sandbox_principal(scope: Scope) -> sandbox_token.SandboxPrincipal | None:
    """Resolve the execution token straight from the ASGI scope, non-raising.

    The per-execution limits have to be admitted *before* the handler runs — that is the whole
    point of a request-count and a concurrency bound — whereas ``request.state.sandbox_principal``
    is set later, by ``app.dependencies.auth_required``. So this middleware verifies the bearer
    itself, with the same ``is_sandbox_shaped`` routing and the same HS256 decode against the
    same key.

    **They agree for a matched route and only for one**, and an earlier draft of this docstring
    asserted the unconditional version. ``limits.caps_for_scope`` reads
    ``scope["state"]["sandbox_principal"]``, which ``app.dependencies.auth_required`` sets — a
    **route** dependency. It is app-level (``app/server.py``'s ``FastAPI(dependencies=[...])``)
    and it runs before every short circuit in ``auth_required``, so every registered route,
    ``@is_public`` ones included, sets it. A request that matches **no** route never solves a
    dependency, so this function resolves a principal, ``admit`` reserves a slot, and
    ``caps_for_scope`` still reports ``RELAXED`` — the response is neither capped nor charged.
    That is bounded by what an unmatched path can produce: the router's own 404/405/307, a fixed
    handful of bytes. It is bounded by nothing else, so a route registered outside that app-level
    dependency would be a real cap bypass — which is why ``docs_url``/``redoc_url``/``openapi_url``
    are ``None`` and the three doc routes are re-registered by hand behind the dependency
    (``app/server.py``, which records the incident where they were not).

    Never raises: a sandbox-shaped bearer that fails validation is left to ``auth_required``,
    which answers the 401 it already answers today. Rejecting here would duplicate that, and
    silently, from a layer with no route context.
    """
    for key, value in scope.get("headers") or ():
        if key == b"authorization":
            header = value.decode("latin-1")
            break
    else:
        return None
    if not header.startswith("Bearer "):
        return None
    token = header[7:]
    if not sandbox_token.is_sandbox_shaped(token):
        return None
    try:
        return sandbox_token.verify_sandbox_token(token)
    except sandbox_token.SandboxTokenError:
        return None


async def _send_json(send: Send, status: int, payload: dict) -> None:
    body = json.dumps(payload).encode()
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
        ],
    })
    await send({"type": "http.response.body", "body": body, "more_body": False})


async def _send_429(send: Send, payload: dict) -> None:
    await _send_json(send, 429, payload)


async def _reject(
    send: Send, scope: Scope, caps: limits.Caps, observed: int, status: int
) -> None:
    """Answer an over-cap response with a bounded stub instead of the body it produced.

    A 2xx becomes a 429: "narrow the request" is actionable, and the caller loses nothing but a
    body it could not have been sent anyway. A non-2xx keeps **its own status**, because there
    the status *is* the answer — rewriting a 404 into a 429 loses it and invites a retry that
    can only 404 again (`tests/test_response_caps.py`). Either way the body that goes on the
    wire is this stub, so the cap binds on every status; the payload is identical so an operator
    reads one shape.
    """
    limit = caps.max_response_bytes
    principal = (scope.get("state") or {}).get("sandbox_principal")
    logger.warning(
        "sandbox response cap exceeded",
        extra={
            "path": scope.get("path"),
            "status": status,
            "limit": limit,
            "observed": observed,
            "sid": getattr(principal, "session_id", None),
            "jti": getattr(principal, "execution_id", None),
        },
    )
    # `code`/`limit`/`observed` are additive and shared with the per-execution limits in
    # app/core/sandbox_budget.py, so every sandbox rejection says which control was hit; the cap
    # itself and its detail string are unchanged
    await _send_json(send, 429 if 200 <= status < 300 else status, {
        "detail": (
            f"Response exceeds the per-execution byte limit ({observed} > {limit}). "
            "Narrow the request — a smaller region, fewer ids — and aggregate in the sandbox."
        ),
        "code": "sandbox_response_bytes",
        "limit": limit,
        "observed": observed,
    })


class SecurityHeadersMiddleware:
    """
    Pure ASGI middleware to add security headers to all responses.
    Implemented as pure ASGI middleware to avoid issues with streaming responses
    that occur with BaseHTTPMiddleware.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.extend(
                    [
                        (
                            b"strict-transport-security",
                            b"max-age=31536000; includeSubDomains",
                        ),
                        (b"x-content-type-options", b"nosniff"),
                        (b"x-frame-options", b"DENY"),
                        (b"x-xss-protection", b"1; mode=block"),
                    ]
                )
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_wrapper)


def setup_middleware(app: FastAPI):
    """Configure all middleware for the FastAPI application."""
    # innermost (added first): sees the response before GZip compresses it, so the cap is
    # measured on the bytes the caller decodes, and its 429 is logged by the usage middleware
    app.add_middleware(SandboxResponseCapMiddleware)

    # usage logging: wraps the cap middleware above, but add_middleware prepends, so
    # SecurityHeaders/GZip/CORS below all end up OUTSIDE it. That position is load-bearing —
    # being inside GZip is what makes the size it records uncompressed body bytes (the same
    # quantity the cap measures) rather than wire bytes, and what lets it log the cap's 429.
    if config.usage_logging_enabled:
        from app.middleware_usage_logging import UsageLoggingMiddleware

        app.add_middleware(UsageLoggingMiddleware)

    app.add_middleware(SecurityHeadersMiddleware)

    app.add_middleware(GZipMiddleware, minimum_size=1000)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=[
            "Accept",
            "Accept-Language",
            "Content-Language",
            "Content-Type",
            "Authorization",
        ],
    )
