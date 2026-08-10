import json
import logging

from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send

import app.config.common as config
from app.core import limits

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
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        state: dict = {"capped": None, "start": None, "body": bytearray()}

        async def send_wrapper(message):
            if state["capped"] is False:
                await send(message)
                return

            if message["type"] == "http.response.start":
                caps = limits.caps_for_scope(scope)
                # only successful responses are capped: an error body is small and its status
                # must survive
                state["capped"] = caps.enforced and 200 <= message["status"] < 300
                if not state["capped"]:
                    await send(message)
                    return
                state["caps"] = caps
                state["start"] = message
                return

            if message["type"] != "http.response.body":
                await send(message)
                return

            caps = state["caps"]
            body = state["body"]
            body += message.get("body", b"")
            if len(body) > caps.max_response_bytes:
                await _reject(send, scope, caps, len(body))
                raise _ResponseCapExceeded
            if message.get("more_body"):
                return

            headers = [
                (k, v)
                for k, v in state["start"]["headers"]
                if k.lower() != b"content-length"
            ]
            headers.append((b"content-length", str(len(body)).encode()))
            await send({**state["start"], "headers": headers})
            # the bytearray goes out as-is: a bytes() copy here doubled the peak for no gain,
            # and nothing downstream mutates or retains it
            await send({"type": "http.response.body", "body": body, "more_body": False})

        try:
            await self.app(scope, receive, send_wrapper)
        except _ResponseCapExceeded:
            pass


async def _reject(send: Send, scope: Scope, caps: limits.Caps, observed: int) -> None:
    limit = caps.max_response_bytes
    principal = (scope.get("state") or {}).get("sandbox_principal")
    logger.warning(
        "sandbox response cap exceeded",
        extra={
            "path": scope.get("path"),
            "limit": limit,
            "observed": observed,
            "sid": getattr(principal, "session_id", None),
            "jti": getattr(principal, "execution_id", None),
        },
    )
    body = json.dumps({
        "detail": (
            f"Response exceeds the per-execution byte limit ({observed} > {limit}). "
            "Narrow the request — a smaller region, fewer ids — and aggregate in the sandbox."
        )
    }).encode()
    await send({
        "type": "http.response.start",
        "status": 429,
        "headers": [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
        ],
    })
    await send({"type": "http.response.body", "body": body, "more_body": False})


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

    # usage logging (outermost - captures full request duration)
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
