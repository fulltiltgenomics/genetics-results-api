"""
Pure ASGI middleware for endpoint usage logging.

Emits structured JSON logs for each request, which can be routed to BigQuery
via a GCP log sink filter on log_type="endpoint_access".
"""

from datetime import datetime, timezone
import logging
import time
from typing import Optional

from starlette.types import ASGIApp, Message, Receive, Scope, Send

import app.config.common as config

logger = logging.getLogger(__name__)

SERVICE = "results-api"


def _should_log_path(path: str) -> bool:
    """check if path should be logged based on config"""
    return path not in config.usage_logging_excluded_paths


def _extract_user_from_header(scope: Scope) -> Optional[str]:
    """Extract user email from the X-Goog-Authenticated-User-Email header (set by IAP or oauth2-proxy).

    Identical rule to app.core.auth.get_authenticated_user: the header is believed only when the
    caller presented the internal secret *and* the asserted address is allow-listed. Otherwise
    any pod that can reach this service could pick whose name lands in the endpoint_access log,
    and an identity the auth path refused would still be attributed a request here.
    """
    from app.core.auth import _email_allowed, is_internal_caller

    headers = dict(scope.get("headers", []))
    iap_email = headers.get(b"x-goog-authenticated-user-email", b"").decode("utf-8", errors="ignore")
    if not iap_email:
        return None
    # latin-1, like starlette's own header decoding and app.middleware's sandbox peek. This is
    # a tightening, and the reason is agreement, not a crash: the two producers of the str
    # is_internal_caller receives (starlette, and this line) must decode the same bytes the
    # same way or the usage log and the auth path disagree about who called. `errors="ignore"`
    # made them disagree in the dangerous direction — it silently dropped undecodable bytes, so
    # b"Bearer <secret>\xff" was attributed to a user here while starlette's latin-1 path
    # rejected the identical request. latin-1 is total over arbitrary bytes, so no errors= is
    # needed, and it round-trips byte-exactly through is_internal_caller's latin-1 re-encode.
    auth_header = headers.get(b"authorization", b"").decode("latin-1")
    if not is_internal_caller(auth_header):
        return None
    # header format: "accounts.google.com:user@domain.com"
    email = iap_email.split(":")[-1] if ":" in iap_email else iap_email
    # normalized exactly as get_authenticated_user does, so one person is one identity in the log
    return email.strip().lower() if _email_allowed(email) else None


class UsageLoggingMiddleware:
    """
    Pure ASGI middleware for logging endpoint usage.

    Captures per request: message, log_type, service, log_source, timestamp, user_email,
    endpoint_path, full_path (stdout only — the Cloud Logging path strips it), http_method,
    status_code, duration_ms and response_body_bytes, plus sid and jti when a sandbox
    token authorized the call.

    Logs are emitted with log_type="endpoint_access" for filtering in GCP log sinks.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")

        if not _should_log_path(path):
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "UNKNOWN")
        start_time = time.perf_counter()
        status_code: int = 0
        response_bytes: int = 0
        user_email = _extract_user_from_header(scope)

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, response_bytes
            if message["type"] == "http.response.start":
                status_code = message.get("status", 0)
            elif message["type"] == "http.response.body":
                # Accumulated here rather than read off Content-Length, because the responses
                # whose size the question is about are the streamed ones: TimedStreamingResponse
                # sets no Content-Length, so the header is absent for precisely the large bodies.
                # The chunk is forwarded untouched and only an int is kept, so nothing is
                # buffered and a stream that must not be held back is not held back; the cost is
                # one O(1) len() and one add per chunk already materialized by the ASGI layer.
                # A client that disconnects mid-stream simply leaves the count at the bytes that
                # actually went out, which is the honest number.
                response_bytes += len(message.get("body", b""))
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000

            # fall back to user resolved by auth dependency (for bearer token requests)
            if not user_email:
                state = scope.get("state", {})
                user_email = state.get("authenticated_user")

            # get route template for privacy (e.g., "/api/v1/search/{query}" instead of actual query)
            route = scope.get("route")
            endpoint_template = route.path if route else path

            query_string = scope.get("query_string", b"").decode("utf-8", errors="ignore")
            full_path = f"{path}?{query_string}" if query_string else path

            entry = {
                "message": "endpoint access",
                "log_type": "endpoint_access",
                # the service discriminator in the shared sink, where db-api's endpoint_access
                # rows sit beside these. Constant and not env-derived on purpose: log_source
                # below is built from DEPLOY_ENV, carries no service name and has already been
                # renamed once in production (genetics-results-api-prod -> finngenie_prod), so a
                # query keyed on it silently returns nothing after a deploy renames it.
                "service": SERVICE,
                "log_source": config.log_source,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "user_email": user_email,
                "endpoint_path": endpoint_template,
                "full_path": full_path,  # included in stdout, stripped for Cloud Logging
                "http_method": method,
                "status_code": status_code,
                "duration_ms": round(duration_ms, 2),
                # Uncompressed application body bytes, excluding headers: `setup_middleware`
                # registers this middleware INSIDE GZipMiddleware, so it sees the body the caller
                # decodes, which is the same quantity SandboxResponseCapMiddleware caps and hence
                # the one the response-cap and truncation questions need.
                #
                # Deliberately a self-describing payload key and NOT `httpRequest.responseSize`:
                # that field means wire size *including* headers, and auth-gateway's
                # `$body_bytes_sent` and the GCLB request logs already fill it with genuine wire
                # bytes elsewhere in this suite, so a third source with different semantics under
                # the same name would mix silently in a join. The sink grows its jsonPayload
                # schema when a new key first appears, so the column costs nothing extra; a plain
                # key also preserves 0, which protobuf JSON drops from an int64 responseSize.
                "response_body_bytes": response_bytes,
            }

            # a sandbox execution carries the conversation and the execution id, which is what
            # makes "what did that script actually read?" answerable across the three services
            principal = scope.get("state", {}).get("sandbox_principal")
            if principal is not None:
                entry["sid"] = principal.session_id
                entry["jti"] = principal.execution_id
                # `sub` is the end-user address chat-backend/genetics-mcp-server put in the
                # token (`src/genetics_mcp_server/sandbox_token.py`); the supervisor only checks
                # it matches the user it was minted for, and this service's verifier only
                # requires the claim to be present and non-empty. That is deliberately NOT the
                # rule the header path above applies — it returns None unless `_email_allowed`
                # passes — and the asymmetry is the point: a signed token is not spoofable by
                # anything that can reach this port, an identity header is.
                #
                # Stamped as `principal.identity` (`sandbox:<sub>`), never the bare address: the
                # prefix is what keeps a script's row separable from a verified human's in the
                # one column that reaches BigQuery, and it is byte-identical to what
                # `get_verified_user` already puts in `state["authenticated_user"]`, so this
                # assignment is a no-op wherever auth ran. It matters where auth did not: on an
                # `@is_public` route with the minimal anonymous surface off, or with
                # REQUIRE_AUTH=false (the local e2e harness), user_email was null and a sandbox
                # row differed from an unaccounted INTERNAL_API_SECRET row only by `jti`
                # (genetics-results-suite-4h6.65).
                entry["user_email"] = principal.identity

            # log as dict for Cloud Logging to parse as jsonPayload
            logger.info(entry)
