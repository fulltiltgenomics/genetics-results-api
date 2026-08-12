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
    auth_header = headers.get(b"authorization", b"").decode("utf-8", errors="ignore")
    if not is_internal_caller(auth_header):
        return None
    # header format: "accounts.google.com:user@domain.com"
    email = iap_email.split(":")[-1] if ":" in iap_email else iap_email
    # normalized exactly as get_authenticated_user does, so one person is one identity in the log
    return email.strip().lower() if _email_allowed(email) else None


class UsageLoggingMiddleware:
    """
    Pure ASGI middleware for logging endpoint usage.

    Captures: timestamp, user_email, endpoint_path, http_method, status_code, duration_ms

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
        user_email = _extract_user_from_header(scope)

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message.get("status", 0)
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
            }

            # a sandbox execution carries the conversation and the execution id, which is what
            # makes "what did that script actually read?" answerable across the three services
            principal = scope.get("state", {}).get("sandbox_principal")
            if principal is not None:
                entry["sid"] = principal.session_id
                entry["jti"] = principal.execution_id

            # log as dict for Cloud Logging to parse as jsonPayload
            logger.info(entry)
