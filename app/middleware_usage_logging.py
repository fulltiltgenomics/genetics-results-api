"""
Pure ASGI middleware for endpoint usage logging.

Emits structured JSON logs for each request, which can be routed to BigQuery
via a GCP log sink filter on log_type="endpoint_access".
"""

from datetime import datetime, timezone
import logging
import time
from typing import Optional

from itsdangerous import URLSafeTimedSerializer, BadSignature
from starlette.types import ASGIApp, Message, Receive, Scope, Send

import app.config.common as config

logger = logging.getLogger(__name__)


def _should_log_path(path: str) -> bool:
    """check if path should be logged based on config"""
    return path not in config.usage_logging_excluded_paths


def _extract_user_from_session(scope: Scope) -> Optional[str]:
    """
    Extract user email from session cookie.

    The session is signed using itsdangerous (Starlette's SessionMiddleware).
    We decode it here since we're in pure ASGI middleware.
    """
    headers = dict(scope.get("headers", []))
    cookie_header = headers.get(b"cookie", b"").decode("utf-8", errors="ignore")

    if not cookie_header:
        return None

    cookies = {}
    for item in cookie_header.split(";"):
        item = item.strip()
        if "=" in item:
            key, value = item.split("=", 1)
            cookies[key.strip()] = value.strip()

    session_cookie = cookies.get("session")
    if not session_cookie:
        return None

    try:
        serializer = URLSafeTimedSerializer(config.session_secret_key)
        session_data = serializer.loads(
            session_cookie,
            max_age=config.session_max_age,
        )
        return session_data.get("user_email")
    except (BadSignature, Exception):
        return None


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
        user_email = _extract_user_from_session(scope)

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message.get("status", 0)
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000

            # get route template for privacy (e.g., "/api/v1/search/{query}" instead of actual query)
            route = scope.get("route")
            endpoint_template = route.path if route else path

            query_string = scope.get("query_string", b"").decode("utf-8", errors="ignore")
            full_path = f"{path}?{query_string}" if query_string else path

            # log as dict for Cloud Logging to parse as jsonPayload
            logger.info(
                {
                    "message": "endpoint access",
                    "log_type": "endpoint_access",
                    "log_source": f"genetics-results-api-{config.deploy_env}",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "user_email": user_email,
                    "endpoint_path": endpoint_template,
                    "full_path": full_path,  # included in stdout, stripped for Cloud Logging
                    "http_method": method,
                    "status_code": status_code,
                    "duration_ms": round(duration_ms, 2),
                }
            )
