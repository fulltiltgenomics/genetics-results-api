"""Authentication via X-Goog-Authenticated-User-Email header (set by IAP or oauth2-proxy)
and Authorization: Bearer token (shared secret or Google Identity Token).
"""

import hmac
import logging
import threading

from fastapi import HTTPException, Request

import app.config.common as config

logger = logging.getLogger(__name__)

# lazily initialized Google auth transport for JWKS caching
_google_request = None
_google_request_lock = threading.Lock()


def _get_google_request():
    global _google_request
    if _google_request is None:
        with _google_request_lock:
            if _google_request is None:
                from google.auth.transport import requests as google_requests
                _google_request = google_requests.Request()
    return _google_request


def get_authenticated_user(request: Request) -> str | None:
    """Extract authenticated user email from the IAP/oauth2-proxy header."""
    iap_email = request.headers.get("X-Goog-Authenticated-User-Email")
    if not iap_email:
        return None
    # header format: "accounts.google.com:user@domain.com"
    return iap_email.split(":")[-1] if ":" in iap_email else iap_email


def get_bearer_token_user(request: Request) -> str | None:
    """Validate a bearer token from the Authorization header.

    Checks in order: shared internal secret, then Google Identity Token (JWT).
    Returns user identity if valid, None if no bearer token present.
    Raises HTTPException(401/403) if token is present but invalid.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return None

    token = auth_header[7:]

    # check shared secret for internal service-to-service auth
    if config.internal_api_secret and hmac.compare_digest(token, config.internal_api_secret):
        return "mcp-tool"

    # try Google Identity Token validation
    from google.oauth2 import id_token
    try:
        payload = id_token.verify_oauth2_token(token, _get_google_request())
    except ValueError as e:
        logger.warning(f"invalid bearer token: {e}")
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    email = payload.get("email")
    if not email:
        raise HTTPException(status_code=401, detail="Token does not contain email")

    if not payload.get("email_verified", False):
        raise HTTPException(status_code=401, detail="Email not verified")

    # domain restriction
    domain = email.split("@")[-1] if "@" in email else ""
    if email not in config.allowed_emails and domain not in config.allowed_email_domains:
        raise HTTPException(status_code=403, detail="Email domain not allowed")

    return email


def get_verified_user(request: Request) -> str | None:
    """Get authenticated user from bearer token or oauth2-proxy header."""
    # try bearer token first (programmatic API access / internal service calls)
    email = get_bearer_token_user(request)
    if email is None:
        # fall back to oauth2-proxy header (browser sessions)
        email = get_authenticated_user(request)
    return email
