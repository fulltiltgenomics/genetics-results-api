"""Authentication via X-Goog-Authenticated-User-Email header (set by IAP or oauth2-proxy)
and Authorization: Bearer token (shared secret or Google Identity Token).

The header alone is not a credential — it is trusted only when the request also carries the
internal shared secret, which marks the caller as one of the in-cluster proxies.
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


def _email_allowed(email: str) -> bool:
    """True when the address is covered by ALLOWED_EMAILS or ALLOWED_EMAIL_DOMAINS.

    Compared case-insensitively on both sides: oauth2-proxy lower-cases the address before its
    own domain check, so `User@FinnGen.fi` gets a session there and must not be rejected here.
    A literal `*` in ALLOWED_EMAIL_DOMAINS means "any domain", matching what oauth2-proxy does
    with the same value — without this it would match no domain at all and lock out every user
    of a deployment whose operator set `oauth_email_domain = "*"` deliberately. Note it also
    opens the Google-JWT path to any verified Google account, leaving GOOGLE_TOKEN_AUDIENCE as
    the only narrowing; that is what `*` asks for.
    """
    domains = {d.strip().lower() for d in config.allowed_email_domains}
    if "*" in domains:
        return True
    email = email.strip().lower()
    domain = email.split("@")[-1] if "@" in email else ""
    return email in {e.strip().lower() for e in config.allowed_emails} or domain in domains


def is_internal_caller(auth_header: str | None) -> bool:
    """True when the Authorization header carries the shared internal secret.

    This is the trusted-proxy marker: only in-cluster services holding INTERNAL_API_SECRET
    (bff, auth-gateway's bearer path, chat-backend, mcp-server) can produce it, so a pod that
    merely has network reach to results-api cannot.
    """
    if not config.internal_api_secret:
        return False
    if not auth_header or not auth_header.startswith("Bearer "):
        return False
    # compare as bytes: compare_digest on str raises TypeError for non-ASCII, and this runs in
    # ASGI middleware before routing, so a non-ASCII bearer would 500 instead of failing closed
    return hmac.compare_digest(
        auth_header[7:].encode("utf-8"), config.internal_api_secret.encode("utf-8")
    )


def get_authenticated_user(request: Request) -> str | None:
    """Extract the user email a trusted proxy asserted via the IAP/oauth2-proxy header.

    X-Goog-Authenticated-User-Email is attacker-controlled on the wire — anything that can
    reach results-api directly can set it to any string. It is therefore honoured only when the
    caller also proves it is an internal service, and the asserted identity is held to the same
    allow-list as the Google-JWT path. Anything else fails closed to unauthenticated.
    """
    iap_email = request.headers.get("X-Goog-Authenticated-User-Email")
    if not iap_email:
        return None
    if not is_internal_caller(request.headers.get("Authorization")):
        logger.warning(
            "ignoring X-Goog-Authenticated-User-Email: caller did not present the internal secret"
        )
        return None
    # header format: "accounts.google.com:user@domain.com"
    email = iap_email.split(":")[-1] if ":" in iap_email else iap_email
    if not _email_allowed(email):
        logger.warning("proxied identity rejected: email not in the allow-list")
        return None
    # matching is case-insensitive and whitespace-tolerant, so the same person can arrive as
    # several spellings; return the normalized form or endpoint_access splits them into
    # separate identities. Must stay in step with _extract_user_from_header.
    return email.strip().lower()


def _validate_user_api_token(token: str) -> str | None:
    """Validate a user API token via the chat backend. Returns user_id or None."""
    if not config.chat_backend_url:
        return None
    try:
        import requests
        headers = {}
        if config.internal_api_secret:
            headers["Authorization"] = f"Bearer {config.internal_api_secret}"
        resp = requests.post(
            f"{config.chat_backend_url}/v1/tokens/validate",
            json={"token": token},
            headers=headers,
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("valid"):
                return data["user_id"]
    except Exception as e:
        logger.warning(f"user token validation failed: {e}")
    return None


def get_bearer_token_user(request: Request) -> str | None:
    """Validate a bearer token from the Authorization header.

    Checks in order: shared internal secret, then routes by format —
    JWTs (contain dots) go to Google validation, others to user token validation.
    Returns user identity if valid, None if no bearer token present.
    Raises HTTPException(401/403) if token is present but invalid.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return None

    token = auth_header[7:]

    # check shared secret for internal service-to-service auth
    if is_internal_caller(auth_header):
        return "mcp-tool"

    # route by token format: JWTs have dots, user tokens don't
    if "." in token:
        # Google Identity Token (JWT) — no HTTP call needed
        from google.oauth2 import id_token
        try:
            payload = id_token.verify_oauth2_token(token, _get_google_request())
        except ValueError as e:
            logger.warning(f"invalid bearer token: {e}")
            raise HTTPException(status_code=401, detail="Invalid or expired token")
    else:
        # user API token — validate via chat backend
        user = _validate_user_api_token(token)
        if user:
            return user
        raise HTTPException(status_code=401, detail="Invalid token")

    # verify_oauth2_token above skips the `aud` claim when no audience is passed, so without
    # this check any Google-signed id_token with an allow-listed email is accepted, including
    # one issued to an unrelated application. Inert until GOOGLE_TOKEN_AUDIENCE is set.
    if config.google_token_audience:
        if payload.get("aud") not in config.google_token_audience:
            logger.warning(f"token audience not allowed: {payload.get('aud')}")
            raise HTTPException(status_code=401, detail="Token audience not allowed")
    else:
        logger.warning(
            "GOOGLE_TOKEN_AUDIENCE is not set: accepting a Google id_token without verifying "
            "it was issued for this service"
        )

    email = payload.get("email")
    if not email:
        raise HTTPException(status_code=401, detail="Token does not contain email")

    if not payload.get("email_verified", False):
        raise HTTPException(status_code=401, detail="Email not verified")

    # domain restriction
    if not _email_allowed(email):
        raise HTTPException(status_code=403, detail="Email domain not allowed")

    return email


def get_verified_user(request: Request) -> str | None:
    """Resolve the caller's identity from the bearer token and/or the trusted-proxy header.

    Precedence, in the order decided here:

    1. **internal marker + allow-listed identity header** -> that email. The marker only says
       "an in-cluster proxy sent this"; when that proxy also asserts *whose* request it is
       relaying, the asserted person is the caller. Checking the bearer first would collapse
       every browser request to the generic ``mcp-tool`` and lose the real user for both
       authorization and the ``endpoint_access`` log, which defeats the point of the marker.
    2. **internal marker + identity header that is not allow-listed** -> ``None`` (401).
       Deliberately *not* downgraded to ``mcp-tool``: a downgrade would let anything holding
       the shared secret launder an unauthorized identity into a working request, i.e. the
       weaker credential would silently rescue a request the stronger claim just failed.
       Rejecting keeps the assertion binding on the proxy that made it.
    3. **internal marker alone** -> ``mcp-tool`` (auth-gateway's ``@api_bearer`` location,
       chat-backend, mcp-server; unchanged).
    4. **Google Identity Token / user API token** -> that identity, or an HTTPException from
       ``get_bearer_token_user`` when the token is present but invalid (unchanged).
    5. **identity header alone, no marker** -> ``None`` (401). This is the hole being closed:
       the header is settable by anything with network reach to port 4000.
    """
    auth_header = request.headers.get("Authorization")
    if is_internal_caller(auth_header):
        # cases 1, 2 and 3 — an asserted identity, once present, decides the outcome either way
        if request.headers.get("X-Goog-Authenticated-User-Email"):
            return get_authenticated_user(request)
        return "mcp-tool"

    email = get_bearer_token_user(request)  # case 4
    if email is None:
        email = get_authenticated_user(request)  # case 5, always None without the marker
    return email
