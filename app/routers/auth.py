import logging
import secrets
from urllib.parse import urlparse
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from app.dependencies import is_public
from app.core.auth import google_auth, verify_membership
import app.config.common as config

logger = logging.getLogger(__name__)

# allowed hosts for OAuth redirect and frontend URLs
ALLOWED_HOSTS = {urlparse(origin).netloc for origin in config.cors_origins}


def _validate_host(host: str) -> bool:
    """Check if the host is in the allowed hosts list."""
    return host in ALLOWED_HOSTS


def _validate_redirect_url(url: str | None) -> str | None:
    """
    Validate and sanitize a redirect URL.
    Returns the URL if valid, "/" if invalid or None.
    """
    if not url:
        return "/"

    try:
        parsed = urlparse(url)
        # allow relative URLs starting with /
        if not parsed.scheme and not parsed.netloc and url.startswith("/"):
            return url
        # for absolute URLs, check if host is allowed
        if parsed.netloc and parsed.netloc in ALLOWED_HOSTS:
            return url
    except Exception:
        pass

    return "/"

router = APIRouter()


@router.get(
    "/login",
    include_in_schema=False,
    responses={
        303: {"description": "Redirect to Google OAuth authorization URL"},
        400: {"description": "Invalid host"},
        500: {"description": "Internal server error"},
    },
)
@is_public
async def login(request: Request, frontend_url: str | None = None):
    # validate and sanitize frontend_url to prevent open redirect
    safe_frontend_url = _validate_redirect_url(frontend_url)
    request.session["next"] = safe_frontend_url
    request.session["frontend_url"] = safe_frontend_url

    # validate host header to prevent redirect URI injection
    host = request.headers.get("x-forwarded-host") or request.headers.get("host", "")
    if not _validate_host(host):
        raise HTTPException(status_code=400, detail="Invalid host")

    scheme = request.headers.get("x-forwarded-proto", "https")
    base_url = f"{scheme}://{host}"
    redirect_uri = f"{base_url}/api/v1/callback/google"

    # generate OAuth state parameter to prevent CSRF
    state = secrets.token_urlsafe(32)
    request.session["oauth_state"] = state

    authorization_url = google_auth.get_authorization_url(redirect_uri, state)

    return RedirectResponse(authorization_url, status_code=303)


@router.get(
    "/callback/google",
    include_in_schema=False,
    responses={
        303: {"description": "Redirect to frontend after successful authentication"},
        400: {"description": "Authentication failed or invalid state"},
        403: {"description": "Unauthorized email address"},
        500: {"description": "Internal server error"},
    },
)
@is_public
async def oauth_callback_google(
    request: Request, code: str, state: str | None = None
):
    # validate OAuth state parameter to prevent CSRF
    expected_state = request.session.get("oauth_state")
    if not expected_state or state != expected_state:
        raise HTTPException(status_code=400, detail="Invalid state parameter")
    # clear state after use
    request.session.pop("oauth_state", None)

    # validate host header
    host = request.headers.get("x-forwarded-host") or request.headers.get("host", "")
    if not _validate_host(host):
        raise HTTPException(status_code=400, detail="Invalid host")

    scheme = request.headers.get("x-forwarded-proto", "https")
    base_url = f"{scheme}://{host}"
    redirect_uri = f"{base_url}/api/v1/callback/google"

    try:
        user_info = await google_auth.get_user_info(code, redirect_uri)
        email = user_info["email"]

        if not verify_membership(email):
            logger.warning(f"Unauthorized email: {email}")
            raise HTTPException(status_code=403, detail="Unauthorized email address")

        request.session["user_email"] = email
        request.session["authenticated"] = True

        # use already-validated frontend_url from session
        frontend_url = request.session.get("frontend_url", "/")

        response = RedirectResponse(frontend_url, status_code=303)

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        raise HTTPException(status_code=400, detail="Authentication failed")


@router.get(
    "/logout",
    include_in_schema=False,
    responses={
        200: {
            "description": "Successful logout",
            "content": {"application/json": {"schema": {"type": "object"}}},
        },
        500: {"description": "Internal server error"},
    },
)
@is_public
async def logout(request: Request):
    request.session.clear()
    response = JSONResponse({"status": "ok"})

    response.delete_cookie(
        "session", path="/", domain=None, secure=True, httponly=True, samesite="lax"
    )

    return response


@router.get(
    "/auth",
    include_in_schema=False,
    responses={
        200: {
            "description": "Authentication information",
            "content": {"application/json": {"schema": {"type": "object"}}},
        },
        401: {"description": "Not authenticated"},
        500: {"description": "Internal server error"},
    },
)
async def auth(request: Request):
    # only return authentication status, not full session data
    return JSONResponse(
        {
            "authenticated": request.session.get("authenticated", False),
        }
    )
