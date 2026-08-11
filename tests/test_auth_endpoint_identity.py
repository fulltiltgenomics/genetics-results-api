"""Pins GET /api/v1/auth against being an unauthenticated identity oracle.

The route is `@is_public`, so `auth_required` returns before any credential check and the
handler runs for a caller with no credential at all — by design, it is how the browser asks
"am I signed in?". The danger is the shape genetics-results-suite-th2 found on chat-backend's
`GET /chat/v1/auth`: reflecting back whatever `X-Goog-Authenticated-User-Email` the caller
sent, turning a public route into an oracle for anything with pod-network reach.

These four cases were measured against the full app (real middleware stack, and again over a
real socket) under genetics-results-suite-r0l and all four passed. They are pinned here so the
property is a test rather than a re-measurement next time someone asks.

Runs entirely offline: the router and `auth_required` are mounted the same way
`app/server.py` mounts them, driven through the ASGI interface directly, so nothing here
touches GCS or BigQuery.
"""

import asyncio
import json

import pytest
from fastapi import Depends, FastAPI

import app.config.common as config
from app.dependencies import auth_required
from app.routers import auth as auth_router

INTERNAL_SECRET = "test-internal-secret"
USER_HEADER = "X-Goog-Authenticated-User-Email"


@pytest.fixture(autouse=True)
def allowlist(monkeypatch):
    monkeypatch.setattr(config, "require_auth", True)
    monkeypatch.setattr(config, "internal_api_secret", INTERNAL_SECRET)
    monkeypatch.setattr(config, "allowed_email_domains", {"finngen.fi"})
    monkeypatch.setattr(config, "allowed_emails", set())
    monkeypatch.setattr(config, "sandbox_token_signing_key", "")


@pytest.fixture
def call():
    """Drive GET /api/v1/auth through ASGI, returning (status, parsed body)."""
    app = FastAPI(dependencies=[Depends(auth_required)])
    app.include_router(auth_router.router, prefix="/api/v1")

    def _call(headers: dict[str, str]):
        async def _run():
            messages = []

            async def receive():
                return {"type": "http.request", "body": b"", "more_body": False}

            async def send(message):
                messages.append(message)

            await app(
                {
                    "type": "http",
                    "asgi": {"version": "3.0", "spec_version": "2.3"},
                    "http_version": "1.1",
                    "method": "GET",
                    "scheme": "http",
                    "path": "/api/v1/auth",
                    "raw_path": b"/api/v1/auth",
                    "query_string": b"",
                    "root_path": "",
                    "headers": [
                        (k.lower().encode(), v.encode()) for k, v in headers.items()
                    ],
                    "client": ("10.0.0.9", 55555),
                    "server": ("testserver", 80),
                },
                receive,
                send,
            )
            status = next(
                m["status"] for m in messages if m["type"] == "http.response.start"
            )
            body = b"".join(
                m.get("body", b"") for m in messages if m["type"] == "http.response.body"
            )
            return status, json.loads(body)

        return asyncio.run(_run())

    return _call


def test_route_is_public():
    """The premise: this route really is reachable with no credential."""
    assert getattr(auth_router.auth, "is_public", False) is True


def test_no_credential_is_unauthenticated(call):
    status, body = call({})
    assert status == 200
    assert body == {"authenticated": False, "user": None}


def test_forged_identity_header_alone_is_not_reflected(call):
    """The oracle test. Without the internal marker the header is not a credential, and the
    address must not come back in the response — a caller must not be able to learn from it
    which addresses this deployment would accept."""
    forged = "accounts.google.com:attacker@finngen.fi"
    status, body = call({USER_HEADER: forged})
    assert status == 200
    assert body == {"authenticated": False, "user": None}
    assert "attacker" not in json.dumps(body)


def test_marker_plus_allow_listed_identity_returns_that_identity(call):
    status, body = call(
        {
            "Authorization": f"Bearer {INTERNAL_SECRET}",
            USER_HEADER: "accounts.google.com:Real.User@FinnGen.fi",
        }
    )
    assert status == 200
    assert body == {"authenticated": True, "user": "real.user@finngen.fi"}


def test_marker_plus_non_allow_listed_identity_is_rejected(call):
    status, body = call(
        {
            "Authorization": f"Bearer {INTERNAL_SECRET}",
            USER_HEADER: "accounts.google.com:outsider@example.com",
        }
    )
    assert status == 200
    assert body == {"authenticated": False, "user": None}


def test_marker_alone_reports_unauthenticated(call):
    """An internal caller with no asserted identity is `mcp-tool` to `get_verified_user`, but
    this handler asks `get_authenticated_user`, which only ever answers about the proxied
    *person*. So the service-to-service caller sees authenticated=false here. Pinned as the
    measured behaviour, not asserted as the desirable one — the point is that no identity
    leaks either way."""
    status, body = call({"Authorization": f"Bearer {INTERNAL_SECRET}"})
    assert status == 200
    assert body == {"authenticated": False, "user": None}
