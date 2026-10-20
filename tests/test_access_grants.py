"""The runtime-grant fallback behind every allow-list decision (app/core/auth.py).

chat-backend owns the grant table; this service asks /chat/v1/access-grants/check after its
own static list misses, and caches the answer briefly in both directions.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import Request

import app.config.common as config
from app.core import auth
from app.middleware_usage_logging import _extract_user_from_header

INTERNAL_SECRET = "test-internal-secret"
USER_HEADER = "X-Goog-Authenticated-User-Email"
GUEST = "visitor@gmail.com"


def _request(headers: dict[str, str]) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/v1/gene/GPT",
            "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        }
    )


@pytest.fixture(autouse=True)
def deployed(monkeypatch):
    monkeypatch.setattr(config, "internal_api_secret", INTERNAL_SECRET)
    monkeypatch.setattr(config, "allowed_email_domains", {"finngen.fi"})
    monkeypatch.setattr(config, "allowed_emails", set())
    monkeypatch.setattr(config, "chat_backend_url", "http://chat-backend:8000/chat")
    auth._access_grant_cache.clear()
    yield
    auth._access_grant_cache.clear()


def _response(status: int, body: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = body
    return resp


def test_static_users_never_ask_chat_backend():
    with patch("requests.get", side_effect=AssertionError("asked")):
        assert auth.identity_allowed("user@finngen.fi") is True


def test_grant_admits_a_proxied_identity_the_static_list_refuses():
    req = _request({USER_HEADER: f"accounts.google.com:{GUEST}", "Authorization": f"Bearer {INTERNAL_SECRET}"})
    with patch("requests.get", return_value=_response(200, {"allowed": True})) as get:
        assert auth.get_authenticated_user(req) == GUEST
        # the usage log attributes the same request to the same person
        assert _extract_user_from_header(req.scope) == GUEST
    get.assert_called_once()
    _, kwargs = get.call_args
    assert kwargs["params"] == {"email": GUEST}
    assert kwargs["headers"] == {"Authorization": f"Bearer {INTERNAL_SECRET}"}
    assert get.call_args[0][0] == "http://chat-backend:8000/chat/v1/access-grants/check"


def test_no_grant_is_still_refused():
    req = _request({USER_HEADER: f"accounts.google.com:{GUEST}", "Authorization": f"Bearer {INTERNAL_SECRET}"})
    with patch("requests.get", return_value=_response(200, {"allowed": False})):
        assert auth.get_authenticated_user(req) is None


def test_negative_and_positive_answers_are_cached():
    with patch("requests.get", return_value=_response(200, {"allowed": False})) as get:
        assert auth._has_access_grant(GUEST) is False
        assert auth._has_access_grant(GUEST.upper()) is False
    assert get.call_count == 1
    auth._access_grant_cache.clear()
    with patch("requests.get", return_value=_response(200, {"allowed": True})) as get:
        assert auth._has_access_grant(GUEST) is True
        assert auth._has_access_grant(GUEST) is True
    assert get.call_count == 1


def test_failures_answer_false():
    with patch("requests.get", side_effect=OSError("down")):
        assert auth._has_access_grant(GUEST) is False
    auth._access_grant_cache.clear()
    with patch("requests.get", return_value=_response(403, {"detail": "Internal endpoint"})):
        assert auth._has_access_grant(GUEST) is False


def test_without_chat_backend_nothing_is_asked(monkeypatch):
    monkeypatch.setattr(config, "chat_backend_url", "")
    with patch("requests.get", side_effect=AssertionError("asked")):
        assert auth._has_access_grant(GUEST) is False
