"""Unit tests for the per-execution sandbox token path in results-api's auth.

Design of record: docs/code-execution-security.md section 4 in genetics-results-suite.

results-api is the harder of the two validators because it already has other JWT callers.
The properties under test are: the sandbox token is discriminated on the JOSE ``alg`` header
rather than on dots (so Google Identity Tokens keep working), it is checked *before* the
shared-secret comparison and never falls through to it or to ``verify_oauth2_token``, and it
fails closed when the signing key is unset.

What both validators must refuse is tests/sandbox_contract.py's `TokenContract`, collected here
as `TestTokenContract` and driven through `get_verified_user`.
"""

import base64
import logging

import jwt
import pytest
from fastapi import HTTPException, Request
from helpers.asgi import request_with_headers as _request
from helpers.sandbox import INTERNAL_SECRET, configure_credentials
from helpers.sandbox import mint as _mint
from sandbox_contract import SIGNING_KEY, TokenContract

import app.config.common as config
from app.core import auth, sandbox_token


def _bearer(token: str) -> Request:
    return _request({"Authorization": f"Bearer {token}"})


@pytest.fixture(autouse=True)
def sandbox_config(monkeypatch):
    configure_credentials(monkeypatch)


# --- happy path -----------------------------------------------------------------------------


def test_valid_token_authenticates_as_a_sandbox_principal():
    identity = auth.get_verified_user(_bearer(_mint()))
    assert identity == "sandbox:user@finngen.fi"


def test_principal_carries_the_session_and_execution_ids():
    """Attribution to a conversation is the point of `sid`; `jti` joins the three log streams."""
    principal = auth.get_sandbox_principal(_bearer(_mint()))
    assert (principal.session_id, principal.execution_id) == ("session-abc", "exec-123")


def test_principal_is_left_on_request_state_for_the_usage_log():
    req = _bearer(_mint())
    auth.get_verified_user(req)
    assert req.state.sandbox_principal.execution_id == "exec-123"


# --- expiry, claims, audience, signature ------------------------------------------------------


@pytest.fixture
def accepts():
    """Whether the bearer authenticated as the sandbox principal; a refusal must be a 401."""

    def accepts(token):
        try:
            identity = auth.get_verified_user(_bearer(token))
        except HTTPException as exc:
            assert exc.status_code == 401
            return False
        assert identity == "sandbox:user@finngen.fi"
        return True

    return accepts


class TestTokenContract(TokenContract):
    audience = "results-api"
    other_audience = "db-api"


# fetches Google's OAuth signing certs, so this one needs the network
@pytest.mark.integration
def test_alg_none_is_not_accepted():
    unsigned = jwt.encode({"iss": "chat-backend"}, key="", algorithm="none")
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(unsigned))


# --- fail closed ------------------------------------------------------------------------------


def test_unset_signing_key_rejects_every_sandbox_token(monkeypatch):
    """No warning-and-continue: the sandbox is the one caller running attacker-authored code."""
    monkeypatch.setattr(config, "sandbox_token_signing_key", "")
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(_mint()))


def test_sandbox_token_never_falls_through_to_the_shared_secret(monkeypatch):
    """An HS256 bearer must not be compared against INTERNAL_API_SECRET, even if it equals it."""
    bad = _mint(key="a-different-key-also-32-bytes-long!")
    monkeypatch.setattr(config, "internal_api_secret", bad)
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(bad))


def test_sandbox_caller_cannot_assert_an_identity_header():
    """It holds no shared secret, so the trusted-proxy marker is absent and case 5 discards it."""
    req = _request({
        "Authorization": f"Bearer {_mint()}",
        "X-Goog-Authenticated-User-Email": "accounts.google.com:someone.else@finngen.fi",
    })
    assert auth.get_verified_user(req) == "sandbox:user@finngen.fi"


def test_require_sandbox_config_exits_when_a_secret_is_missing(monkeypatch):
    monkeypatch.setattr(config, "sandbox_enabled", True)
    monkeypatch.setattr(config, "sandbox_token_signing_key", "")
    with pytest.raises(SystemExit) as exc:
        sandbox_token.require_sandbox_config()
    assert exc.value.code == 1


def test_require_sandbox_config_exits_when_both_secrets_are_missing(monkeypatch):
    """The both-unset case: the sandbox could otherwise send no Authorization header at all."""
    monkeypatch.setattr(config, "sandbox_enabled", True)
    monkeypatch.setattr(config, "internal_api_secret", "")
    monkeypatch.setattr(config, "sandbox_token_signing_key", "")
    with pytest.raises(SystemExit):
        sandbox_token.require_sandbox_config()


def test_require_sandbox_config_is_inert_while_the_sandbox_is_not_deployed(monkeypatch):
    monkeypatch.setattr(config, "sandbox_enabled", False)
    monkeypatch.setattr(config, "internal_api_secret", "")
    monkeypatch.setattr(config, "sandbox_token_signing_key", "")
    sandbox_token.require_sandbox_config()


# --- the discriminator: alg, never dots --------------------------------------------------------


def test_rs256_bearer_is_not_sandbox_shaped():
    """Google Identity Tokens are three-segment JWTs; routing on dots would 401 all of them."""
    header = base64.urlsafe_b64encode(b'{"alg":"RS256","typ":"JWT"}').decode().rstrip("=")
    assert sandbox_token.is_sandbox_shaped(f"{header}.payload.signature") is False


@pytest.mark.parametrize("bearer", ["", "opaque-user-token", "a.b", "a.b.c.d", "!!!.!!!.!!!"])
def test_non_jwt_bearers_are_not_sandbox_shaped(bearer):
    assert sandbox_token.is_sandbox_shaped(bearer) is False


def test_google_token_path_is_untouched(monkeypatch):
    """An RS256 bearer still reaches verify_oauth2_token — the sandbox branch does not shadow it."""
    called = {}

    class _FakeIdToken:
        @staticmethod
        def verify_oauth2_token(token, request):
            called["token"] = token
            return {"aud": "x", "email": "user@finngen.fi", "email_verified": True}

    import sys
    import types

    # get_bearer_token_user imports google.oauth2.id_token lazily, inside the branch
    monkeypatch.setitem(sys.modules, "google.oauth2", types.SimpleNamespace(id_token=_FakeIdToken))
    monkeypatch.setattr(config, "google_token_audience", set())

    header = base64.urlsafe_b64encode(b'{"alg":"RS256","typ":"JWT"}').decode().rstrip("=")
    token = f"{header}.eyJhIjoxfQ.sig"
    assert auth.get_verified_user(_bearer(token)) == "user@finngen.fi"
    assert called["token"] == token


# --- pre-existing credential types still work ---------------------------------------------------


def test_internal_secret_still_authenticates():
    assert auth.get_verified_user(_bearer(INTERNAL_SECRET)) == "mcp-tool"


def test_internal_marker_plus_identity_header_still_resolves_the_user():
    req = _request({
        "Authorization": f"Bearer {INTERNAL_SECRET}",
        "X-Goog-Authenticated-User-Email": "accounts.google.com:user@finngen.fi",
    })
    assert auth.get_verified_user(req) == "user@finngen.fi"


def test_no_credentials_is_still_unauthenticated():
    assert auth.get_verified_user(_request({})) is None


# --- the signing key must be long enough to be a key (genetics-results-suite-4h6.36) -------


@pytest.mark.parametrize("weak", ["   ", "\n", "x", "0", "a" * 31])
def test_require_sandbox_config_refuses_a_blank_ish_or_short_signing_key(monkeypatch, weak):
    """Every one of these passed the truthiness check and became a guessable HMAC key that
    mints valid sandbox principals. 31 bytes is the boundary: PyJWT warns below 32."""
    monkeypatch.setattr(config, "sandbox_enabled", True)
    monkeypatch.setattr(config, "internal_api_secret", INTERNAL_SECRET)
    monkeypatch.setattr(config, "sandbox_token_signing_key", weak)
    with pytest.raises(SystemExit) as exc:
        sandbox_token.require_sandbox_config()
    assert exc.value.code == 1


def test_require_sandbox_config_accepts_a_real_generated_key(monkeypatch):
    """`openssl rand -base64 32`, what scripts/create-secrets.sh produces: 44 chars."""
    key = base64.b64encode(b"\x11" * 32).decode()
    assert len(key) == 44
    monkeypatch.setattr(config, "sandbox_enabled", True)
    monkeypatch.setattr(config, "internal_api_secret", INTERNAL_SECRET)
    monkeypatch.setattr(config, "sandbox_token_signing_key", key)
    sandbox_token.require_sandbox_config()  # must not raise


def test_the_gate_does_not_normalise_the_key_that_reaches_jwt_decode(monkeypatch, caplog):
    """The property the obvious `.strip()` fix would have broken. chat-backend mints with its
    own copy of the secret, so a key deployed with a trailing newline signs as "key\\n"; if the
    gate normalised it, every legitimate token would 401. The gate warns and changes nothing."""
    key = SIGNING_KEY + "\n"
    monkeypatch.setattr(config, "sandbox_enabled", True)
    monkeypatch.setattr(config, "internal_api_secret", INTERNAL_SECRET)
    monkeypatch.setattr(config, "sandbox_token_signing_key", key)
    with caplog.at_level(logging.WARNING, logger="app.core.sandbox_token"):
        sandbox_token.require_sandbox_config()  # must not raise
    assert any("whitespace" in r.message for r in caplog.records)

    assert config.sandbox_token_signing_key == key, "the gate mutated the key it verifies with"

    principal = sandbox_token.verify_sandbox_token(_mint(key=key))
    assert principal.user == "user@finngen.fi"
    with pytest.raises(sandbox_token.SandboxTokenError):
        sandbox_token.verify_sandbox_token(_mint(key=key.strip()))
