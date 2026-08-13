"""Unit tests for the per-execution sandbox token path in results-api's auth.

Design of record: docs/code-execution-security.md section 4 in genetics-results-suite.

results-api is the harder of the two validators because it already has other JWT callers.
The properties under test are: the sandbox token is discriminated on the JOSE ``alg`` header
rather than on dots (so Google Identity Tokens keep working), it is checked *before* the
shared-secret comparison and never falls through to it or to ``verify_oauth2_token``, and it
fails closed when the signing key is unset.
"""

import base64
import json
import time

import jwt
import pytest
from fastapi import HTTPException, Request

import app.config.common as config
from app.core import auth, sandbox_token

INTERNAL_SECRET = "test-internal-secret"
SIGNING_KEY = "test-sandbox-signing-key-that-is-32-bytes+"


def _request(headers: dict[str, str]) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/v1/gene/GPT",
            "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        }
    )


def _bearer(token: str) -> Request:
    return _request({"Authorization": f"Bearer {token}"})


def _mint(key=SIGNING_KEY, audience="results-api", issuer="chat-backend", age=0, ttl=300, **over):
    iat = int(time.time()) - age
    claims = {
        "iss": issuer,
        "aud": audience,
        "sub": "user@finngen.fi",
        "sid": "session-abc",
        "jti": "exec-123",
        "iat": iat,
        "exp": iat + ttl,
        "scope": "query:views",
    }
    for k, v in over.items():
        if v is None:
            claims.pop(k, None)
        else:
            claims[k] = v
    return jwt.encode(claims, key, algorithm="HS256")


@pytest.fixture(autouse=True)
def sandbox_config(monkeypatch):
    monkeypatch.setattr(config, "internal_api_secret", INTERNAL_SECRET)
    monkeypatch.setattr(config, "sandbox_token_signing_key", SIGNING_KEY)
    monkeypatch.setattr(config, "allowed_email_domains", {"finngen.fi"})
    monkeypatch.setattr(config, "allowed_emails", set())


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


def test_expired_token_is_rejected():
    with pytest.raises(HTTPException) as exc:
        auth.get_verified_user(_bearer(_mint(age=400, ttl=300)))
    assert exc.value.status_code == 401


def test_backdated_iat_is_rejected():
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(_mint(age=3600, ttl=7200)))


@pytest.mark.parametrize("claim", ["iss", "aud", "sub", "sid", "jti", "iat", "exp", "scope"])
def test_every_required_claim_is_required(claim):
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(_mint(**{claim: None})))


@pytest.mark.parametrize("claim", ["sub", "sid", "jti"])
def test_empty_attribution_claim_is_rejected(claim):
    """`options={"require": ...}` only rejects missing/null; an empty string yields a principal
    attributing the query to nobody, which defeats the point of the credential."""
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(_mint(**{claim: ""})))


def test_multi_audience_token_is_rejected():
    """PyJWT reads a list `aud` as membership, so this would otherwise validate at BOTH
    services and one-token-per-audience would be a minter property, not a validator one."""
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(_mint(audience=["db-api", "results-api"])))


def test_single_element_list_audience_is_also_rejected():
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(_mint(audience=["results-api"])))


def test_freshly_minted_token_from_a_slightly_fast_minter_is_accepted():
    """iat 3s in the future: PyJWT >= 2.10 raises ImmatureSignatureError with leeway=0, and the
    minter and this service are different pods with different clocks."""
    assert auth.get_verified_user(_bearer(_mint(age=-3))) == "sandbox:user@finngen.fi"


def test_leeway_does_not_widen_the_300s_iat_bound():
    """The MAX_TOKEN_AGE_SECONDS check is separate from the decoder and stays exact."""
    assert auth.get_verified_user(_bearer(_mint(age=299, ttl=600))) == "sandbox:user@finngen.fi"
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(_mint(age=301, ttl=600)))


def test_db_api_token_cannot_be_replayed_at_results_api():
    """Audience binding: same key, same claims, minted for the other service."""
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(_mint(audience="db-api")))


def test_foreign_issuer_is_rejected():
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(_mint(issuer="someone-else")))


def test_wrong_signing_key_is_rejected():
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(_mint(key="a-different-key-also-32-bytes-long!")))


def test_tampered_payload_is_rejected():
    header, payload, signature = _mint().split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    claims["sub"] = "attacker@example.com"
    forged = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    with pytest.raises(HTTPException):
        auth.get_verified_user(_bearer(f"{header}.{forged}.{signature}"))


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
