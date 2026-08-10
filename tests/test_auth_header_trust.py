"""Unit tests for the X-Goog-Authenticated-User-Email trust rule.

The header is settable by anything that can reach results-api on the pod network, so it must
only be honoured when the caller also presents the internal shared secret. These tests run
against the auth helpers directly — no live server needed.
"""

import pytest
from fastapi import Request

import app.config.common as config
from app.core import auth
from app.middleware_usage_logging import _extract_user_from_header

INTERNAL_SECRET = "test-internal-secret"
USER_HEADER = "X-Goog-Authenticated-User-Email"


def _request(headers: dict[str, str]) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/v1/gene/GPT",
            "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        }
    )


def _scope(headers: dict[str, str]) -> dict:
    return {
        "type": "http",
        "method": "GET",
        "path": "/api/v1/gene/GPT",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
    }


@pytest.fixture(autouse=True)
def allowlist(monkeypatch):
    """Pin the secret and allow-list so the tests do not depend on the deployed environment."""
    monkeypatch.setattr(config, "internal_api_secret", INTERNAL_SECRET)
    monkeypatch.setattr(config, "allowed_email_domains", {"finngen.fi"})
    monkeypatch.setattr(config, "allowed_emails", {"guest@example.org"})


# ---------------------------------------------------------------------------
# the vulnerability: header alone must not authenticate
# ---------------------------------------------------------------------------


def test_header_without_internal_secret_is_ignored():
    req = _request({USER_HEADER: "accounts.google.com:anyone@finngen.fi"})
    assert auth.get_authenticated_user(req) is None
    assert auth.get_verified_user(req) is None


def test_header_with_arbitrary_string_is_ignored():
    req = _request({USER_HEADER: "attacker"})
    assert auth.get_authenticated_user(req) is None


def test_header_with_wrong_bearer_is_ignored():
    req = _request(
        {
            USER_HEADER: "accounts.google.com:anyone@finngen.fi",
            "Authorization": "Bearer not-the-secret",
        }
    )
    # a non-JWT, non-matching token is a user API token; with no chat backend configured it is
    # rejected outright rather than falling through to the header
    with pytest.raises(Exception):
        auth.get_verified_user(req)
    assert auth.get_authenticated_user(req) is None


def test_header_ignored_when_no_internal_secret_configured(monkeypatch):
    """Fail closed rather than open when the deployment has no secret to compare against."""
    monkeypatch.setattr(config, "internal_api_secret", "")
    req = _request(
        {
            USER_HEADER: "accounts.google.com:anyone@finngen.fi",
            "Authorization": f"Bearer {INTERNAL_SECRET}",
        }
    )
    assert auth.get_authenticated_user(req) is None


# ---------------------------------------------------------------------------
# the allow-list now applies to the proxied identity too
# ---------------------------------------------------------------------------


def test_proxied_identity_outside_allowlist_is_rejected():
    req = _request(
        {
            USER_HEADER: "accounts.google.com:someone@evil.example",
            "Authorization": f"Bearer {INTERNAL_SECRET}",
        }
    )
    assert auth.get_authenticated_user(req) is None


def test_proxied_identity_in_allowed_domain_is_accepted():
    req = _request(
        {
            USER_HEADER: "accounts.google.com:user@finngen.fi",
            "Authorization": f"Bearer {INTERNAL_SECRET}",
        }
    )
    assert auth.get_authenticated_user(req) == "user@finngen.fi"


def test_proxied_identity_in_allowed_emails_is_accepted():
    req = _request(
        {
            USER_HEADER: "accounts.google.com:guest@example.org",
            "Authorization": f"Bearer {INTERNAL_SECRET}",
        }
    )
    assert auth.get_authenticated_user(req) == "guest@example.org"


def test_header_without_provider_prefix_still_parses():
    req = _request(
        {USER_HEADER: "user@finngen.fi", "Authorization": f"Bearer {INTERNAL_SECRET}"}
    )
    assert auth.get_authenticated_user(req) == "user@finngen.fi"


# ---------------------------------------------------------------------------
# working paths must keep working
# ---------------------------------------------------------------------------


def test_internal_bearer_alone_still_authenticates_as_mcp_tool():
    """auth-gateway's @api_bearer location and mcp-server/chat-backend calls."""
    req = _request({"Authorization": f"Bearer {INTERNAL_SECRET}"})
    assert auth.get_verified_user(req) == "mcp-tool"


def test_no_credentials_at_all_is_unauthenticated():
    assert auth.get_verified_user(_request({})) is None


# ---------------------------------------------------------------------------
# get_verified_user precedence — the four cases the trusted-proxy design defines
# ---------------------------------------------------------------------------


def test_precedence_marker_plus_allowlisted_header_yields_that_email():
    """Case 1: the asserted identity wins over the generic service identity."""
    req = _request(
        {
            USER_HEADER: "accounts.google.com:user@finngen.fi",
            "Authorization": f"Bearer {INTERNAL_SECRET}",
        }
    )
    assert auth.get_verified_user(req) == "user@finngen.fi"


def test_precedence_marker_plus_non_allowlisted_header_is_rejected():
    """Case 2: rejected, never downgraded to mcp-tool.

    A downgrade would let anything holding the shared secret launder an identity the allow-list
    just refused into a working, service-attributed request.
    """
    req = _request(
        {
            USER_HEADER: "accounts.google.com:outsider@evil.example",
            "Authorization": f"Bearer {INTERNAL_SECRET}",
        }
    )
    assert auth.get_verified_user(req) is None


def test_precedence_marker_alone_is_still_mcp_tool():
    """Case 3: auth-gateway @api_bearer, chat-backend, mcp-server — unchanged."""
    req = _request({"Authorization": f"Bearer {INTERNAL_SECRET}"})
    assert auth.get_verified_user(req) == "mcp-tool"


def test_precedence_header_alone_is_unauthenticated():
    """Case 5: the hole being closed."""
    req = _request({USER_HEADER: "accounts.google.com:user@finngen.fi"})
    assert auth.get_verified_user(req) is None


def test_precedence_marker_with_empty_header_value_is_mcp_tool():
    """A literally empty header is no assertion, so the service identity still applies.

    Not reachable through auth-gateway: nginx drops a header whose value is the empty string,
    so `proxy_set_header X-Goog-Authenticated-User-Email "accounts.google.com:$email"` can
    never produce one — see the prefix-only test below for what an empty $email really sends.
    """
    req = _request({USER_HEADER: "", "Authorization": f"Bearer {INTERNAL_SECRET}"})
    assert auth.get_verified_user(req) == "mcp-tool"


def test_marker_with_prefix_only_header_is_rejected():
    """What an empty oauth2-proxy $email actually puts on the wire: the bare legacy prefix.

    It is truthy, so it is an assertion of the empty address, which is not allow-listed -> 401.
    Unreachable in production because oauth2-proxy cannot return 200 for /oauth2/auth without
    an email (its own domain check needs one), but it is case 2, not case 3.
    """
    req = _request(
        {USER_HEADER: "accounts.google.com:", "Authorization": f"Bearer {INTERNAL_SECRET}"}
    )
    assert auth.get_verified_user(req) is None


@pytest.mark.parametrize(
    "asserted",
    ["accounts.google.com:User@FinnGen.fi", "accounts.google.com:  mervi.aavikko@finngen.fi  "],
)
def test_identity_is_returned_normalized(asserted):
    """Case-insensitive, whitespace-tolerant matching must not leak spellings downstream.

    endpoint_access attributes by this string, so an un-normalized return splits one person
    across several identities. Both resolvers must agree on the normalized form.
    """
    headers = {USER_HEADER: asserted, "Authorization": f"Bearer {INTERNAL_SECRET}"}
    expected = asserted.split(":")[-1].strip().lower()
    assert auth.get_verified_user(_request(headers)) == expected
    assert _extract_user_from_header(_scope(headers)) == expected


# ---------------------------------------------------------------------------
# the lockout the two validators found: the real bff-passthrough request shape
# ---------------------------------------------------------------------------

# paths named users hit in the last two weeks, none of them @is_public
LOCKED_OUT_PATHS = [
    "/api/v1/resources",
    "/api/v1/resource_metadata/finngen_R12",
    "/api/v1/search/height",
    "/api/v1/credible_sets_by_region/1:1000000-2000000",
    "/api/v1/summary_stats/finngen_R12/gwas",
]
REAL_USERS = [
    "vivaswat.shastry@finngen.fi",
    "masahiro.kanai@finngen.fi",
    "mervi.aavikko@finngen.fi",
]


def _bff_passthrough_request(path: str, email: str, api_token: str) -> Request:
    """Exactly what bff/passthrough.ts now puts on the wire.

    pickForwardHeaders copies the browser hop's headers verbatim minus hop-by-hop (so the
    oauth2-proxy identity header survives) and then adds the internal bearer. nginx diverts
    anything already carrying Authorization to @api_bearer, so a browser request never brings
    its own — cookie and user-agent are along for the ride.
    """
    headers = {
        USER_HEADER: f"accounts.google.com:{email}",
        "cookie": "_oauth2_proxy=abcdef",
        "user-agent": "Mozilla/5.0",
        "accept": "application/json",
    }
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        }
    )


@pytest.mark.parametrize("path", LOCKED_OUT_PATHS)
@pytest.mark.parametrize("email", REAL_USERS)
def test_real_browser_traffic_authenticates_through_the_bff(path, email):
    req = _bff_passthrough_request(path, email, INTERNAL_SECRET)
    assert auth.get_verified_user(req) == email
    # and the usage log attributes the person, not the proxy
    scope = dict(req.scope)
    assert _extract_user_from_header(scope) == email


@pytest.mark.parametrize("path", LOCKED_OUT_PATHS)
def test_same_traffic_without_the_bff_bearer_is_the_lockout(path):
    """Regression guard: this is the pre-fix bff shape, and it must stay a 401 (None).

    It is the reason the browser half has to ship first — see the deployment ordering note in
    the suite's docs/project-spec.md.
    """
    req = _bff_passthrough_request(path, REAL_USERS[0], "")
    assert auth.get_verified_user(req) is None


# ---------------------------------------------------------------------------
# allow-list comparison rules
# ---------------------------------------------------------------------------


def test_allowlist_is_case_insensitive():
    """oauth2-proxy lower-cases before its domain check; a mixed-case address it admits
    must not be rejected here."""
    req = _request(
        {
            USER_HEADER: "accounts.google.com:User@FinnGen.fi",
            "Authorization": f"Bearer {INTERNAL_SECRET}",
        }
    )
    # admitted, and returned in the normalized form (see test_identity_is_returned_normalized)
    assert auth.get_verified_user(req) == "user@finngen.fi"
    assert auth._email_allowed("GUEST@Example.ORG") is True


def test_allowlist_case_insensitive_on_the_configured_side(monkeypatch):
    monkeypatch.setattr(config, "allowed_email_domains", {"FinnGen.FI"})
    monkeypatch.setattr(config, "allowed_emails", {"Guest@Example.org"})
    assert auth._email_allowed("user@finngen.fi") is True
    assert auth._email_allowed("guest@example.org") is True


def test_wildcard_domain_allows_everything(monkeypatch):
    """oauth2-proxy reads "*" as allow-all; matching it here avoids a total lockout of a
    deployment configured with oauth_email_domain = "*"."""
    monkeypatch.setattr(config, "allowed_email_domains", {"*"})
    assert auth._email_allowed("anyone@anywhere.example") is True


def test_non_ascii_bearer_does_not_raise():
    """compare_digest on str raises TypeError for non-ASCII, and is_internal_caller runs in
    ASGI middleware before routing — that would be a 500 instead of a clean 401."""
    assert auth.is_internal_caller("Bearer pässwörd") is False
    scope = _scope({USER_HEADER: "user@finngen.fi", "Authorization": "Bearer pässwörd"})
    assert _extract_user_from_header(scope) is None


# ---------------------------------------------------------------------------
# usage-log attribution must not be forgeable either
# ---------------------------------------------------------------------------


def test_usage_log_applies_the_allowlist_too():
    """docs claim the identical rule to get_authenticated_user, so an identity the auth path
    refuses must not be attributed a request in the endpoint_access log either."""
    scope = _scope(
        {
            USER_HEADER: "accounts.google.com:outsider@evil.example",
            "Authorization": f"Bearer {INTERNAL_SECRET}",
        }
    )
    assert _extract_user_from_header(scope) is None


def test_usage_log_ignores_untrusted_header():
    assert _extract_user_from_header(_scope({USER_HEADER: "victim@finngen.fi"})) is None


def test_usage_log_keeps_trusted_header():
    scope = _scope(
        {
            USER_HEADER: "accounts.google.com:user@finngen.fi",
            "Authorization": f"Bearer {INTERNAL_SECRET}",
        }
    )
    assert _extract_user_from_header(scope) == "user@finngen.fi"


def test_is_internal_caller_rejects_malformed_authorization():
    assert auth.is_internal_caller(None) is False
    assert auth.is_internal_caller("") is False
    assert auth.is_internal_caller(INTERNAL_SECRET) is False
    assert auth.is_internal_caller(f"Basic {INTERNAL_SECRET}") is False
    assert auth.is_internal_caller(f"Bearer {INTERNAL_SECRET}") is True
