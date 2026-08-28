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


def _allowed(monkeypatch, email: str, domains: set[str]) -> bool:
    monkeypatch.setattr(config, "allowed_email_domains", domains)
    monkeypatch.setattr(config, "allowed_emails", {"guest@example.org"})
    return auth._email_allowed(email)


def test_leading_dot_domain_matches_subdomains(monkeypatch):
    """oauth2-proxy's `--email-domain=.example.com` form (genetics-results-suite-zl2).

    v7.14.3 `isEmailValidWithDomains` (validator.go) accepts on
    `HasSuffix(email, "@"+domain)`, and separately on
    `HasPrefix(domain, ".") && HasSuffix(atoms[len(atoms)-1], domain)` where `atoms[len-1]` is
    the part after the last `@`. Before this, such a deployment got a session at the gateway
    and a 401 here: a logged-in UI in which every call fails."""
    assert _allowed(monkeypatch, "user@sub.example.com", {".example.com"}) is True
    assert _allowed(monkeypatch, "user@deep.sub.example.com", {".example.com"}) is True


def test_leading_dot_domain_does_not_match_the_bare_domain(monkeypatch):
    """The half that is easy to get backwards: "@.example.com" is a suffix of no real address,
    so oauth2-proxy refuses this one and so must we."""
    assert _allowed(monkeypatch, "user@example.com", {".example.com"}) is False


def test_domain_match_is_not_a_careless_suffix_test(monkeypatch):
    """A suffix test against the whole address, or one forgetting the dot, admits these."""
    assert _allowed(monkeypatch, "user@notexample.com", {".example.com"}) is False
    assert _allowed(monkeypatch, "user@evilexample.com", {"example.com"}) is False
    assert _allowed(monkeypatch, "user@other.example", {".example.com"}) is False


def test_exact_and_star_forms_survive_the_leading_dot_branch(monkeypatch):
    """These assertions are duplicated in genetics-mcp-server's tests/test_auth_header_trust.py
    — the two implementations must not drift apart."""
    assert _allowed(monkeypatch, "user@example.com", {"example.com"}) is True
    assert _allowed(monkeypatch, "user@sub.example.com", {"example.com"}) is False
    assert _allowed(monkeypatch, "anyone@anywhere.example", {"*"}) is True
    assert _allowed(monkeypatch, "Guest@Example.ORG", {"example.com"}) is True


def test_star_dot_domain_is_a_synonym_for_the_leading_dot_form(monkeypatch):
    """v7.14.3 strips the star (`domain[1:]`) and runs the leading-dot suffix test, so
    `*.example.com` and `.example.com` decide identically — and neither is allow-all."""
    assert _allowed(monkeypatch, "user@sub.example.com", {"*.example.com"}) is True
    assert _allowed(monkeypatch, "user@deep.sub.example.com", {"*.example.com"}) is True
    assert _allowed(monkeypatch, "user@example.com", {"*.example.com"}) is False
    assert _allowed(monkeypatch, "user@notexample.com", {"*.example.com"}) is False
    # the over-admission this code has never made: a "*."-prefixed entry is not allow-all
    assert _allowed(monkeypatch, "anyone@anywhere.example", {"*.example.com"}) is False
    # while a bare "*" still is
    assert _allowed(monkeypatch, "anyone@anywhere.example", {"*"}) is True


def test_domain_is_taken_from_the_last_at_sign(monkeypatch):
    """Go's `atoms[len(atoms)-1]` is the part after the LAST "@"; str.split("@")[-1] agrees,
    including when there is no local part at all."""
    assert _allowed(monkeypatch, "a@b@example.com", {"example.com"}) is True
    assert _allowed(monkeypatch, "a@b@sub.example.com", {".example.com"}) is True
    assert _allowed(monkeypatch, "a@b@example.com", {".example.com"}) is False
    assert _allowed(monkeypatch, "@sub.example.com", {".example.com"}) is True


def test_degenerate_dot_entries_reduce_to_a_trailing_dot_test(monkeypatch):
    """A configured "." or "*." reduces to HasSuffix(domain_part, "."), which no ordinary
    address satisfies. Pinning what oauth2-proxy does with the value, not endorsing it."""
    assert _allowed(monkeypatch, "user@example.com", {"."}) is False
    assert _allowed(monkeypatch, "user@example.com", {"*."}) is False
    assert _allowed(monkeypatch, "user@example.com.", {"."}) is True
    assert _allowed(monkeypatch, "user@example.com.", {"*."}) is True


def test_address_without_an_at_sign_is_refused(monkeypatch):
    """The one known divergence, recorded in the comment at auth.py's `domain = ...` line:
    oauth2-proxy's last atom is the whole string, so `.com` would admit this at the gateway.
    Here the domain part is "" and it is refused — fail-closed, and unreachable unless an IdP
    emits an address with no "@"."""
    assert _allowed(monkeypatch, "example.com", {".com"}) is False
    assert _allowed(monkeypatch, "example.com", {"com"}) is False


def test_non_ascii_bearer_does_not_raise():
    """compare_digest on str raises TypeError for non-ASCII, and is_internal_caller runs in
    ASGI middleware before routing — that would be a 500 instead of a clean 401."""
    assert auth.is_internal_caller("Bearer pässwörd") is False
    scope = _scope({USER_HEADER: "user@finngen.fi", "Authorization": "Bearer pässwörd"})
    assert _extract_user_from_header(scope) is None
    # outside latin-1 entirely: the byte-exact re-encode must fail closed, not raise
    assert auth.is_internal_caller("Bearer пароль") is False


def test_non_ascii_secret_compares_the_bytes_actually_sent(monkeypatch):
    """Starlette decodes raw header bytes as latin-1, so re-encoding the presented credential
    with utf-8 compared mojibake against the secret. latin-1 undoes that decode exactly — for a
    caller that put utf-8 on the wire, which is what `_request` builds and what aiohttp sends;
    see `test_which_wire_bytes_authenticate_a_non_ascii_secret` for the callers that do not
    agree. The point asserted here is that the usage-logging middleware, which decodes the same
    bytes itself, reaches the same verdict as the auth path."""
    secret = "sécret"
    monkeypatch.setattr(config, "internal_api_secret", secret)
    req = _request({"Authorization": f"Bearer {secret}"})
    assert auth.is_internal_caller(req.headers.get("Authorization")) is True

    scope = _scope(
        {
            USER_HEADER: "accounts.google.com:user@finngen.fi",
            "Authorization": f"Bearer {secret}",
        }
    )
    assert _extract_user_from_header(scope) == "user@finngen.fi"


def test_ascii_secret_is_unaffected():
    """The codecs agree on ASCII, so the deployed shape behaves exactly as before."""
    req = _request({"Authorization": f"Bearer {INTERNAL_SECRET}"})
    assert auth.is_internal_caller(req.headers.get("Authorization")) is True
    assert auth.is_internal_caller(f"Bearer {INTERNAL_SECRET}") is True


def test_which_wire_bytes_authenticate_a_non_ascii_secret(monkeypatch):
    """Pin the accept/reject map over RAW wire bytes, which is what genetics-results-suite-ctq
    actually changed.

    This cannot be written with TestClient: `starlette/testclient.py` does `value.encode()`
    (utf-8) on httpx's already-decoded header str, so latin-1 wire bytes are silently rewritten
    to utf-8 before the app ever sees them and every case below would look like the utf-8 one.
    Hence a hand-built ASGI scope, whose header values go through unmodified.

    The clients disagree about which of these two a non-ASCII secret becomes on the wire —
    node/undici and requests emit the latin-1 form, aiohttp the utf-8 form, httpx neither
    (it refuses outright) — which is why `config.require_ascii_internal_secret` refuses a
    non-ASCII secret at startup and makes this map unreachable in a real deployment. The
    comparison function is still reachable, so the map is pinned here rather than through
    the app.
    """
    monkeypatch.setattr(config, "internal_api_secret", "sécret")

    def _bearer(raw: bytes) -> str:
        req = Request(
            {
                "type": "http",
                "method": "GET",
                "path": "/api/v1/gene/GPT",
                "headers": [(b"authorization", raw)],
            }
        )
        return req.headers["Authorization"]

    # utf-8 on the wire (aiohttp-shaped): accepted now, rejected before ctq
    assert auth.is_internal_caller(_bearer(b"Bearer s\xc3\xa9cret")) is True
    # latin-1 on the wire (node/undici- and requests-shaped, i.e. the BFF): rejected now,
    # accepted before ctq — this is the flip, stated plainly rather than hidden
    assert auth.is_internal_caller(_bearer(b"Bearer s\xe9cret")) is False


def test_the_ascii_guard_fires_only_on_a_non_ascii_secret():
    """genetics-results-suite-ctq: the invariant the comparison relies on is enforced, not
    merely documented. Absent and empty are the dev/test configuration and must stay silent."""
    with pytest.raises(RuntimeError, match="INTERNAL_API_SECRET"):
        config.require_ascii_internal_secret("sécret")
    config.require_ascii_internal_secret(INTERNAL_SECRET)
    config.require_ascii_internal_secret("")


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


# ---------------------------------------------------------------------------
# the `*` divergence: allow-all on the proxied path, refused on the bearer path
# (genetics-results-suite-g8i). Mirrors genetics-mcp-server's tests of the same name.
# ---------------------------------------------------------------------------


def _bearer_request(token: str) -> Request:
    return _request({"Authorization": f"Bearer {token}"})


@pytest.fixture
def google_id_token(monkeypatch):
    """Make `get_bearer_token_user` accept a fixed payload for the token "a.b.c".

    Patches the real verifier, so everything between it and the allow-list check — the
    audience branch, `email_verified`, the missing-email guard — still runs.
    """
    from google.oauth2 import id_token

    def _verify(token, request, *args, **kwargs):
        if token != "a.b.c":
            raise ValueError("unexpected token")
        return {"email": _verify.email, "email_verified": True, "aud": "any"}

    _verify.email = "anyone@anywhere.example"
    monkeypatch.setattr(id_token, "verify_oauth2_token", _verify)
    # unset audience is the deployed-inert case and the one that makes the allow-list the whole
    # of the authorization; keep it so, or the test would not be testing the exposed shape
    monkeypatch.setattr(config, "google_token_audience", set())
    return _verify


def _bearer_email_accepted(monkeypatch, google_id_token, email: str, domains: set[str]) -> bool:
    from fastapi import HTTPException

    google_id_token.email = email
    monkeypatch.setattr(config, "allowed_email_domains", domains)
    try:
        return auth.get_bearer_token_user(_bearer_request("a.b.c")) == email
    except HTTPException as exc:
        assert exc.status_code == 403, exc.detail
        return False


def test_wildcard_is_refused_on_the_bearer_path(monkeypatch, google_id_token):
    """A literal `*` is the ONE form the bearer path deliberately does not honour.

    oauth2-proxy reads the same terraform `${OAUTH_EMAIL_DOMAIN}` and honours `*` as "any
    domain the gateway admits". Nothing proxies this path — the token was minted by Google,
    not by the gateway — so honouring it here would accept every Google-verified address, with
    `GOOGLE_TOKEN_AUDIENCE` (unset here, the public gcloud client id when set) as no backstop.
    """
    monkeypatch.setattr(config, "allowed_email_domains", {"*"})
    assert _bearer_email_accepted(monkeypatch, google_id_token, "anyone@anywhere.example", {"*"}) is False


def test_wildcard_still_allows_everything_on_the_proxied_path(monkeypatch):
    """The marker-gated path is untouched: `*` still means allow-all there.

    `allow_wildcard` defaults to True precisely so this stays true; a default flipped to False
    would show up here rather than in production as a total lockout of a deployment that set
    `oauth_email_domain = "*"` on purpose. `_extract_user_from_header` must agree with it, or
    the usage log and the auth path would disagree about who the caller was.
    """
    monkeypatch.setattr(config, "allowed_email_domains", {"*"})
    req = _request(
        {
            USER_HEADER: "accounts.google.com:anyone@anywhere.example",
            "Authorization": f"Bearer {INTERNAL_SECRET}",
        }
    )
    assert auth.get_authenticated_user(req) == "anyone@anywhere.example"
    assert auth.get_verified_user(req) == "anyone@anywhere.example"
    assert _extract_user_from_header(
        _scope(
            {
                USER_HEADER: "accounts.google.com:anyone@anywhere.example",
                "Authorization": f"Bearer {INTERNAL_SECRET}",
            }
        )
    ) == "anyone@anywhere.example"


def test_star_dot_form_still_matches_a_subdomain_on_both_paths(monkeypatch, google_id_token):
    """`*` and `*.example.com` are different values: refusing the first must not touch the
    second, on either path, even when both are configured together."""
    assert _allowed(monkeypatch, "user@sub.example.com", {"*", "*.example.com"}) is True
    assert (
        _bearer_email_accepted(monkeypatch, google_id_token, "user@sub.example.com", {"*", "*.example.com"})
        is True
    )
    assert (
        _bearer_email_accepted(monkeypatch, google_id_token, "user@sub.example.com", {"*.example.com"})
        is True
    )


def test_wildcard_alongside_a_real_domain_still_refuses_strangers(monkeypatch, google_id_token):
    """Dropping `*` from the set must not turn the remaining entries into allow-all, nor stop
    them matching."""
    assert (
        _bearer_email_accepted(monkeypatch, google_id_token, "alice@finngen.fi", {"*", "finngen.fi"}) is True
    )
    assert _bearer_email_accepted(monkeypatch, google_id_token, "eve@evil.example", {"*", "finngen.fi"}) is False


def test_bare_star_is_not_itself_a_matchable_domain(monkeypatch):
    """With the wildcard refused, the star must not survive in the set as a domain that the
    plain membership test could match — the malformed address "a@*" is the one that would."""
    monkeypatch.setattr(config, "allowed_email_domains", {"*"})
    assert auth._email_allowed("a@*", allow_wildcard=False) is False
    assert auth._email_allowed("a@*") is True  # allow-all, for the same reason as any address


def test_the_opt_out_does_not_mutate_the_configured_domains(monkeypatch):
    """`domains.discard("*")` is the only mutation this change introduced, and it must stay on
    the per-call set comprehension: a refusal that ate the star out of the configured value
    would silently turn the proxied path into a lockout for the rest of the process."""
    configured = {"*"}
    monkeypatch.setattr(config, "allowed_email_domains", configured)
    assert auth._email_allowed("anyone@anywhere.example", allow_wildcard=False) is False
    assert auth._email_allowed("anyone@anywhere.example") is True
    assert configured == {"*"}


def test_every_other_form_is_unchanged_by_the_opt_out(monkeypatch):
    """The opt-out touches the bare `*` and nothing else."""
    monkeypatch.setattr(config, "allowed_emails", {"Guest@Example.org"})
    for domains, email in (
        ({"finngen.fi"}, "User@FinnGen.fi"),
        ({".example.com"}, "user@sub.example.com"),
        ({"*.example.com"}, "user@sub.example.com"),
        ({"*", "*.example.com"}, "user@deep.sub.example.com"),
        (set(), "guest@example.org"),
    ):
        monkeypatch.setattr(config, "allowed_email_domains", domains)
        assert auth._email_allowed(email, allow_wildcard=False) is True, (domains, email)
    for domains, email in (
        ({".example.com"}, "user@example.com"),
        ({".example.com"}, "user@notexample.com"),
        ({"example.com"}, "user@evilexample.com"),
        ({"*.example.com"}, "anyone@anywhere.example"),
    ):
        monkeypatch.setattr(config, "allowed_email_domains", domains)
        assert auth._email_allowed(email, allow_wildcard=False) is False, (domains, email)


def test_startup_warning_fires_only_for_a_literal_star(monkeypatch, caplog):
    """The operator set `*` from a terraform variable that never mentions this service, so
    silence about the half-honoured result is the whole problem repeating."""
    import logging

    monkeypatch.setattr(config, "allowed_email_domains", {"*"})
    with caplog.at_level(logging.WARNING, logger="app.core.auth"):
        auth.warn_if_wildcard_allow_list()
    fired = [r for r in caplog.records if "ALLOWED_EMAIL_DOMAINS contains a literal" in r.message]
    assert len(fired) == 1
    # the operator has to learn WHICH path disagrees, or the warning is just "something is wrong"
    assert "REFUSES" in fired[0].message and "id_token" in fired[0].message

    caplog.clear()
    monkeypatch.setattr(config, "allowed_email_domains", {"finngen.fi", "*.example.com"})
    with caplog.at_level(logging.WARNING, logger="app.core.auth"):
        auth.warn_if_wildcard_allow_list()
    assert [r for r in caplog.records if "ALLOWED_EMAIL_DOMAINS contains a literal" in r.message] == []
