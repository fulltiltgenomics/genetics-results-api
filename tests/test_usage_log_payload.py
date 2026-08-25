"""Tests for the shape of results-api's `endpoint_access` log payload.

These rows land in `phewas-development.genetics_api_logs.stdout` alongside db-api's, because a
Cloud Logging -> BigQuery sink names its table after the log ID (`stdout`), not after the
service. Telling the two apart therefore depends entirely on a field in this payload, and both
previous candidates moved underneath the queries built on them: `endpoint_path IS NULL` held
only while db-api emitted no path, and `log_source` is derived from DEPLOY_ENV and was renamed
in production once already (`genetics-results-api-prod` -> `finngenie_prod`), which returns an
empty result and no error. `service` is pinned here so it cannot acquire the same habit.
"""

import asyncio
import logging

import pytest

import app.config.common as config
from app.core.sandbox_token import PRINCIPAL_PREFIX, SandboxPrincipal
from app.middleware_usage_logging import UsageLoggingMiddleware


def _scope(path: str = "/api/v1/rsid/variants", method: str = "GET") -> dict:
    return {"type": "http", "method": method, "path": path, "headers": []}


async def _ok_app(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"{}"})


def _emit(caplog, scope: dict) -> dict:
    """Run one request through the middleware and return the logged dict payload."""
    with caplog.at_level(logging.INFO, logger="app.middleware_usage_logging"):

        async def _noop_receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        asyncio.run(UsageLoggingMiddleware(_ok_app)(scope, _noop_receive, lambda m: _sent(m)))
    entries = [r.msg for r in caplog.records if isinstance(r.msg, dict)]
    assert entries, "middleware logged nothing"
    return entries[-1]


async def _sent(message):
    return None


def test_service_is_present_and_names_this_service(caplog):
    assert _emit(caplog, _scope())["service"] == "results-api"


def test_service_is_not_derived_from_the_environment(monkeypatch, caplog):
    """The whole point: a deploy that renames the environment must not move the discriminator.

    log_source follows DEPLOY_ENV and is expected to change; `service` must not.
    """
    monkeypatch.setattr(config, "log_source", "some-renamed-env")
    entry = _emit(caplog, _scope())
    assert entry["service"] == "results-api"
    assert entry["log_source"] == "some-renamed-env"


@pytest.mark.parametrize("path", ["/api/v1", "/api/v1/variant_sets", "/api/v1/gene/GPT"])
def test_service_is_on_every_logged_route_not_only_authenticated_ones(caplog, path):
    """A discriminator missing from public routes would silently drop them from every split."""
    assert _emit(caplog, _scope(path))["service"] == "results-api"


def test_log_type_still_marks_the_row_for_the_sink_filter(caplog):
    assert _emit(caplog, _scope())["log_type"] == "endpoint_access"


def _sandbox_principal(user: str = "person@example.org") -> SandboxPrincipal:
    return SandboxPrincipal(
        user=user, session_id="sess-9", execution_id="exec-9", scope="read", expires_at=0
    )


def test_a_sandbox_request_is_attributable_to_the_user_from_this_log_alone(caplog):
    """The token's `sub` is the end user; without it the row named a session, not a person.

    An INTERNAL_API_SECRET caller logs `user_email: null` too, so a null here left an accounted
    sandbox request and an unaccounted internal one differing only by the presence of `jti`
    (genetics-results-suite-4h6.65). Reachable only where no auth dependency ran — an
    `@is_public` route with ANONYMOUS_SURFACE_MINIMAL off, or REQUIRE_AUTH=false.
    """
    scope = _scope()
    scope["state"] = {"sandbox_principal": _sandbox_principal()}
    entry = _emit(caplog, scope)
    assert entry["user_email"] == "sandbox:person@example.org"
    assert entry["sid"] == "sess-9"
    assert entry["jti"] == "exec-9"


def test_the_sandbox_marker_survives_into_the_one_column_that_reaches_bigquery(caplog):
    """`sandbox:` is the only thing separating a script's row from a verified human's.

    The sink admits `log_type="endpoint_access"` and nothing else, so if this stamped the bare
    address the two rows for the same person would be identical in every field a query can see
    — which is what `docs/code-execution-security.md` says cannot happen.
    """
    scope = _scope()
    scope["state"] = {"sandbox_principal": _sandbox_principal()}
    assert _emit(caplog, scope)["user_email"].startswith(PRINCIPAL_PREFIX)


def test_the_sandbox_stamp_agrees_with_what_the_auth_dependency_already_stored(caplog):
    """`get_verified_user` returns `principal.identity`, so on an authenticated route the
    fallback has already put that string in `user_email`. Stamping anything else here would
    make the same request log two different identities depending on which route it hit."""
    principal = _sandbox_principal()
    scope = _scope()
    scope["state"] = {"sandbox_principal": principal, "authenticated_user": principal.identity}
    assert _emit(caplog, scope)["user_email"] == principal.identity


def test_a_non_sandbox_request_still_carries_no_sandbox_attribution(caplog):
    entry = _emit(caplog, _scope())
    assert entry["user_email"] is None
    assert "sid" not in entry and "jti" not in entry
