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


def _emit_record(caplog, scope: dict, app=_ok_app) -> logging.LogRecord:
    """Run one request through the middleware and return the LogRecord it emitted."""
    with caplog.at_level(logging.INFO, logger="app.middleware_usage_logging"):

        async def _noop_receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        try:
            asyncio.run(UsageLoggingMiddleware(app)(scope, _noop_receive, lambda m: _sent(m)))
        except _AppFailed:
            pass  # the middleware logs from a finally, so a failing app still produces a row
    records = [r for r in caplog.records if isinstance(r.msg, dict)]
    assert records, "middleware logged nothing"
    return records[-1]


def _emit(caplog, scope: dict, app=_ok_app) -> dict:
    """Run one request through the middleware and return the logged dict payload."""
    return _emit_record(caplog, scope, app).msg


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


# --- response size (genetics-results-suite-fv5) -------------------------------------------
#
# No response size was recorded on any of the 278,757 rows of genetics_api_logs, so "did any
# response approach the cap?" was unanswerable from logs. These pin the number actually
# recorded, and the key it is recorded under: `httpRequest.responseSize` means wire bytes
# including headers and is already filled with genuine wire bytes by auth-gateway and the GCLB
# elsewhere in the suite, so this number — uncompressed body bytes, pre-gzip — must not share
# that name.


class _AppFailed(Exception):
    pass


async def _streaming_app(scope, receive, send):
    """Several chunks and no Content-Length — what TimedStreamingResponse puts on the wire.

    The final message omits `body` entirely, which the ASGI spec permits and which is the
    shape that makes a naive `message["body"]` raise.
    """
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/tab-separated-values")],
        }
    )
    for chunk in (b"chrom\tpos\n", b"1\t100\n", b"1\t200\n"):
        await send({"type": "http.response.body", "body": chunk, "more_body": True})
    await send({"type": "http.response.body", "more_body": False})


_STREAMED_BYTES = len(b"chrom\tpos\n") + len(b"1\t100\n") + len(b"1\t200\n")


async def _disconnecting_app(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"partial", "more_body": True})
    raise _AppFailed("client went away mid-stream")


def test_response_size_is_recorded_for_a_buffered_response(caplog):
    assert _emit(caplog, _scope())["response_body_bytes"] == len(b"{}")


def test_response_size_counts_every_chunk_of_a_streamed_body(caplog):
    """Content-Length is absent for exactly these responses, so the header could not answer.

    Reading it would have left the streamed bulk endpoints — the only ones whose size the
    truncation question is about — with no size at all.
    """
    entry = _emit(caplog, _scope("/api/v1/credible_sets"), _streaming_app)
    assert entry["response_body_bytes"] == _STREAMED_BYTES


def test_the_size_is_not_emitted_as_httpRequest_responseSize(caplog):
    """Different quantity, same name: `httpRequest.responseSize` is wire size *including*
    headers, and auth-gateway's `$body_bytes_sent` and the GCLB request logs already populate
    that field with real wire bytes elsewhere in this suite. This number is pre-gzip body
    bytes, so under that name it would over-report and mix silently in a join. Pinned on both
    emission paths — the payload dict and the LogRecord `extra` the dev handler reads."""
    record = _emit_record(caplog, _scope())
    assert "httpRequest" not in record.msg, record.msg
    assert not hasattr(record, "http_request")


def test_the_size_key_stays_top_level_in_the_stdout_line(caplog):
    """Production and staging log to stdout (`use_cloud_logging_api` is dev-only), and the sink
    grows a jsonPayload column for a top-level key when the first row carries it. Nested
    anywhere else there is no column to query."""
    import json

    from app.core.logging_config import GCPJsonFormatter

    line = json.loads(GCPJsonFormatter(strip_sensitive=True).format(_emit_record(caplog, _scope())))
    assert line["response_body_bytes"] == len(b"{}")


def test_a_zero_byte_response_records_zero_rather_than_nothing(caplog):
    """A plain int key keeps the 0 that protobuf JSON would drop from `responseSize`, so an
    empty body stays distinguishable from a row written before the field existed."""

    async def _empty_app(scope, receive, send):
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    entry = _emit(caplog, _scope(), _empty_app)
    assert entry["response_body_bytes"] == 0


def test_a_stream_torn_down_mid_body_records_the_bytes_that_went_out(caplog):
    entry = _emit(caplog, _scope(), _disconnecting_app)
    assert entry["response_body_bytes"] == len(b"partial")
