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
