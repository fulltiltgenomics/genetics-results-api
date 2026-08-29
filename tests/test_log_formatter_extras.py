"""What survives `extra=` on its way into jsonPayload.

The formatter used to copy only the names on an `EXTRA_LOG_FIELDS` allow-list, and the failure
mode of an allow-list is silence: `sandbox_budget.log_rejection` passed `code`, `limit` and
`observed`, none of them listed, so every rejection line reached the operator naming who but
never which control fired, and nothing raised (genetics-results-suite-4h6.65). These tests pin
the inversion — a field attached to a record is in the payload unless LogRecord already owns
the name — so re-introducing a list would have to break them.
"""

import json
import logging

import pytest

from app.core import sandbox_budget
from app.core.logging_config import GCPJsonFormatter


class _Principal:
    session_id = "sess-1"
    execution_id = "exec-1"


@pytest.fixture
def rejection_payload(caplog):
    """Format the real `log_rejection` line the way a GKE stdout handler would."""
    with caplog.at_level(logging.WARNING, logger="app.core.sandbox_budget"):
        sandbox_budget.log_rejection(
            sandbox_budget.Rejection(
                code="sandbox_concurrency", limit=1, observed=2, detail="too many"
            ),
            "/api/v1/rsid/variants",
            _Principal(),
        )
    records = [r for r in caplog.records if r.msg == "sandbox per-execution limit exceeded"]
    assert records, "log_rejection logged nothing"
    return json.loads(GCPJsonFormatter(strip_sensitive=True).format(records[-1]))


def test_a_rejection_line_names_which_limit_fired(rejection_payload):
    """The one field that makes a 429 actionable: runaway script vs ceiling set too low."""
    assert rejection_payload["code"] == "sandbox_concurrency"


def test_a_rejection_line_carries_the_limit_and_the_observed_value(rejection_payload):
    assert rejection_payload["limit"] == 1
    assert rejection_payload["observed"] == 2


def test_a_rejection_line_still_carries_the_attribution_it_always_had(rejection_payload):
    assert rejection_payload["sid"] == "sess-1"
    assert rejection_payload["jti"] == "exec-1"
    assert rejection_payload["path"] == "/api/v1/rsid/variants"


def test_a_field_nobody_enumerated_reaches_the_payload():
    """The structural property, not the four names above: no list to forget."""
    record = logging.LogRecord(
        "t", logging.INFO, __file__, 1, "hello", None, None
    )
    record.a_field_added_next_year = "kept"
    payload = json.loads(GCPJsonFormatter().format(record))
    assert payload["a_field_added_next_year"] == "kept"
    assert payload["message"] == "hello"


def test_record_internals_do_not_leak_into_the_payload():
    payload = json.loads(
        GCPJsonFormatter().format(
            logging.LogRecord("t", logging.INFO, __file__, 1, "hi", None, None)
        )
    )
    assert set(payload) == {"timestamp", "severity", "logger", "message"}


class _StrRaises:
    def __str__(self):
        raise RuntimeError("nope")


def _circular_dict():
    d = {}
    d["self"] = d
    return d


def _circular_list():
    items = []
    items.append(items)
    return items


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(_circular_dict(), id="self_referential_dict"),
        pytest.param(_circular_list(), id="self_referential_list"),
        pytest.param({("a", "b"): 1}, id="tuple_keyed_dict"),
        pytest.param(_StrRaises(), id="str_raises"),
        pytest.param(object(), id="plain_object"),
    ],
)
def test_an_unserializable_extra_does_not_take_the_line_down(value):
    """`default=str` is not a total rescue, and the failure mode of a raise here is the worst
    one available: `Handler.handleError` swallows it and re-emits the traceback on stderr,
    unstructured, so GKE ingests noise and the record itself is gone. The first four values
    raised ValueError/TypeError/RuntimeError out of `format()` before the degrade path.
    """
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "hi", None, None)
    record.weird = value
    payload = json.loads(GCPJsonFormatter().format(record))
    assert payload["message"] == "hi"
    assert payload["severity"] == "INFO"
    assert "weird" in payload


def test_a_degraded_line_says_why_it_is_degraded():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "hi", None, None)
    record.weird = _circular_dict()
    payload = json.loads(GCPJsonFormatter().format(record))
    assert "Circular reference" in payload["log_format_error"]


def test_a_serializable_line_carries_no_degrade_marker():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "hi", None, None)
    record.fine = {"a": 1}
    assert "log_format_error" not in json.loads(GCPJsonFormatter().format(record))


@pytest.mark.parametrize(
    "key", ["severity", "timestamp", "logger", "trace", "labels", "httpRequest"]
)
def test_an_extra_cannot_overwrite_a_reserved_cloud_logging_key(key):
    """The deny-list let extras win over the line's own output keys, and on GKE those keys are
    not payload — the ingester honours them. `extra={"severity": "DEBUG"}` on a WARNING filed
    the line at DEBUG, i.e. any caller could put its own line under an alerting threshold.
    The value must still appear somewhere: dropping it is the silent loss this whole change
    is about.
    """
    record = logging.LogRecord("t", logging.WARNING, __file__, 1, "hi", None, None)
    setattr(record, key, "forged")
    payload = json.loads(GCPJsonFormatter().format(record))
    assert payload["severity"] == "WARNING"
    assert payload.get(key) != "forged"
    assert payload["extra_" + key] == "forged"


def test_a_forged_cloud_logging_special_field_is_re_keyed_not_passed_through():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "hi", None, None)
    setattr(record, "logging.googleapis.com/labels", {"env": "prod"})
    payload = json.loads(GCPJsonFormatter().format(record))
    assert "logging.googleapis.com/labels" not in payload
    assert payload["extra_logging.googleapis.com/labels"] == {"env": "prod"}


def test_a_re_keyed_extra_does_not_collide_with_a_real_extra_of_that_name():
    record = logging.LogRecord("t", logging.WARNING, __file__, 1, "hi", None, None)
    record.severity = "DEBUG"
    record.extra_severity = "mine"
    payload = json.loads(GCPJsonFormatter().format(record))
    assert payload["severity"] == "WARNING"
    assert payload["extra_severity"] == "mine"
    assert payload["extra_extra_severity"] == "DEBUG"
