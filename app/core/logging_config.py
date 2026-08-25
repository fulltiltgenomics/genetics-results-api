import json
import logging
import sys
from datetime import datetime, timezone

import app.config.common as config

# Names LogRecord owns. Anything else a caller attaches via `extra=` is, by construction, a
# field somebody wanted in the log, so the formatter copies all of it into jsonPayload.
#
# This was an EXTRA_LOG_FIELDS allow-list, and it failed in the only direction that is silent:
# `app/core/sandbox_budget.py`'s `log_rejection` passes `code`, `limit` and `observed`, none of
# which were listed, so every "sandbox per-execution limit exceeded" line reached the operator
# saying who but never which control fired — and nothing raised, nothing warned, the line simply
# arrived short (genetics-results-suite-4h6.65). An allow-list makes forgetting an entry a
# no-op; a deny-list of names that already mean something makes the omission impossible instead
# of merely discouraged.
#
# Derived from a probe record rather than typed out, so it tracks the interpreter: `taskName`
# exists on 3.12+ and did not before. `logging.Logger.makeRecord` already raises on an `extra`
# key that collides with one of these, so this set is a backstop for records built by other
# means, not the primary guard.
_RESERVED_RECORD_ATTRS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {
    "message",
    "asctime",
    "taskName",
}

# Keys the Cloud Logging ingester lifts out of a structured stdout line and honours as the
# entry's own metadata rather than as payload. An extra reaching one of them is not a cosmetic
# clash: `extra={"severity": "DEBUG"}` on a `logger.warning()` would file the line at DEBUG and
# put it under any alerting threshold keyed on severity, and `httpRequest`/`trace`/`labels`
# would let a caller forge the request line, the trace correlation and the index labels. The
# allow-list this formatter used to have made that unreachable by construction; inverting it to
# a deny-list reopened it, so the reserved names are re-keyed instead — never dropped, since a
# silently missing field is the exact failure genetics-results-suite-4h6.65 is about.
_CLOUD_LOGGING_RESERVED_KEYS = frozenset(
    {"severity", "timestamp", "message", "logger", "trace", "labels", "httpRequest"}
)
_CLOUD_LOGGING_RESERVED_PREFIX = "logging.googleapis.com/"


def _extra_fields(record: logging.LogRecord) -> dict:
    return {k: v for k, v in vars(record).items() if k not in _RESERVED_RECORD_ATTRS}


def _is_reserved_output_key(key: str) -> bool:
    return key in _CLOUD_LOGGING_RESERVED_KEYS or key.startswith(_CLOUD_LOGGING_RESERVED_PREFIX)


def _merge_extras(log_entry: dict, extras: dict) -> None:
    """Copy `extras` in without letting any of them displace a key the line already owns.

    Two passes so that a caller's own `extra_severity` keeps its name and the re-keyed
    `severity` moves further out of the way, rather than the outcome depending on which
    attribute happened to be set on the record first.
    """
    displaced = {}
    for key, value in extras.items():
        if key in log_entry or _is_reserved_output_key(key):
            displaced[key] = value
        else:
            log_entry[key] = value
    for key, value in displaced.items():
        safe = f"extra_{key}"
        while safe in log_entry or _is_reserved_output_key(safe):
            safe = f"extra_{safe}"
        log_entry[safe] = value


def _safe_str(value: object) -> str:
    try:
        return str(value)
    except Exception:
        # a `__str__` that raises is one of the three shapes `default=str` cannot rescue
        return f"<unrepresentable {type(value).__name__}>"


def _degrade(log_entry: dict, exc: BaseException) -> str:
    """Serialize an entry `json.dumps` refused, replacing only the values it choked on.

    `default=str` covers an object json does not know; it does not cover a self-referential
    dict or list (ValueError: Circular reference detected), a non-str dict key (TypeError) or a
    `__str__` that raises. Letting those propagate out of `format()` loses the whole line:
    `logging.Handler.handleError` swallows the exception and prints a traceback on stderr,
    which on GKE is ingested as an unparsed line — so the operator loses the record and gains
    noise. A degraded entry naming the offending value is strictly better.
    """
    degraded = {}
    for key, value in log_entry.items():
        try:
            json.dumps(value, default=str)
        except Exception:
            value = _safe_str(value)
        degraded[_safe_str(key)] = value
    degraded["log_format_error"] = f"{type(exc).__name__}: {_safe_str(exc)}"
    return json.dumps(degraded, default=str)


class GCPJsonFormatter(logging.Formatter):
    """
    JSON formatter for GCP Cloud Logging.

    Outputs logs in a format that GCP Cloud Logging can parse,
    with extra fields included in jsonPayload for log sink filtering.
    """

    def __init__(self, strip_sensitive: bool = False):
        super().__init__()
        self.strip_sensitive = strip_sensitive

    def format(self, record: logging.LogRecord) -> str:
        # handle dict messages (structured logs from middleware)
        msg = record.msg
        if isinstance(msg, dict):
            log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "severity": record.levelname,
                "logger": record.name,
                **msg,
            }
        else:
            log_entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "severity": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            _merge_extras(log_entry, _extra_fields(record))

        # strip sensitive fields (full_path) when going to Cloud Logging
        if self.strip_sensitive:
            log_entry.pop("full_path", None)

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        try:
            return json.dumps(log_entry, default=str)
        except Exception as exc:
            try:
                return _degrade(log_entry, exc)
            except Exception:
                return json.dumps(
                    {
                        "severity": record.levelname,
                        "logger": record.name,
                        "message": "log entry could not be serialized",
                    }
                )


class StripSensitiveFieldsFilter(logging.Filter):
    """strip sensitive fields (full_path) before sending to Cloud Logging"""

    FIELDS_TO_STRIP = {"full_path"}

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, dict):
            # create a copy without sensitive fields
            record.msg = {k: v for k, v in record.msg.items() if k not in self.FIELDS_TO_STRIP}
        return True


def _setup_cloud_logging_api():
    """use google-cloud-logging library to send logs directly to Cloud Logging API"""
    import google.cloud.logging
    from google.cloud.logging_v2.handlers import CloudLoggingHandler

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.log_level, logging.INFO))

    # Handler order is load-bearing, not cosmetic. `CloudLoggingFilter.filter()` in
    # google.cloud.logging_v2.handlers.handlers mutates the SHARED record, writing ~13
    # underscore-prefixed attributes onto it (`_http_request` — whose `requestUrl` carries the
    # query string this formatter's `strip_sensitive` exists to remove — plus `_trace`,
    # `_labels`, `_source_location` and the rest). None of them are LogRecord attributes, so
    # none are in `_RESERVED_RECORD_ATTRS`, and the deny-list would copy every one of them into
    # jsonPayload. It is not live only because `callHandlers` runs handlers in insertion order
    # and stdout is added first, so this formatter sees the record before that filter has
    # touched it. Swap the two `addHandler` calls below and every stdout line regains the query
    # string. The risk here is other HANDLERS on the same logger, not other interpreters.
    #
    # stdout first (includes full_path for debugging)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(GCPJsonFormatter())
    root_logger.addHandler(stdout_handler)

    # Cloud Logging second (filter strips full_path for privacy)
    client = google.cloud.logging.Client()
    cloud_handler = CloudLoggingHandler(client, name="genetics-results-api")
    cloud_handler.addFilter(StripSensitiveFieldsFilter())
    root_logger.addHandler(cloud_handler)


def _setup_stdout_logging():
    """log JSON to stdout (for GKE where stdout is captured automatically)"""
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.log_level, logging.INFO))

    # strip sensitive fields since stdout goes to Cloud Logging on GKE
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(GCPJsonFormatter(strip_sensitive=True))
    root_logger.addHandler(handler)


_logging_initialized = False


def setup_logging():
    global _logging_initialized
    if _logging_initialized:
        return

    # clear any existing handlers to avoid duplicates
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    cloud_logging_failure = None
    if config.use_cloud_logging_api:
        import google.auth.exceptions

        try:
            _setup_cloud_logging_api()
        except (google.auth.exceptions.GoogleAuthError, OSError) as e:
            # Cloud Logging needs ADC and a project. Without either (tests, a laptop, any
            # credential-free import) the client constructor raises and, since setup_logging()
            # runs at module scope in app.core.streams, takes the whole import down with it.
            # google-cloud-core raises a bare OSError for the missing project, hence the pair.
            cloud_logging_failure = e
            root_logger.handlers.clear()  # drop the stdout handler added before the client failed
            _setup_stdout_logging()
    else:
        _setup_stdout_logging()

    # suppress noisy logs
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("fsspec").setLevel(logging.WARNING)
    logging.getLogger("gcsfs").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    _logging_initialized = True

    # after the handlers exist, so a real production auth failure is visible instead of swallowed
    if cloud_logging_failure is not None:
        logging.getLogger(__name__).warning(
            "cloud logging API unavailable (%s: %s), falling back to stdout logging",
            type(cloud_logging_failure).__name__,
            cloud_logging_failure,
        )
