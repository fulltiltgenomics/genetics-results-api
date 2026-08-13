import json
import logging
import sys
from datetime import datetime, timezone

import app.config.common as config

# extra fields to include in structured logs
EXTRA_LOG_FIELDS = [
    "log_type",
    "log_source",
    "timestamp",
    "user_email",
    "endpoint_path",
    "full_path",
    "http_method",
    "status_code",
    "duration_ms",
    # sandbox execution attribution (app/core/auth.py's "sandbox request authorized" line).
    # Without these three names the formatter's string-message branch drops the whole `extra=`,
    # so nothing of that line reaches jsonPayload and the sink cannot answer "which execution".
    "sub",
    "sid",
    "jti",
]


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
            for key in EXTRA_LOG_FIELDS:
                if hasattr(record, key):
                    log_entry[key] = getattr(record, key)

        # strip sensitive fields (full_path) when going to Cloud Logging
        if self.strip_sensitive:
            log_entry.pop("full_path", None)

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


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
