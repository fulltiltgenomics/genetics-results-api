import json
import logging
import sys
from datetime import datetime, timezone

import app.config.common as config


class GCPJsonFormatter(logging.Formatter):
    """
    JSON formatter for GCP Cloud Logging.

    Outputs logs in a format that GCP Cloud Logging can parse,
    with extra fields included in jsonPayload for log sink filtering.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # include extra fields (log_type, user_email, etc.)
        for key in ["log_type", "user_email", "endpoint_path", "http_method", "status_code", "duration_ms"]:
            if hasattr(record, key):
                log_entry[key] = getattr(record, key)

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


def setup_logging():
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.log_level, logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(GCPJsonFormatter())
    root_logger.addHandler(handler)

    # suppress noisy logs
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("fsspec").setLevel(logging.WARNING)
    logging.getLogger("gcsfs").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
