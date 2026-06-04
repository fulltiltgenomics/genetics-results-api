"""Bounded retry for transient GCS errors on fsspec/gcsfs reads.

The GoogleEgressBandwidth quota is enforced per-project per-region, so a burst
from any workload in the same region (e.g. a Cromwell pipeline reading release
data) can briefly throttle our small metadata reads with a 429. gcsfs's own
retries cover short blips; this adds a bounded outer retry so a slightly longer
saturation is absorbed instead of surfaced to the caller.
"""

import logging
import random
import time
from typing import Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

# rate-limit + transient server errors worth another attempt
_TRANSIENT_STATUSES = {408, 429, 500, 502, 503, 504}


def _is_transient(exc: BaseException) -> bool:
    # gcsfs raises gcsfs.retry.HttpError with a numeric .code; aiohttp/google
    # client errors expose .status. fall back to spotting a 429 in the text.
    code = getattr(exc, "code", None)
    if code is None:
        code = getattr(exc, "status", None)
    if isinstance(code, int):
        return code in _TRANSIENT_STATUSES
    return "429" in str(exc)


def with_gcs_retry(
    fn: Callable[[], T],
    *,
    max_attempts: int = 4,
    base_delay: float = 1.0,
    max_delay: float = 20.0,
) -> T:
    """Call ``fn`` and retry transient GCS errors with jittered backoff.

    ``fn`` must perform the whole open-and-read and return a materialized result
    so a retry re-opens the file; retrying a half-consumed stream would not help.
    Non-transient errors propagate immediately.
    """
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:
            attempt += 1
            if attempt >= max_attempts or not _is_transient(exc):
                raise
            delay = min(max_delay, base_delay * 2 ** (attempt - 1))
            delay *= 0.5 + random.random()  # full jitter so retries don't resync
            logger.warning(
                f"transient GCS error on read (attempt {attempt}/{max_attempts - 1}), "
                f"retrying in {delay:.1f}s: {exc}"
            )
            time.sleep(delay)
