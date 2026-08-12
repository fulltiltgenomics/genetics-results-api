"""Per-execution (`jti`) aggregate limits for sandbox callers.

Design of record: `docs/code-execution-security.md` section 4 in genetics-results-suite.

`SandboxResponseCapMiddleware` (app/middleware.py) bounds **one** response at
`SANDBOX_MAX_RESPONSE_BYTES`. A script has ~120 seconds of wall clock, and nothing in that cap
bounds how many responses it asks for, at what concurrency, or how many bytes it accumulates in
total. This module is the analogue of db-api's `_jti_bytes` counter (`api/main.py`,
`SANDBOX_AGGREGATE_BYTES_BUDGET`) and is deliberately shaped like it: one in-process map keyed
on the token's `jti`, checked **before** the work starts, answering 429 rather than truncating.
An operator reasoning about sandbox limits should not have to hold two mental models.

Three deviations from db-api, each for a reason:

1. **Four counters, not one**, plus a bound on the map itself. db-api's single scarce resource
   is BigQuery bytes. Here the scarce resources are egress bytes
   (`SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET`), request slots
   (`SANDBOX_MAX_REQUESTS_PER_EXECUTION`), and *memory* — concurrency per execution
   (`SANDBOX_MAX_CONCURRENT_REQUESTS`) and pod-wide
   (`SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL`), because every in-flight capped request buffers up
   to 16 MiB on a `replicas: 1` pod that already preloads the gene maps and the search index.
   Concurrency is the one control here with a memory-exhaustion failure mode rather than a cost
   one. `SANDBOX_MAX_TRACKED_EXECUTIONS` bounds the counter map rather than any execution, so
   it is a backstop rather than a fifth per-execution limit — but it too can answer 429, which
   is why this module emits **five** rejection codes. The 16 MiB per-response cap is a sixth
   sandbox control and does **not** live here; it is `app/core/limits.py` and
   `app/middleware.py`.
2. **Env-configurable.** db-api's budget is a module constant. results-api's payload sizes vary
   by dataset and format in a way BigQuery byte counts do not, so an operator has to be able to
   widen or tighten these without a rebuild.
3. **Eviction on token expiry, not LRU.** db-api trims `_jti_bytes` to 1024 entries LRU, which
   *can* drop a live execution's counter and silently reset its budget — the fail-open
   direction. Here an entry is evictable only once its token can no longer authenticate a
   request **and** it has nothing in flight, so a live execution can never lose its accounting.
   The map is still hard-bounded; when the bound is reached it is a *new* execution that is
   refused, never a running one that is evicted. See `_sweep_locked` and `MAX_TRACKED`.

**Reject, never queue.** Every one of the five rejections answers 429 immediately. Queueing would hold the
request while the sandbox's ~120 second wall clock keeps running, which the script cannot
distinguish from slow data and cannot act on; worse, work admitted from a queue can complete
after the execution is already dead, which is exactly the wasted production
`genetics-results-suite-4h6.28` removed on the rejection path. A fast, labelled 429 lets a
script narrow its request or back off while it still has clock left.

**In-process, so `replicas: 1` is load-bearing** — the same way it is for db-api. At N replicas
these bound per replica, not per execution. `k8s/deployments/results-api.yaml` carries a comment
on `replicas: 1` saying so.

**Two limitations an operator must know, because as shipped these bound less than the rest of
this docstring implies:**

1. **They only bind a request that volunteers a token.** `_sandbox_principal` in
   `app/middleware.py` reads the `Authorization` header; with no header it returns `None` and
   `admit` is never called, so the request is not counted at all. results-api has seven
   `@is_public` routes (re-derive with `grep -rn "@is_public" app/`) that answer 200 with no
   credential, and the sandbox's NetworkPolicy egress reaches `results-api:4000` **directly**,
   bypassing auth-gateway. Measured: 20 of 20 header-less requests were served 200 with
   `_executions == {}`. So these are limits on an *honest* execution's consumption — which is
   what they were written for — and not yet a complete bound on what a script can extract.
   `app/core/limits.py`'s argument that omitting the header cannot buy a *looser* limit holds
   for the per-response byte cap only; for the four counters here, omitting it buys **no**
   limit. Closing that needs a way to identify sandbox traffic without a token, which is a
   design decision tracked separately — do not paper over it with a rate limiter here.
2. **`sandbox_execution_tracker_full` and `SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL` are
   cross-tenant.** Both are pod-wide, so a caller that fills the map or holds the pod-wide slots
   locks *other* executions out — a denial surface, not merely a self-limit. The "23 chat
   turns/hour" sizing argument above is about honest volume and says nothing about an attacker;
   with limitation 1 unclosed there is no per-tenant fairness behind these two numbers. They are
   deliberately sized far above honest use so that an honest execution never meets them.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass

from app.core import sandbox_token

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    """Read a positive integer limit, failing at import rather than at the first request.

    Every value here is a *ceiling*, and `admit` compares with `>=`, so 0 or a negative turns
    the limit into "reject every sandbox request" — a silent, total outage of the sandbox data
    path that no test and no health check would attribute to a typo in a manifest. A bad value
    must stop the pod from starting instead.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive integer, got {raw!r}") from None
    if value < 1:
        raise ValueError(
            f"{name} must be a positive integer, got {value}. Every limit here is a ceiling "
            "compared with >=, so 0 or a negative rejects every sandbox request."
        )
    return value


# Aggregate response bytes one execution may be *sent*. 1 GiB is 64 responses at the 16 MiB
# per-response cap, or ~8.5 MB/s sustained across the whole 120 second wall clock — orders of
# magnitude above the design's intent, in which a script aggregates in-pod and returns 64 KiB to
# the model (section 2), while still bounding egress for a script that only loops.
SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET = _env_int(
    "SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET", 1024**3
)
# Requests one execution may issue. The byte budget alone does not bound a loop of *small*
# responses, and every request costs a tabix seek or a GCS range read regardless of its size.
# 1000 over 120 seconds is ~8 rps, well above any legitimate per-gene or per-variant loop.
SANDBOX_MAX_REQUESTS_PER_EXECUTION = _env_int("SANDBOX_MAX_REQUESTS_PER_EXECUTION", 1000)
# In-flight requests per execution. 4 x 16 MiB = 64 MiB of buffered bodies, against an 8Gi pod
# limit that already holds the gene maps and the search index.
SANDBOX_MAX_CONCURRENT_REQUESTS = _env_int("SANDBOX_MAX_CONCURRENT_REQUESTS", 4)
# In-flight sandbox requests across the whole pod, i.e. across all executions. The sandbox is
# specified at `concurrency: 1` with a queue, so today this can only be reached by a single
# execution and the per-execution limit binds first; it exists so that raising the sandbox's own
# concurrency cannot silently multiply this pod's peak buffer. 8 x 16 MiB = 128 MiB.
SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL = _env_int("SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL", 8)
# Hard bound on the counter map itself, so a flood of distinct `jti`s cannot grow it without
# limit. Entries live at most one token lifetime (~305s), so at the measured peak of 23 chat
# turns/hour this is never approached; it is a backstop, not a working limit.
SANDBOX_MAX_TRACKED_EXECUTIONS = _env_int("SANDBOX_MAX_TRACKED_EXECUTIONS", 4096)

if SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL < SANDBOX_MAX_CONCURRENT_REQUESTS:
    # the pod-wide bound is meant to sit *above* the per-execution one; inverted, a single
    # execution can never reach its own allowance and the per-execution number in the manifest
    # is a lie an operator would have to read the code to catch
    raise ValueError(
        "SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL "
        f"({SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL}) must be >= "
        f"SANDBOX_MAX_CONCURRENT_REQUESTS ({SANDBOX_MAX_CONCURRENT_REQUESTS}): the pod-wide "
        "bound is a ceiling over all executions and cannot be tighter than one execution's."
    )


@dataclass
class _Execution:
    """One execution's running totals. `expires_at` is the token's `exp`."""

    expires_at: int
    bytes_sent: int = 0
    requests: int = 0
    in_flight: int = 0


@dataclass(frozen=True)
class Rejection:
    """A refused admission. `code` names *which* limit, so a 429 is actionable in a log."""

    code: str
    limit: int
    observed: int
    detail: str


_executions: dict[str, _Execution] = {}
_in_flight_total = 0
_lock = threading.Lock()


def _sweep_locked(now: float) -> None:
    """Drop entries that can never be charged again.

    Evictable means **both**: the token has passed the point where `verify_sandbox_token` would
    still accept it (`exp` plus the verifier's own leeway), so no further request can present
    that `jti`; and nothing is in flight under it, which covers a stream that outlives its own
    token. Anything else is a live execution, and evicting one would silently reset its budget —
    the fail-open direction, and the reason this is not db-api's LRU.
    """
    cutoff = now - sandbox_token.LEEWAY_SECONDS
    for jti in [
        jti
        for jti, entry in _executions.items()
        if entry.in_flight == 0 and entry.expires_at < cutoff
    ]:
        del _executions[jti]


def admit(principal: sandbox_token.SandboxPrincipal) -> Rejection | None:
    """Reserve a request slot for `principal`'s execution, or say why not.

    Returns `None` when admitted — the caller **must** then call `release` exactly once — or a
    `Rejection` to be turned into a 429. Checked before the handler runs, so a spent execution
    costs no GCS read.
    """
    global _in_flight_total
    jti = principal.execution_id
    with _lock:
        entry = _executions.get(jti)
        if entry is None:
            _sweep_locked(time.time())
            if len(_executions) >= SANDBOX_MAX_TRACKED_EXECUTIONS:
                # refuse the new execution rather than evict a live one: an evicted counter is a
                # reset budget, which is the failure that matters
                return Rejection(
                    code="sandbox_execution_tracker_full",
                    limit=SANDBOX_MAX_TRACKED_EXECUTIONS,
                    observed=len(_executions),
                    detail=(
                        "Too many sandbox executions are being tracked by this pod "
                        f"({len(_executions)} of {SANDBOX_MAX_TRACKED_EXECUTIONS}). Retry in a "
                        "few minutes."
                    ),
                )
            entry = _executions[jti] = _Execution(expires_at=principal.expires_at)

        if entry.requests >= SANDBOX_MAX_REQUESTS_PER_EXECUTION:
            return Rejection(
                code="sandbox_request_count",
                limit=SANDBOX_MAX_REQUESTS_PER_EXECUTION,
                observed=entry.requests,
                detail=(
                    "Request limit for this execution reached: "
                    f"{entry.requests} of {SANDBOX_MAX_REQUESTS_PER_EXECUTION} requests. "
                    "Fetch wider ranges in fewer calls and aggregate in the sandbox."
                ),
            )
        if entry.bytes_sent >= SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET:
            return Rejection(
                code="sandbox_aggregate_bytes",
                limit=SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET,
                observed=entry.bytes_sent,
                detail=(
                    "Aggregate response-byte budget for this execution exhausted: "
                    f"{entry.bytes_sent} of {SANDBOX_AGGREGATE_RESPONSE_BYTES_BUDGET} bytes "
                    "already sent. Narrow the requests and aggregate in the sandbox."
                ),
            )
        if entry.in_flight >= SANDBOX_MAX_CONCURRENT_REQUESTS:
            return Rejection(
                code="sandbox_concurrency",
                limit=SANDBOX_MAX_CONCURRENT_REQUESTS,
                observed=entry.in_flight,
                detail=(
                    "Too many concurrent requests for this execution: "
                    f"{entry.in_flight} of {SANDBOX_MAX_CONCURRENT_REQUESTS} in flight. "
                    "Await the requests already issued before starting more."
                ),
            )
        if _in_flight_total >= SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL:
            return Rejection(
                code="sandbox_concurrency_pod",
                limit=SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL,
                observed=_in_flight_total,
                detail=(
                    "Too many concurrent sandbox requests on this pod: "
                    f"{_in_flight_total} of {SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL} in flight. "
                    "Retry shortly."
                ),
            )

        entry.requests += 1
        entry.in_flight += 1
        _in_flight_total += 1
        return None


def release(jti: str, bytes_sent: int = 0) -> None:
    """Free the request slot and charge what the response actually put on the wire.

    `bytes_sent` is the length of the body `SandboxResponseCapMiddleware` sent, taken from that
    middleware's own buffer, so the two agree by construction and nothing is counted twice.
    Every status is charged, not only 2xx: FastAPI's 422 handler echoes the offending input, so
    a non-2xx body is as caller-controlled as a 200, and while error bodies were uncharged the
    real egress bound was `SANDBOX_MAX_REQUESTS_PER_EXECUTION` x (whatever fits in a URI or a
    request body) rather than this budget.

    One body is still uncharged: a response the middleware *rejected* over the per-response cap,
    which never went on the wire — what the caller received there is a bounded stub, and a loop
    of those is what `SANDBOX_MAX_REQUESTS_PER_EXECUTION` bounds.
    """
    global _in_flight_total
    with _lock:
        _in_flight_total = max(0, _in_flight_total - 1)
        entry = _executions.get(jti)
        if entry is None:
            return
        entry.in_flight = max(0, entry.in_flight - 1)
        if bytes_sent > 0:
            entry.bytes_sent += bytes_sent


def snapshot(jti: str) -> _Execution | None:
    """The running totals for `jti`, for tests and for diagnostics. Never mutated by callers."""
    with _lock:
        entry = _executions.get(jti)
        return None if entry is None else _Execution(**vars(entry))


def reset() -> None:
    """Drop all accounting. Test-only; nothing in the request path calls this."""
    global _in_flight_total
    with _lock:
        _executions.clear()
        _in_flight_total = 0


def log_rejection(rejection: Rejection, path: str | None, principal) -> None:
    logger.warning(
        "sandbox per-execution limit exceeded",
        extra={
            "path": path,
            "code": rejection.code,
            "limit": rejection.limit,
            "observed": rejection.observed,
            "sid": getattr(principal, "session_id", None),
            "jti": getattr(principal, "execution_id", None),
        },
    )
