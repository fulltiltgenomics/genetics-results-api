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
   is why this module emits **six** rejection codes — the sixth being
   `sandbox_concurrency_pod_share`, the fairness half of the pod-wide bound described in
   limitation 2 below. The 16 MiB per-response cap is a further sandbox control and does **not**
   live here; it is `app/core/limits.py` and `app/middleware.py`. So is the request deadline,
   `SANDBOX_REQUEST_TIMEOUT_SECONDS`, whose value is declared here but which is armed and
   answered in `app/middleware.py`.
2. **Env-configurable.** db-api's budget is a module constant. results-api's payload sizes vary
   by dataset and format in a way BigQuery byte counts do not, so an operator has to be able to
   widen or tighten these without a rebuild.
3. **Eviction on token expiry, not LRU.** db-api trims `_jti_bytes` to 1024 entries LRU, which
   *can* drop a live execution's counter and silently reset its budget — the fail-open
   direction. Here an entry is evictable only once its token can no longer authenticate a
   request **and** it has nothing in flight, so a live execution can never lose its accounting.
   The map is still hard-bounded; when the bound is reached it is a *new* execution that is
   refused, never a running one that is evicted. See `_sweep_locked` and `MAX_TRACKED`.

**Reject, never queue.** Every one of the **six** rejections `admit` returns answers 429
immediately. Two further codes are not `admit`'s and are logged by `log_request_timeout` below,
for the deadline `app/middleware.py` arms: `sandbox_request_timeout` answers **504**, and
`sandbox_request_timeout_after_send` answers nothing at all because the response had already
begun — eight codes in total, and that function's docstring states what each one means. Queueing would hold the
request while the sandbox's ~120 second wall clock keeps running, which the script cannot
distinguish from slow data and cannot act on; worse, work admitted from a queue can complete
after the execution is already dead, which is exactly the wasted production
`genetics-results-suite-4h6.28` removed on the rejection path. A fast, labelled 429 lets a
script narrow its request or back off while it still has clock left.

**In-process, so `replicas: 1` is load-bearing** — the same way it is for db-api. At N replicas
these bound per replica, not per execution. `k8s/deployments/results-api.yaml` carries a comment
on `replicas: 1` saying so.

**Two limitations an operator must know, because these bound less than the rest of this
docstring implies:**

1. **They only bind a request that volunteers a token, and what makes that sufficient lives
   elsewhere.** `_sandbox_principal` in `app/middleware.py` reads the `Authorization` header;
   with no header it returns `None` and `admit` is never called, so the request is not counted
   at all. That was an exploitable hole (genetics-results-suite-0lf): seven `@is_public` routes
   answered 200 with no credential — measured, 20 of 20 header-less requests served with
   `_executions == {}` — and the sandbox's NetworkPolicy egress reaches `results-api:4000`
   **directly**, bypassing auth-gateway, so a script could shed all four counters by not sending
   the header.
   The **no-credential** half is closed in `app.dependencies.is_public_endpoint`, not here: with
   `ANONYMOUS_SURFACE_MINIMAL` on (the default, and forced by `SANDBOX_ENABLED`) the anonymous
   surface is `/healthz` alone, so every route touching a data path answers 401 to a request
   carrying nothing. Nothing in this module changed for it,
   and nothing here may be relaxed on the assumption that an anonymous request is harmless —
   `tests/test_anonymous_surface.py` is what keeps that assumption true.
   **The other half is still open.** `_sandbox_principal` accepts an HS256 sandbox token and
   nothing else, so presenting *a* credential is not the same as calling `admit`: a caller
   presenting `INTERNAL_API_SECRET` satisfies `is_internal_caller`, resolves as `mcp-tool`, and
   reaches the handler with none of these four counters touched — measured, 200 on
   `/api/v1/rsid/variants` and `/api/v1/variant_sets` with `_executions == {}`. The sandbox holds
   **The sandbox's half of that is now closed**: `genetics-results-suite-4h6.44` made the SDK
   build its client from the per-execution tokens and never attach `INTERNAL_API_SECRET`
   alongside or instead, and `genetics-results-suite-4h6.7` keeps the Deployment half — the
   sandbox pod is given no credentials at all (`k8s/deployments/sandbox.yaml`). What remains is
   **intentional and is not the sandbox's**: chat-backend, mcp-server and bff legitimately
   authenticate with the secret and none of them is a per-execution tenant, so an internal-secret
   caller inside the namespace is still served with none of these counters touched.
   `tests/test_anonymous_surface.py::test_the_internal_secret_path_survives_but_the_sdk_no_longer_takes_it`
   pins both halves.
   `app/core/limits.py`'s argument that omitting the header cannot buy a *looser* limit holds
   for the per-response byte cap only; for the four counters here, omitting it would buy **no**
   limit, which is why the anonymous surface has to be empty rather than merely capped.
2. **`sandbox_execution_tracker_full` is cross-tenant; the pod-wide concurrency bound now has a
   fairness rule and is not.** Both are pod-wide, so a caller that fills the map or holds the
   pod-wide slots locks *other* executions out — a denial surface, not merely a self-limit. The
   "23 chat turns/hour" sizing argument above is about honest volume and says nothing about an
   attacker.
   `SANDBOX_RESERVED_POD_SLOTS` closes the concurrency half (`genetics-results-suite-yv4`): an
   execution that already holds a pod-wide slot cannot take the last few, so two executions at
   their per-execution allowance can no longer occupy all of them and deny every newcomer its
   first request. **The newcomer's fairness is paid for by the incumbents**, which is inherent to
   a reservation and is not stated by the number 4 anywhere it is advertised: an execution
   reaches `SANDBOX_MAX_CONCURRENT_REQUESTS` in full only when it is alone. Measured at 8/4/2, 3
   other executions parked on one slot each cut a tenant to 3 concurrent, 4 parked to 2, 6 parked
   to 1. What that buys is bounded and worth stating exactly. Filling the pod takes
   `ceil((TOTAL - RESERVED) / PER_EXECUTION) + RESERVED` distinct executions — **four** at the
   shipped 8/4/2, against two before, by exhaustive search of the reachable states (`k`
   executions reach `k=1 -> 4`, `k=2 -> 6`, `k=3 -> 7`, `k=4 -> 8` in flight). So the guarantee
   is "no execution is denied its first concurrent request until at least `RESERVED` others each
   hold one", not "no execution is ever denied".
   **`sandbox_execution_tracker_full` remains cross-tenant with no fairness behind it**, and
   deliberately: refusing the newcomer is the direct cost of the fail-closed eviction rule in
   `_sweep_locked`, and the alternative — shedding the oldest idle entry — trades a bounded
   denial for an unbounded one, since a shed entry's execution can still authenticate and would
   come back with its budget reset. It is sized far above honest use so an honest execution
   never meets it, and `SANDBOX_REQUEST_TIMEOUT_SECONDS` bounds how long any one entry can stay
   unevictable.
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
    must stop the pod from starting instead. Two values read through here are not ceilings —
    `SANDBOX_RESERVED_POD_SLOTS` and `SANDBOX_REQUEST_TIMEOUT_SECONDS` — and the same floor is
    right for them for their own reasons, each stated at the constant.
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
# limit that already holds the gene maps and the search index. **Reachable in full only by a
# lone execution**: `SANDBOX_RESERVED_POD_SLOTS` below is paid for out of the incumbents'
# allowance, so with other executions parked on a slot each this ceiling is the smaller of 4 and
# what the reserve leaves — measured at 8/4/2, 3 parked executions cut a tenant to 3 concurrent,
# 4 parked to 2, and 6 parked to 1.
SANDBOX_MAX_CONCURRENT_REQUESTS = _env_int("SANDBOX_MAX_CONCURRENT_REQUESTS", 4)
# In-flight sandbox requests across the whole pod, i.e. across all executions. The sandbox is
# specified at `concurrency: 1` with a queue, so today this can only be reached by a single
# execution and the per-execution limit binds first; it exists so that raising the sandbox's own
# concurrency cannot silently multiply this pod's peak buffer. 8 x 16 MiB = 128 MiB.
SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL = _env_int("SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL", 8)
# Hard bound on the counter map itself, so a flood of distinct `jti`s cannot grow it without
# limit. At the measured peak of 23 chat turns/hour this is never approached; it is a backstop,
# not a working limit.
# LATENT, recorded rather than fixed: "entries live at most one token lifetime (~305s)" is an
# invariant of the **minter**, not of anything here. `expires_at` is the token's `exp` taken
# verbatim, and `verify_sandbox_token` bounds `iat` in the past (>= now - 300) but puts **no
# ceiling on `exp - now`**, so a token minted with a far-future `exp` produces an entry
# `_sweep_locked` will not touch for as long as that `exp` says. Only chat-backend mints, and it
# mints 300s tokens; enforcing a ceiling belongs in `app/core/sandbox_token.py`, not here.
SANDBOX_MAX_TRACKED_EXECUTIONS = _env_int("SANDBOX_MAX_TRACKED_EXECUTIONS", 4096)
# Pod-wide slots that only an execution with **nothing in flight** may take. Without it, the
# pod-wide bound is a cross-tenant denial surface: two executions at their per-execution
# allowance occupy all 8 slots and every other execution's *first* request is refused
# (genetics-results-suite-yv4). With it, an execution that already holds a slot stops at
# `TOTAL - RESERVED`, so filling the last slots takes as many distinct executions as there are
# reserved slots, and no execution can be denied its first concurrent request until at least
# `SANDBOX_RESERVED_POD_SLOTS` other executions each hold one. `_env_int`'s floor of 1 is the
# right floor here for the opposite reason to the ceilings above: 0 does not reject everything,
# it restores exactly the denial surface this exists to remove.
SANDBOX_RESERVED_POD_SLOTS = _env_int("SANDBOX_RESERVED_POD_SLOTS", 2)
# Wall clock one sandbox request may occupy a slot for. 120s is the sandbox's own hard ceiling
# (`docs/code-execution-security.md` section 4), so a request outliving it is producing a body
# no execution is still alive to read. Enforced in `SandboxResponseCapMiddleware`, whose
# `finally` releases the slot; see that docstring for why the deadline is armed there and not
# in uvicorn or an outer middleware.
SANDBOX_REQUEST_TIMEOUT_SECONDS = _env_int("SANDBOX_REQUEST_TIMEOUT_SECONDS", 120)

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

if SANDBOX_RESERVED_POD_SLOTS > (
    SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL - SANDBOX_MAX_CONCURRENT_REQUESTS
):
    # the reserve must not be able to bind *before* one execution reaches its own allowance:
    # with a single execution running, `_in_flight_total` is that execution's own `in_flight`,
    # so this inequality is exactly the condition under which a lone execution never meets the
    # reserve. Violated, the per-execution number in the manifest becomes unreachable — the same
    # lie the check above refuses, arrived at from the other side.
    raise ValueError(
        f"SANDBOX_RESERVED_POD_SLOTS ({SANDBOX_RESERVED_POD_SLOTS}) must be <= "
        f"SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL ({SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL}) - "
        f"SANDBOX_MAX_CONCURRENT_REQUESTS ({SANDBOX_MAX_CONCURRENT_REQUESTS}): a reserve that "
        "large would refuse a lone execution its own per-execution allowance. Raise the "
        "pod-wide bound or lower the reserve."
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

# Observability counters. Every rejection is a *denial of service to a caller*, and until these
# existed a log showed a `logger.warning` per rejection and nothing at all per admission, so an
# operator could not tell a denied hour from a quiet one, nor size 1000/4/8 against real traffic
# (genetics-results-suite-yv4 item 4). They are process-lifetime totals and high-water marks, not
# rates: `replicas: 1` and a restart-on-deploy make the pod's uptime the window, and every
# rejection log line carries the current values so the denial and its denominator arrive
# together.
_admitted_total = 0
_rejections: dict[str, int] = {}
_peak_in_flight_total = 0
_peak_in_flight_execution = 0
_peak_requests_execution = 0
_peak_tracked = 0


def _refuse(code: str, limit: int, observed: int, detail: str) -> Rejection:
    """Build a `Rejection` and count it. Callers hold `_lock`."""
    _rejections[code] = _rejections.get(code, 0) + 1
    return Rejection(code=code, limit=limit, observed=observed, detail=detail)


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
    global _in_flight_total, _admitted_total
    global _peak_in_flight_total, _peak_in_flight_execution, _peak_requests_execution
    global _peak_tracked
    jti = principal.execution_id
    with _lock:
        entry = _executions.get(jti)
        if entry is None:
            _sweep_locked(time.time())
            if len(_executions) >= SANDBOX_MAX_TRACKED_EXECUTIONS:
                # refuse the new execution rather than evict a live one: an evicted counter is a
                # reset budget, which is the failure that matters
                return _refuse(
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
            _peak_tracked = max(_peak_tracked, len(_executions))

        if entry.requests >= SANDBOX_MAX_REQUESTS_PER_EXECUTION:
            return _refuse(
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
            return _refuse(
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
            return _refuse(
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
            return _refuse(
                code="sandbox_concurrency_pod",
                limit=SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL,
                observed=_in_flight_total,
                detail=(
                    "Too many concurrent sandbox requests on this pod: "
                    f"{_in_flight_total} of {SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL} in flight. "
                    "Retry shortly."
                ),
            )
        if (
            entry.in_flight > 0
            and _in_flight_total
            >= SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL - SANDBOX_RESERVED_POD_SLOTS
        ):
            # the pod-wide bound alone lets two executions at their per-execution allowance take
            # every slot, so the party denied is a third execution that did nothing wrong. An
            # execution already holding a slot yields the last `SANDBOX_RESERVED_POD_SLOTS` to
            # executions with nothing in flight; it keeps what it holds — nothing is preempted,
            # because preemption would corrupt a running execution's accounting the way eviction
            # would, and queueing is refused here for the reasons in the module docstring.
            return _refuse(
                code="sandbox_concurrency_pod_share",
                limit=SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL - SANDBOX_RESERVED_POD_SLOTS,
                observed=_in_flight_total,
                detail=(
                    "This pod is near its concurrent-sandbox-request bound "
                    f"({_in_flight_total} of {SANDBOX_MAX_CONCURRENT_REQUESTS_TOTAL} in flight) "
                    f"and its last {SANDBOX_RESERVED_POD_SLOTS} slots are reserved for "
                    "executions with no request in flight. Await the requests this execution "
                    "has already issued before starting more."
                ),
            )

        entry.requests += 1
        entry.in_flight += 1
        _in_flight_total += 1
        _admitted_total += 1
        _peak_in_flight_total = max(_peak_in_flight_total, _in_flight_total)
        _peak_in_flight_execution = max(_peak_in_flight_execution, entry.in_flight)
        _peak_requests_execution = max(_peak_requests_execution, entry.requests)
        # Nothing may follow the three increments above inside this function. The caller's
        # "admitted" flag is set only once `admit` returns, so anything that can raise in between
        # strands a per-execution slot, a pod-wide slot and a map entry `_sweep_locked` will not
        # evict — permanently, since the caller's deadline is not armed yet either. That is why
        # the admission log is `log_admission` below, called by the middleware, rather than a
        # `logger.info` here: logging is not raise-free (a filter, a `LogRecordFactory` or a
        # handler whose `emit` bypasses `handleError` can raise, and this repo ships a Cloud
        # Logging handler), and the fix is to remove the shape rather than audit the instances.
        return None


def log_admission(principal: sandbox_token.SandboxPrincipal) -> None:
    """Log an execution's **first** admitted request, and nothing after it.

    One line per execution rather than per request: at the measured peak of 23 chat turns/hour
    this is the admission signal an operator needs to tell a denied hour from a quiet one, while
    a per-request line would be 1000x the volume for no extra information.

    Deliberately not called from `admit`, and deliberately safe to fail: it runs after the caller
    has recorded the admission, so a raise here costs a 500 and never a stranded slot. `requests`
    is incremented once per admission and never decremented, so `== 1` selects exactly one
    request per execution.
    """
    with _lock:
        entry = _executions.get(principal.execution_id)
        if entry is None or entry.requests != 1:
            return
        # snapshotted under the lock: `stats()` iterates `_rejections`, which `_refuse` mutates
        # under this same lock
        fields = stats()
    logger.info(
        "sandbox execution admitted",
        extra={"sid": principal.session_id, "jti": principal.execution_id, **fields},
    )


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
        # LATENT, recorded rather than fixed: these two clamps are asymmetric and both fail
        # open. The pod-wide counter is decremented *before* the `entry is None` return, so a
        # release with no entry still frees a pod-wide slot, and a double release manufactures
        # one; a missed release destroys one. Nothing detects either — there is no check that
        # `_in_flight_total == sum(e.in_flight for e in _executions.values())`. Unreachable
        # today: the middleware releases exactly once per admission, in a `finally`.
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


def stats() -> dict[str, int]:
    """Process-lifetime admission and denial counters, for logs and diagnostics.

    `_lock` is deliberately **not** taken: the values are independent ints read for a log line
    on a path that must not contend with `admit`, and a torn read costs an operator nothing.
    """
    return {
        "admitted": _admitted_total,
        # every refusal, of all three kinds: the six 429s `admit` returns, the 504s
        # `log_request_timeout` records, and the deadlines that fired too late to answer at all
        # (`sandbox_request_timeout_after_send`). `rejected_by_code` is what separates them.
        "rejected": sum(_rejections.values()),
        "rejected_by_code": dict(_rejections),
        "tracked": len(_executions),
        "in_flight_total": _in_flight_total,
        "peak_in_flight_total": _peak_in_flight_total,
        "peak_in_flight_execution": _peak_in_flight_execution,
        "peak_requests_execution": _peak_requests_execution,
        "peak_tracked": _peak_tracked,
    }


def reset() -> None:
    """Drop all accounting. Test-only; nothing in the request path calls this."""
    global _in_flight_total, _admitted_total
    global _peak_in_flight_total, _peak_in_flight_execution, _peak_requests_execution
    global _peak_tracked
    with _lock:
        _executions.clear()
        _in_flight_total = 0
        _admitted_total = 0
        _rejections.clear()
        _peak_in_flight_total = 0
        _peak_in_flight_execution = 0
        _peak_requests_execution = 0
        _peak_tracked = 0


# Rejections that deny a caller *other than the one at fault*. A per-execution limit is a
# self-limit and a WARNING; these three are pod-wide, so one of them is either an attack or a
# capacity signal and an operator has to see it above the warning noise.
CROSS_TENANT_CODES = frozenset(
    {
        "sandbox_execution_tracker_full",
        "sandbox_concurrency_pod",
        "sandbox_concurrency_pod_share",
    }
)


def log_rejection(rejection: Rejection, path: str | None, principal) -> None:
    level = logging.ERROR if rejection.code in CROSS_TENANT_CODES else logging.WARNING
    logger.log(
        level,
        "sandbox per-execution limit exceeded",
        extra={
            "path": path,
            "code": rejection.code,
            "limit": rejection.limit,
            "observed": rejection.observed,
            "sid": getattr(principal, "session_id", None),
            "jti": getattr(principal, "execution_id", None),
            # the counters travel with the denial: a rejection rate is meaningless without the
            # admissions it happened against, and this is the only line an operator sees. (This
            # `stats()` read is also outside `_lock`; see the note in `log_request_timeout` for
            # why that is recorded and not fixed here.)
            **stats(),
        },
    )


def log_request_timeout(path: str | None, principal, *, response_started: bool = False) -> None:
    """A sandbox request hit `SANDBOX_REQUEST_TIMEOUT_SECONDS`. Both outcomes are recorded here.

    **What a logged timeout means, in both directions, because the two are not interchangeable
    and an operator reads the difference:**

    * `sandbox_request_timeout` (ERROR) — the deadline fired with **nothing on the wire**. The
      caller got a 504 naming the deadline, and the request held a per-execution slot, a pod-wide
      slot and an unevictable map entry for the full timeout.
    * `sandbox_request_timeout_after_send` (WARNING) — the deadline fired **after** the response
      had begun, so the caller keeps its answer and **no 504 is sent**: a second
      `http.response.start` on a completed response is an ASGI protocol violation. Measured cause:
      a `yield` dependency whose teardown runs after the body. It is a WARNING and not an ERROR
      because nobody was served worse — but it is **not** free, and that is why it is counted: the
      slot was pinned for the full deadline exactly as above, so a caller that can arrange it
      holds the cheapest slot-pinning primitive in this module.

    The converse holds too and is the point of counting both: a request that hit the deadline is
    **never** silent. An unlogged request did not reach it.
    """
    code = "sandbox_request_timeout_after_send" if response_started else "sandbox_request_timeout"
    with _lock:
        _rejections[code] = _rejections.get(code, 0) + 1
    logger.log(
        logging.WARNING if response_started else logging.ERROR,
        "sandbox request deadline fired after the response was sent"
        if response_started
        else "sandbox request timed out",
        extra={
            "path": path,
            "code": code,
            "limit": SANDBOX_REQUEST_TIMEOUT_SECONDS,
            "sid": getattr(principal, "session_id", None),
            "jti": getattr(principal, "execution_id", None),
            # LATENT, recorded rather than fixed: `stats()` is called here **outside** `_lock`,
            # so its `sum(_rejections.values())` can race a concurrent `_refuse`. Removed only
            # from `admit`'s call site, where a raise would strand a slot; here and in
            # `log_rejection` a raise costs a 500 and nothing else. Believed unreachable in a
            # single-event-loop process — but nothing proves no other thread calls `admit`.
            **stats(),
        },
    )
