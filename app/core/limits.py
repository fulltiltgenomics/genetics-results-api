"""Per-credential response caps.

Design of record: `docs/code-execution-security.md` section 4 in genetics-results-suite.

results-api serves the bulkiest payloads in the suite (summary-statistic ranges, LD), so it
carries its own response-byte cap rather than inheriting db-api's, which are expressed in
BigQuery bytes billed and mean nothing here.

**Bytes only.** There was a 25 000-row cap here too, mirroring db-api's. It is gone: enforcing
it meant `json.loads` over the whole buffered body just to count, an object graph several times
the byte size, on the event loop of a `replicas: 1` pod — a memory amplifier only a sandbox
caller could trigger, which made presenting the token worse for the service than omitting it.
It also never bound the payloads it was written for: the counter saw only JSON, and TSV is the
default `format` of every bulk range endpoint. The byte cap was already the binding one, so
nothing an operator relied on changes.

**The relax condition is broader than db-api's, deliberately.** db-api has exactly one caller
and one auth path, so "verified non-sandbox" there is a shared-secret comparison. Here it is
*any* successfully verified non-sandbox principal — the shared secret, a verified Google
id_token, or a per-user chat API token — because `k8s/deployments/auth-gateway.yaml`'s
``@api_bearer`` location routes programmatic clients straight to results-api with their own
token and deliberately no shared secret. An hmac-only rule would put verified humans on the
sandbox defaults on exactly the endpoints they come here for.

**Why the rule below reduces to "sandbox or not".** On a route that requires a credential,
`auth_required` answers 401 for a missing or unverified one, so the only requests that reach a
handler are the sandbox and the verified non-sandbox principals — which is the spec's rule
exactly. Two cases reach a handler with no principal resolved, and each is handled on its own
terms rather than by defaulting:

* an ``@is_public`` route. Re-derive the list with ``grep -rn "@is_public" app/``; today it is
  seven: ``/api/v1`` and ``/healthz`` (`app/server.py`), ``/api/v1/auth``,
  ``/api/v1/variant_sets``, ``/api/v1/variant_sets/{name}``, and ``/api/v1/rsid/variants`` GET
  and POST. `auth_required` returns before `get_verified_user`, so no principal exists to relax
  on. These are relaxed. Measured, tight caps there would truncate nothing today — the largest
  public response the code can produce is 888 rows / 18.6 KB.

  The reason relaxing them carries **zero security delta** is that every one of these routes is
  bounded in its own handler for *every* caller, so the byte cap is not what stands between a
  script and a large public response. The one route that was not — ``POST /rsid/variants``,
  which read an unbounded body and answered one object per id — now bounds the id count in
  `app.routers.rsid.MAX_RSIDS`, uniformly and with no sandbox special case. That uniform bound
  is what makes the invariant hold: omitting the sandbox header can no longer buy a *looser*
  limit than presenting it. (Presenting it does still cap the response here, because the
  sandbox principal is resolved ahead of the public-route short circuit in
  `app.dependencies.auth_required` — but the invariant must not rest on that alone, since it
  only tightens the caller that chose to identify itself.) Evidence: section 4 of
  `docs/code-execution-security.md` in genetics-results-suite.
* ``REQUIRE_AUTH=false`` — local development only; the shipped deployment sets it true. Dev
  gets the operator-configured limits so a local run does not fail in ways production never
  would, and a sandbox token still gets the tight ones so the caps remain testable locally.

Neither case lets a caller widen its limits by presenting a *weaker* credential: presenting a
sandbox token never yields more than presenting the shared secret, and dropping a verified
credential on a route that requires one yields a 401, not a wider cap.
"""

import os
from dataclasses import dataclass

# A summary-statistic row is wide: 25 000 of them serialize to roughly 5 MB. 16 MiB leaves that
# comfortable headroom while still bounding the buffer the cap is enforced in, and is far above
# the 64 KiB a script returns to the model after aggregating in-pod (section 2).
SANDBOX_MAX_RESPONSE_BYTES = int(os.environ.get("SANDBOX_MAX_RESPONSE_BYTES", str(16 * 1024**2)))


@dataclass(frozen=True)
class Caps:
    max_response_bytes: int | None

    @property
    def enforced(self) -> bool:
        return self.max_response_bytes is not None


# The operator-configured case is *no ceiling*: unlike db-api, results-api has no MAX_ROWS or
# MAX_BYTES_BILLED — neither the manifest nor the code defines one — so "relaxed" here means
# exactly the behaviour every caller has today, and this path never buffers or inspects a
# response.
RELAXED = Caps(None)


def sandbox_caps() -> Caps:
    return Caps(SANDBOX_MAX_RESPONSE_BYTES)


def caps_for_scope(scope) -> Caps:
    """The caps this ASGI request runs under. See the module docstring for the rule."""
    principal = (scope.get("state") or {}).get("sandbox_principal")
    return sandbox_caps() if principal is not None else RELAXED
