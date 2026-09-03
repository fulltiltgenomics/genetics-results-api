"""Front the FinnGen LD server for callers that cannot reach it themselves.

WHY THIS IS A PROXY AND NOT A DATASET. The LD is not ours and is not a file we serve; the
upstream computes it. What this adds is reachability: the sandbox has no DNS and no internet
egress — the design of record in genetics-results-suite `k8s/network-policies/sandbox-policy.yaml`,
load-bearing against exfiltration — so a `run_analysis` script's `genetics.ld(...)` resolved
nothing and every locuszoom came out grey. results-api is reachable from the sandbox and is not
itself confined, so it can make the call the script cannot.

WHAT IT DELIBERATELY DOES NOT DO. It does not reinterpret. The upstream's `ld` entries are
passed through with their own field names, so the semantics — which of `variation1`/`variation2`
is the query, what `r2` is computed over — live in exactly one place, the caller that already
knew them. Only the fields this service documents survive, so an upstream that grows a field
does not start leaking it silently.

NO CACHE, deliberately and provisionally: a locuszoom re-run asks for the same window twice and
would benefit, but a cache here needs an eviction policy and a memory bound on a `replicas: 1`
pod that already holds the gene maps and the search index. Left out until the request pattern
is measured rather than guessed at.
"""

import logging

import aiohttp

from app.config import ld as ld_config
from app.core.exceptions import DataException

logger = logging.getLogger(__name__)

# the fields the proxy is contracted to return. An upstream that adds one does not start
# forwarding it: a field not listed here does not exist, the same rule the sandbox wire
# contract states for its own shape.
LD_ENTRY_FIELDS = ("variation1", "variation2", "r2", "d_prime")


class LDUpstreamError(DataException):
    """The upstream refused, failed or could not be reached.

    Distinct from a caller error so the router can answer 502 rather than 4xx: nothing the
    caller sent is wrong, and a script that retries a 4xx forever against a healthy request is
    the failure this separation exists to avoid.
    """

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class LDService:
    """One aiohttp session, opened on first use and closed by the lifespan's cleanup pass.

    The field is named `_upstream_session` rather than the obvious short form on purpose:
    tests/test_gcs_session_guard.py greps all of app/ for the GCS client's two backing-field
    names, to keep them behind their accessors. That sweep is worth more unconditional than
    carrying an exemption for a class with nothing to do with GCS — including in prose, which
    is why this paragraph does not spell the names out either.
    """

    _upstream_session: aiohttp.ClientSession | None = None

    def _ensure_upstream_session(self) -> aiohttp.ClientSession:
        # opened lazily for the same reason GCloudTabixBase does it: a pod that never serves
        # an LD request should not hold a session, and construction happens on the event loop
        # at container-resolution time where opening one is not free.
        if self._upstream_session is None or self._upstream_session.closed:
            self._upstream_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=ld_config.ld_upstream_timeout_seconds)
            )
        return self._upstream_session

    async def cleanup(self) -> None:
        """Called by the lifespan shutdown; must never open a session."""
        if self._upstream_session and not self._upstream_session.closed:
            await self._upstream_session.close()

    async def variants_in_ld(
        self, variant: str, *, window: int, r2_threshold: float, panel: str
    ) -> list[dict]:
        """The upstream's `ld` entries for one query variant, trimmed to LD_ENTRY_FIELDS."""
        params = {
            "variant": variant,
            "window": str(window),
            "panel": panel,
            "r2_thresh": str(r2_threshold),
        }
        session = self._ensure_upstream_session()
        try:
            async with session.get(ld_config.ld_upstream_url, params=params) as resp:
                if resp.status != 200:
                    # the upstream's body is not forwarded: it is a third party's text and
                    # goes to a caller that may be a model-authored script
                    body = (await resp.text())[:200]
                    logger.warning(
                        "LD upstream answered HTTP %s for variant=%s panel=%s: %s",
                        resp.status, variant, panel, body,
                    )
                    raise LDUpstreamError(
                        f"the LD server answered HTTP {resp.status}", status=resp.status
                    )
                payload = await resp.json(content_type=None)
        except LDUpstreamError:
            raise
        except aiohttp.ClientError as exc:
            logger.warning("LD upstream unreachable for variant=%s: %s", variant, exc)
            raise LDUpstreamError("the LD server could not be reached") from exc
        except TimeoutError as exc:
            logger.warning(
                "LD upstream timed out after %ss for variant=%s",
                ld_config.ld_upstream_timeout_seconds, variant,
            )
            raise LDUpstreamError(
                f"the LD server did not answer within "
                f"{ld_config.ld_upstream_timeout_seconds:g}s"
            ) from exc

        if not isinstance(payload, dict):
            raise LDUpstreamError("the LD server returned a body that is not an object")
        entries = payload.get("ld")
        if entries is None:
            return []
        if not isinstance(entries, list):
            raise LDUpstreamError("the LD server returned an `ld` field that is not a list")
        return [
            {field: entry.get(field) for field in LD_ENTRY_FIELDS}
            for entry in entries
            if isinstance(entry, dict)
        ]
