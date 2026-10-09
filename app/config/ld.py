"""Settings for the LD proxy.

This is the API's first live third-party HTTP dependency on the request path (auth's token
verification aside), which is the whole reason the endpoint exists: the sandbox has no DNS and
no internet egress by design, so a script cannot reach the FinnGen LD server itself and
results-api — which is reachable from the sandbox and is not itself confined — stands in.

Everything here is env-overridable because the upstream is someone else's service: a URL move
or an outage that needs the timeout cut must not need a rebuild.
"""

import os

from app.config.profile import load_profile_module

# the upstream the proxy fronts. A full URL rather than a host, so a move to a different path
# needs no code change.
ld_upstream_url = os.environ.get("LD_UPSTREAM_URL", "https://api.finngen.fi/api/ld")

# Bounds the whole upstream exchange. 30s matches what the MCP tool layer used when it called
# the upstream directly, so the proxy does not silently change the deadline callers were
# already written against. A sandbox script has ~120s of wall clock in total.
ld_upstream_timeout_seconds = float(os.environ.get("LD_UPSTREAM_TIMEOUT_SECONDS", "30"))

# The upstream's own bounds: outside them it answers 400 "window must be between 100000 and
# 5000000". They are repeated here so the refusal is a 422 that names the limit, rather than
# a 502 a caller reads as an outage. An upstream that widens its bounds makes these too
# strict, and narrowing them brings the 502 back. Callers are refused rather than clamped,
# because a silently narrowed window returns fewer variants and looks like a sparse locus.
ld_min_window = int(os.environ.get("LD_MIN_WINDOW", str(100_000)))
ld_max_window = int(os.environ.get("LD_MAX_WINDOW", str(5_000_000)))

ld_default_window = int(os.environ.get("LD_DEFAULT_WINDOW", str(1_500_000)))

# No allow-list of panel names: the set is the upstream's to define and this service cannot
# enumerate it without guessing, so a wrong panel is the upstream's 4xx to answer rather than
# a list here that goes stale. The shape is validated instead — see PANEL_PATTERN in the
# router — which is what keeps an arbitrary string out of the outbound query.
ld_default_panel = os.environ.get("LD_DEFAULT_PANEL", "sisu42")

# The statistics an LD source may carry. `r2` is mandatory on every source because the
# threshold filters on it; `r` (signed, needed by a correlated-instrument estimator) and
# `d_prime` are what a particular source happens to have. A caller picks any subset with
# `fields=`; asking a source for a statistic it lacks is a 422 that names what it has.
LD_STATISTICS = ("r", "r2", "d_prime")

# the key columns every panel file starts with; the statistics follow, in `fields` order
LD_KEY_COLUMNS = (b"chr", b"pos", b"variant1", b"variant2")

# panels served from the suite's own tabix-indexed matrices rather than relayed from the
# upstream. Profile-specific because the files are: see app/config/profiles/*/ld.py.
_profile = load_profile_module("ld")
ld_panels: list[dict] = _profile.ld_panels


def _validate_panels(panels: list[dict]) -> None:
    """Refuse at import a panel the request path could not serve correctly.

    `r2` must be among the fields (the threshold and the floor are r² values), every field
    must be a known statistic (an unknown one would be served under a name no caller can
    ask for), and names must be unique (a lookup by name must have one answer).
    """
    seen: set[str] = set()
    for cfg in panels:
        name = cfg["panel"]
        if name in seen:
            raise ValueError(f"LD panel {name!r} is configured twice")
        seen.add(name)
        fields = tuple(cfg["fields"])
        unknown = [f for f in fields if f not in LD_STATISTICS]
        if unknown or "r2" not in fields:
            raise ValueError(
                f"LD panel {name!r}: fields must be a subset of {LD_STATISTICS} including "
                f"'r2', got {fields}"
            )
        if len(set(fields)) != len(fields):
            raise ValueError(f"LD panel {name!r}: fields repeat: {fields}")
        if "{chrom}" not in cfg["file_template"]:
            raise ValueError(f"LD panel {name!r}: file_template must contain '{{chrom}}'")


_validate_panels(ld_panels)


def panel_config(panel: str) -> dict | None:
    for cfg in ld_panels:
        if cfg["panel"] == panel:
            return cfg
    return None


def panel_file(cfg: dict, chrom: str) -> str:
    """The file holding every pair whose first variant is on `chrom` (spelled as the
    panel's chromosome list spells it, e.g. "22" or "X")."""
    return cfg["file_template"].format(chrom=chrom)


def panel_header(cfg: dict) -> list[bytes]:
    """The exact header every file of the panel must carry (startup_checks proves it)."""
    return [*LD_KEY_COLUMNS, *(f.encode() for f in cfg["fields"])]
