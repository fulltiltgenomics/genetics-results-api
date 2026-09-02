"""Settings for the LD proxy.

This is the API's first live third-party HTTP dependency on the request path (auth's token
verification aside), which is the whole reason the endpoint exists: the sandbox has no DNS and
no internet egress by design, so a script cannot reach the FinnGen LD server itself and
results-api — which is reachable from the sandbox and is not itself confined — stands in.

Everything here is env-overridable because the upstream is someone else's service: a URL move
or an outage that needs the timeout cut must not need a rebuild.
"""

import os

# the upstream the proxy fronts. A full URL rather than a host, so a move to a different path
# needs no code change.
ld_upstream_url = os.environ.get("LD_UPSTREAM_URL", "https://api.finngen.fi/api/ld")

# Bounds the whole upstream exchange. 30s matches what the MCP tool layer used when it called
# the upstream directly, so the proxy does not silently change the deadline callers were
# already written against. A sandbox script has ~120s of wall clock in total.
ld_upstream_timeout_seconds = float(os.environ.get("LD_UPSTREAM_TIMEOUT_SECONDS", "30"))

# The pair path in the tool layer computes window = 2 * distance + 1 Mb over a distance capped
# at 5 Mb, so 11 Mb is the largest window any current caller can ask for. Callers are refused
# above it rather than having it clamped, because a silently narrowed window returns fewer
# variants and looks like a sparse locus.
ld_max_window = int(os.environ.get("LD_MAX_WINDOW", str(11_000_000)))

ld_default_window = int(os.environ.get("LD_DEFAULT_WINDOW", str(1_500_000)))

# No allow-list of panel names: the set is the upstream's to define and this service cannot
# enumerate it without guessing, so a wrong panel is the upstream's 4xx to answer rather than
# a list here that goes stale. The shape is validated instead — see PANEL_PATTERN in the
# router — which is what keeps an arbitrary string out of the outbound query.
ld_default_panel = os.environ.get("LD_DEFAULT_PANEL", "sisu42")
