"""
Common configuration settings used across the application.

This module contains general settings like authentication, database paths,
chunk sizes, and other common constants.
"""

import logging
import os

from app.config.profile import load_profile_module

log_level = "INFO"
deploy_env = os.environ.get("DEPLOY_ENV", "dev1")
log_source = os.environ.get("LOG_SOURCE", f"genetics-results-api-{deploy_env}")

# use Cloud Logging API directly on VM
# on GKE, stdout is captured automatically so this should be False
use_cloud_logging_api = deploy_env.startswith("dev")

# usage logging (for BigQuery export via GCP log sink)
usage_logging_enabled = True
usage_logging_excluded_paths = {
    "/healthz",
    "/api/v1/docs",
    "/api/v1/redoc",
    "/api/v1/openapi.json",
    "/favicon.ico",
}

# when True, require X-Goog-Authenticated-User-Email header (set by IAP or oauth2-proxy)
# set REQUIRE_AUTH=true in environments where IAP/oauth2-proxy is in front of the service
require_auth = os.environ.get("REQUIRE_AUTH", "false").lower() in ("1", "true", "yes")

# shared secret for internal service-to-service auth
internal_api_secret = os.environ.get("INTERNAL_API_SECRET", "")


def require_ascii_internal_secret(secret: str) -> None:
    """Refuse a non-ASCII INTERNAL_API_SECRET (`genetics-results-suite-ctq`).

    Measured off a real socket, HTTP clients do not agree on how to put a non-ASCII header
    value on the wire: node fetch/undici (the browser BFF) and python-requests send latin-1,
    aiohttp sends utf-8, and httpx 0.28 refuses to send one at all (UnicodeEncodeError in the
    client). No server-side codec is therefore correct for every caller, so byte-exactness is
    unachievable in general and the only well-defined rule is that the secret is ASCII, where
    every client agrees and every codec coincides.

    Failing at startup is the good failure mode: the new pod never passes readiness, the
    rollout stalls with the old pods still serving, and the message names the variable —
    versus every internal call 401ing at request time with nothing local saying why.

    Silent when the secret is absent or empty: that is the dev/test configuration, and
    `is_internal_caller` already fails closed there.
    """
    if secret and not secret.isascii():
        raise RuntimeError(
            "INTERNAL_API_SECRET contains non-ASCII characters. HTTP clients disagree on how "
            "to encode a non-ASCII header value (node/undici and python-requests send latin-1, "
            "aiohttp sends utf-8, httpx refuses to send one at all), so no server-side decoding "
            "recovers the same secret from every caller. Set INTERNAL_API_SECRET to an ASCII "
            "value — scripts/create-secrets.sh generates one with `openssl rand -base64 32`."
        )


require_ascii_internal_secret(internal_api_secret)

# chat backend URL for user token validation (optional, empty = disabled)
chat_backend_url = os.environ.get("CHAT_BACKEND_URL", "")

# signing key for the per-execution sandbox tokens (docs/code-execution-security.md §4 in
# genetics-results-suite). Deliberately NOT internal_api_secret: separate key, separate blast
# radius. Unset means every sandbox-shaped bearer is rejected — never accepted with a warning.
sandbox_token_signing_key = os.environ.get("SANDBOX_TOKEN_SIGNING_KEY", "")

# true once the sandbox Deployment exists. A separate required input rather than a derivation
# from the signing key: with neither secret set, a sandbox script that simply omits the
# Authorization header would be served as an unauthenticated caller. When this is true and
# either secret is missing, the service refuses to start (app/core/sandbox_token.py).
sandbox_enabled = os.environ.get("SANDBOX_ENABLED", "").strip().lower() in ("1", "true", "yes")

# whether the anonymous surface is the minimal one (`/healthz` alone) rather than the full
# `@is_public` set. Separate from `sandbox_enabled` because that variable is an INCIDENT lever —
# "turn the sandbox off" — and this one is a SECURITY lever; keying the second on the first made
# a routine `SANDBOX_ENABLED=false` re-open six routes to anonymous callers, which is failing
# open on the action an operator takes under pressure (genetics-results-suite-rhh).
# `sandbox_enabled` still FORCES it in `app.dependencies.is_public_endpoint`, so the sandbox can
# never run with the wide surface; this only lets the surface be narrow WITHOUT the sandbox.
# The parse is inverted relative to every other boolean here: unset means true, and only an
# explicit false-y value turns it off, so a typo (`ANONYMOUS_SURFACE_MINIMAL=flase`) fails safe
# instead of silently widening the surface. A function rather than an inline expression so the
# default and the typo case can be tested without re-importing this module.
#
# The falsey spellings are deliberately wider than `sandbox_enabled`'s truthy set eighteen lines
# up. This variable is documented in `k8s/deployments/results-api.yaml` as a break-glass, and
# `off` is what an operator types under pressure — with only `0/false/no` accepted, `off`,
# `disabled` and `n` all left the surface narrow while looking like they had widened it. That is
# the same silent-failure shape genetics-results-suite-rhh was filed to fix, one layer down. An
# unrecognised value keeps the fail-safe default AND logs, so a typo is visible rather than mute.
_SURFACE_MINIMAL_OFF = frozenset({"0", "false", "no", "off", "disabled", "n", "f"})
_SURFACE_MINIMAL_ON = frozenset({"1", "true", "yes", "on", "enabled", "y", "t"})


def _parse_anonymous_surface_minimal() -> bool:
    value = os.environ.get("ANONYMOUS_SURFACE_MINIMAL", "").strip().lower()
    if not value:
        return True
    if value in _SURFACE_MINIMAL_OFF:
        return False
    if value in _SURFACE_MINIMAL_ON:
        return True
    logging.getLogger(__name__).warning(
        "ANONYMOUS_SURFACE_MINIMAL is set to %r, which is not a recognised boolean; assuming ON "
        "(the anonymous surface stays minimal: /healthz only). To widen it, set one of %s.",
        value,
        ", ".join(sorted(_SURFACE_MINIMAL_OFF)),
    )
    return True


anonymous_surface_minimal = _parse_anonymous_surface_minimal()

# bearer token auth: allowed email domains and specific emails
allowed_email_domains = {
    d.strip() for d in os.environ.get("ALLOWED_EMAIL_DOMAINS", "finngen.fi").split(",") if d.strip()
}
allowed_emails = {
    e.strip() for e in os.environ.get("ALLOWED_EMAILS", "").split(",") if e.strip()
}

# OAuth client id(s) a Google Identity Token must be addressed to (its `aud` claim).
# id_token.verify_oauth2_token skips audience verification when no audience is passed, so
# without this ANY Google-signed id_token belonging to an allow-listed email is accepted,
# including one minted for an unrelated third-party app the user signed into.
google_token_audience = {
    a.strip() for a in os.environ.get("GOOGLE_TOKEN_AUDIENCE", "").split(",") if a.strip()
}

# data paths and profile-specific settings loaded from the active profile
_profile = load_profile_module("common")

hgnc_file = _profile.hgnc_file
rsid_db = _profile.rsid_db
gnomad = _profile.gnomad
dataset_to_resource = _profile.dataset_to_resource
dataset_mapping_files = _profile.dataset_mapping_files
dataset_display_names = _profile.dataset_display_names
variant_set_files = _profile.variant_set_files
variant_annotation_sources = _profile.variant_annotation_sources
phenotype_markdown_template = _profile.phenotype_markdown_template

# CORS settings: profile-specific origins + any extra from env var
cors_origins = _profile.cors_origins

read_chunk_size = 16 * 1024  # 16KB
response_chunk_size = 64 * 1024  # 64KB
max_range_size_stream = 1e7  # 10Mb
max_range_size_json = 5e6  # 5Mb
max_gene_window = 3e6  # 3Mb
max_query_variants = 2000

# The suite's shared definition of a coding variant, matched against `most_severe`. The other
# four copies are genetics-mcp-server's sdk/plots.py `_CODING_CONSEQUENCES` and the chat
# prompt's Terminology block, and genetics-results-browser's src/utils/coding.ts and
# bff/coding.ts; changing one means changing all five.
#
# Terms whose SO name carries no `_variant` suffix are written without one — `transcript_ablation`
# is the value the annotation actually holds, so a suffixed spelling matches nothing.
# `synonymous_variant` and `coding_sequence_variant` are deliberately absent: neither changes
# the protein, which is what this set is for.
coding_set = set(
    [
        "missense_variant",
        "frameshift_variant",
        "inframe_insertion",
        "inframe_deletion",
        "transcript_ablation",
        "stop_gained",
        "stop_lost",
        "start_lost",
        "splice_acceptor_variant",
        "splice_donor_variant",
        "incomplete_terminal_codon_variant",
        "protein_altering_variant",
    ]
)
