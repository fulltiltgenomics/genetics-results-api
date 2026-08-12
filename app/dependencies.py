"""
FastAPI dependency injection providers.

This module provides Depends() functions for injecting services into route handlers.
"""

import logging
from typing import TYPE_CHECKING

from fastapi import HTTPException, Request

import app.config.common as config
from app.core.auth import get_sandbox_principal, get_verified_user
from app.core.service_container import container
from app.services.gcloud_tabix_base import ensure_gcs_token  # noqa: F401 - re-exported for router dependencies

if TYPE_CHECKING:
    from app.services.request_util import RequestUtil
    from app.services.search_service import SearchIndex
    from app.services.data_access import DataAccess
    from app.services.data_access_coloc import DataAccessColoc
    from app.services.data_access_expression import DataAccessExpression
    from app.services.data_access_chromatin_peaks import DataAccessChromatinPeaks
    from app.services.data_access_open_chromatin import DataAccessOpenChromatin
    from app.services.data_access_variant_effect import DataAccessVariantEffect
    from app.services.data_access_mpra import DataAccessMpra
    from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
    from app.services.gene_group_service import GeneGroupService
    from app.services.gene_disease_data import GeneDiseaseData
    from app.services.phenotype_markdown_service import PhenotypeMarkdownService
    from app.services.credible_set_stats_service import CredibleSetStatsService
    from app.services.rsid_db import RsidDB
    from app.services.sumstats_data_access import SumstatsDataAccess
    from app.services.variant_annotation_service import VariantAnnotationService
    from app.services.variant_set_service import VariantSetService

logger = logging.getLogger(__name__)


# Routes that must still answer with no credential at all once the sandbox is deployed.
#
# `/healthz` is probed by the kubelet, which holds no credential of any kind and whose probes
# are exempt from NetworkPolicy on this cluster's dataplane, so it cannot be made to present
# one. Its handler is a constant JSON document that touches no data path.
#
# Nothing else belongs here. See `is_public_endpoint()` below for why the rest of the
# `@is_public` set stops being anonymous when `SANDBOX_ENABLED` is true.
ALWAYS_ANONYMOUS_PATHS = frozenset({"/healthz"})


def is_public_endpoint(request: Request) -> bool:
    """Whether this route may be served with no principal resolved at all.

    With `SANDBOX_ENABLED` false this is the full `@is_public` set — the shipped behaviour that
    predates the sandbox and that `app/core/limits.py` documents.

    With it true the anonymous surface collapses to `ALWAYS_ANONYMOUS_PATHS`. The four
    per-execution counters in `app/core/sandbox_budget.py` are admitted from the `Authorization`
    header, so a caller that sends none is counted against nothing; the sandbox reaches
    `results-api:4000` directly (its NetworkPolicy egress bypasses auth-gateway), so on an
    anonymous route a script could shed every per-execution bound simply by omitting the header.

    **This closes the no-credential path of genetics-results-suite-0lf and only that path.** It
    is *not* true that the only way into a handler is to present a credential whose presentation
    calls `admit`. `_sandbox_principal` accepts an HS256 sandbox token and nothing else, whereas
    `INTERNAL_API_SECRET` satisfies `is_internal_caller`, so a caller presenting the internal
    secret resolves as `mcp-tool`, reaches the handler, and is accounted against **nothing** —
    measured, 200 on `/api/v1/rsid/variants` and `/api/v1/variant_sets` with the counter map still
    empty. The sandbox is handed that secret today and the SDK attaches it to every request, so as
    things stand this converts "omit the header" into "send the other header". The residual path
    closes when genetics-results-suite-4h6.7 stops giving the sandbox `INTERNAL_API_SECRET` (the
    Deployment) and genetics-results-suite-4h6.14 makes the SDK send the per-execution token (the
    transport) — neither of which lives in this module.
    `tests/test_anonymous_surface.py::test_an_internal_secret_caller_is_served_but_not_accounted`
    pins the residue and is expected to fail once those two land.

    Requiring a principal costs the browser nothing, and that is load-bearing rather than
    incidental:
    `k8s/deployments/auth-gateway.yaml` sends every browser `/api/` request through
    `auth_request /oauth2/auth` to the BFF, which attaches the shared secret as its own
    `Authorization: Bearer` on the upstream call (`bff/upstream.ts`), and routes a programmatic
    client's own bearer to `@api_bearer`. No legitimate caller of these routes is anonymous in
    production.

    Gated on `SANDBOX_ENABLED` rather than applied unconditionally so that the deploy which
    creates the sandbox is the one that activates it — the same flip
    genetics-results-suite-4h6.7 must make in the commit that lands the workload. Until then
    this answers exactly what it always did, so nothing about today's production behaviour
    changes.
    """
    route = request.scope.get("route")
    if not (route and getattr(route.endpoint, "is_public", False)):
        return False
    if config.sandbox_enabled:
        return getattr(route, "path", None) in ALWAYS_ANONYMOUS_PATHS
    return True


def public_route_paths() -> frozenset[str]:
    """Every route path carrying the `@is_public` marker, read from the live route table.

    Derived rather than listed so it cannot rot: a route that gains the decorator shows up here
    on the next import, and `tests/test_anonymous_surface.py` pins the resulting anonymous
    surface in both `SANDBOX_ENABLED` states, so adding one is a deliberate act with a failing
    test attached rather than a silent widening.
    """
    from app.server import app as _app

    return frozenset(
        route.path
        for route in _app.routes
        if getattr(getattr(route, "endpoint", None), "is_public", False)
    )


async def auth_required(request: Request) -> str | None:
    # Resolved ahead of both short circuits below, because the response caps in
    # app/core/limits.py key on it: a script must not be able to shed the tight limits by
    # picking a route that needs no credential, or by running against a REQUIRE_AUTH=false
    # deployment. A sandbox-shaped bearer that does not validate still raises 401 here, which
    # is the same answer it already gets on every authenticated route.
    sandbox = get_sandbox_principal(request)
    if sandbox is not None:
        request.state.sandbox_principal = sandbox

    if not config.require_auth:
        return None
    if is_public_endpoint(request):
        return None
    user = get_verified_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    # store for usage logging middleware to pick up (bearer token users
    # don't have the X-Goog-Authenticated-User-Email header)
    request.state.authenticated_user = user
    return user


def is_public(func):
    setattr(func, "is_public", True)
    return func


def get_request_util() -> "RequestUtil":
    """Get RequestUtil service instance."""
    return container.get("request_util")


def get_search_index() -> "SearchIndex":
    """Get SearchIndex service instance."""
    return container.get("search_index")


def get_data_access() -> "DataAccess":
    """Get DataAccess service instance."""
    return container.get("data_access")


def get_data_access_coloc() -> "DataAccessColoc":
    """Get DataAccessColoc service instance."""
    return container.get("data_access_coloc")


def get_data_access_expression() -> "DataAccessExpression":
    """Get DataAccessExpression service instance."""
    return container.get("data_access_expression")


def get_data_access_chromatin_peaks() -> "DataAccessChromatinPeaks":
    """Get DataAccessChromatinPeaks service instance."""
    return container.get("data_access_chromatin_peaks")


def get_data_access_open_chromatin() -> "DataAccessOpenChromatin":
    """Get DataAccessOpenChromatin service instance."""
    return container.get("data_access_open_chromatin")


def get_data_access_variant_effect() -> "DataAccessVariantEffect":
    """Get DataAccessVariantEffect service instance."""
    return container.get("data_access_variant_effect")


def get_data_access_mpra() -> "DataAccessMpra":
    """Get DataAccessMpra service instance."""
    return container.get("data_access_mpra")


def get_gene_name_mapping() -> "GeneNameAndPositionMapping":
    """Get GeneNameAndPositionMapping service instance."""
    return container.get("gene_name_mapping")


def get_gene_group_service() -> "GeneGroupService":
    """Get GeneGroupService service instance."""
    return container.get("gene_group_service")


def get_gene_disease_data() -> "GeneDiseaseData":
    """Get GeneDiseaseData service instance."""
    return container.get("gene_disease_data")


def get_phenotype_markdown_service() -> "PhenotypeMarkdownService":
    """Get PhenotypeMarkdownService instance."""
    return container.get("phenotype_markdown_service")


def get_credible_set_stats_service() -> "CredibleSetStatsService":
    """Get CredibleSetStatsService instance."""
    return container.get("credible_set_stats_service")


def get_rsid_db() -> "RsidDB":
    """Get RsidDB service instance."""
    return container.get("rsid_db")


def get_sumstats_data_access() -> "SumstatsDataAccess":
    """Get SumstatsDataAccess service instance."""
    return container.get("sumstats_data_access")


def get_variant_annotation_service() -> "VariantAnnotationService":
    """Get VariantAnnotationService instance."""
    return container.get("variant_annotation_service")


def get_variant_set_service() -> "VariantSetService":
    """Get VariantSetService instance."""
    return container.get("variant_set_service")
