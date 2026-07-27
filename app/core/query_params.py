"""Reject query parameters an endpoint does not declare.

FastAPI silently ignores undeclared query parameters. That made client/API drift
invisible: the MCP tools advertised a `data_types` filter that no endpoint ever
implemented, so callers asking for one association type kept receiving every type
and had no way to tell the filter had not been applied. Failing loudly turns that
class of bug into an immediate 422 instead of quietly wrong data.
"""

import logging

from fastapi import HTTPException, Request
from fastapi.dependencies.models import Dependant

logger = logging.getLogger(__name__)


def _declared_query_params(dependant: Dependant, seen: set[int] | None = None) -> set[str]:
    """Collect query param aliases from a dependant and everything it depends on."""
    if seen is None:
        seen = set()
    if id(dependant) in seen:
        return set()
    seen.add(id(dependant))

    names = {field.alias for field in dependant.query_params}
    for sub in dependant.dependencies:
        names |= _declared_query_params(sub, seen)
    return names


def reject_unknown_query_params(request: Request) -> None:
    """Fail with 422 when the request carries a query param the route does not declare."""
    route = request.scope.get("route")
    dependant = getattr(route, "dependant", None)
    if dependant is None:  # non-API routes (static, mounts) have no dependant
        return

    declared = getattr(route, "_declared_query_params", None)
    if declared is None:
        declared = _declared_query_params(dependant)
        route._declared_query_params = declared

    unknown = sorted(set(request.query_params.keys()) - declared)
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Unknown query parameter(s): {', '.join(unknown)}. "
                f"Accepted for this endpoint: {', '.join(sorted(declared)) or 'none'}"
            ),
        )
