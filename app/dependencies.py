"""
FastAPI dependency injection providers.

This module provides Depends() functions for injecting services into route handlers.
"""

import logging
from typing import TYPE_CHECKING

from fastapi import Depends, HTTPException, Request

import app.config.common as config
from app.core.auth import get_current_user
from app.core.service_container import container

if TYPE_CHECKING:
    from app.services.request_util import RequestUtil
    from app.services.search_service import SearchIndex
    from app.services.data_access import DataAccess
    from app.services.data_access_coloc import DataAccessColoc
    from app.services.data_access_expression import DataAccessExpression
    from app.services.data_access_chromatin_peaks import DataAccessChromatinPeaks
    from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
    from app.services.gene_disease_data import GeneDiseaseData
    from app.services.phenotype_markdown_service import PhenotypeMarkdownService

logger = logging.getLogger(__name__)


def is_public_endpoint(request: Request) -> bool:
    route = request.scope.get("route")
    if route and getattr(route.endpoint, "is_public", False):
        return True
    return False


async def auth_required(
    request: Request,
    user: str | None = Depends(get_current_user),
) -> str:
    if not config.authentication:
        return None
    if is_public_endpoint(request):
        return None
    if user is None:
        # log only safe headers to avoid exposing sensitive data
        safe_headers = {
            "host": request.headers.get("host"),
            "user-agent": request.headers.get("user-agent"),
            "x-forwarded-for": request.headers.get("x-forwarded-for"),
        }
        logger.debug(f"Auth required but no user: {safe_headers}")
        host = request.headers.get("x-forwarded-host") or request.headers.get(
            "host", "localhost:4000"
        )
        scheme = request.headers.get("x-forwarded-proto", "http")
        base_url = f"{scheme}://{host}"

        raise HTTPException(
            status_code=401,
            detail="Not authenticated",
            headers={
                "X-Login-URL": f"{base_url}/api/v1/login",
            },
        )
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


def get_gene_name_mapping() -> "GeneNameAndPositionMapping":
    """Get GeneNameAndPositionMapping service instance."""
    return container.get("gene_name_mapping")


def get_gene_disease_data() -> "GeneDiseaseData":
    """Get GeneDiseaseData service instance."""
    return container.get("gene_disease_data")


def get_phenotype_markdown_service() -> "PhenotypeMarkdownService":
    """Get PhenotypeMarkdownService instance."""
    return container.get("phenotype_markdown_service")
