"""
Router for phenotype-related endpoints.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import Response

from app.dependencies import get_phenotype_markdown_service
from app.services.phenotype_markdown_service import PhenotypeMarkdownService
from app.core.exceptions import NotFoundException

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/phenotype/{resource}/{phenocode}/markdown",
    summary="Get markdown documentation for a phenotype",
    responses={
        200: {
            "description": "Markdown content for the phenotype",
            "content": {
                "text/markdown": {
                    "schema": {"type": "string"},
                    "example": "# Type 2 Diabetes\n\nDescription of the phenotype...",
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Markdown file not found for this resource/phenotype"},
        500: {"description": "Internal server error"},
    },
)
async def get_phenotype_markdown(
    resource: str = Path(
        ...,
        description="Data resource (e.g., finngen)",
        example="finngen",
    ),
    phenocode: str = Path(
        ...,
        description="Phenotype code",
        example="T2D_WIDE",
    ),
    markdown_service: PhenotypeMarkdownService = Depends(get_phenotype_markdown_service),
) -> Response:
    """
    Get markdown documentation for a phenotype.

    Returns the markdown content for the specified phenotype from the
    configured GCS location.
    """
    try:
        content = markdown_service.get_markdown(resource, phenocode)
        return Response(
            content=content,
            media_type="text/markdown",
        )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
