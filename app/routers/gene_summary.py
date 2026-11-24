import logging
import time

from fastapi import APIRouter, Depends, HTTPException, Path, Request

from app.core.exceptions import GeneNotFoundException
from app.core.responses import TimedJSONResponse
from app.dependencies import get_gene_name_mapping, get_gene_summary_access
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
from app.services.gene_summary_access import GeneSummaryAccess

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/gene_summary/{gene}",
    summary="Get the AI summary of a gene's evidence",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "application/json": {
                    "example": {
                        "gene": "NOD2",
                        "generated": "2026-11-23",
                        "model": "claude-fable-5-2",
                        "headline": "Coding variants in NOD2 are strongly associated with Crohn's disease.",
                        "overview": "...",
                        "findings": [
                            {
                                "statement": "...",
                                "tab": "gene burden",
                                "evidence": [
                                    {
                                        "id": "B1",
                                        "label": "IBD_exome · Crohn's disease · nonsynonymous · p 1.4e-58",
                                    }
                                ],
                            }
                        ],
                        "gaps": ["..."],
                    }
                }
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Gene not found, or the gene has no summary"},
        500: {"description": "Internal server error"},
    },
)
async def gene_summary(
    request: Request,
    gene: str = Path(..., description="Gene symbol", examples=["NOD2"]),
    access: GeneSummaryAccess = Depends(get_gene_summary_access),
    mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
):
    """
    Get the precomputed AI summary of this API's own evidence for a gene.

    The summary is written offline from a fixed evidence bundle per gene (burden, exome
    variants, GWAS credible sets, QTLs, colocalization, expression, Mendelian curation,
    constraint), with no literature. Each finding names the rows it rests on.
    """
    start_time = time.time()
    request_url = str(request.url)
    try:
        # only a name the gene mappings know goes into the object path, in their spelling.
        # the files are keyed by approved symbol, so an alias finds nothing
        symbol = mapping.known_gene_name(gene)
    except GeneNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    try:
        summary = await access.get(symbol)
    except Exception as e:
        logger.error(f"{request_url} error reading gene summary: {e}")
        raise HTTPException(status_code=500, detail="Error reading gene summary")
    if summary is None:
        raise HTTPException(status_code=404, detail=f"No summary for gene {symbol}")
    return TimedJSONResponse(summary, request_url, start_time)
