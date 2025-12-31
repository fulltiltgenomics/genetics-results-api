import logging
import time
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from app.dependencies import get_gene_disease_data
from app.services.gene_disease_data import GeneDiseaseData
from app.core.responses import TimedStreamingResponse, TimedJSONResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/gene_disease/{gene_name}",
    summary="Get gene-to-disease relationships",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "resource\tuuid\tgene_symbol\tdisease_curie\tdisease_title\tclassification\tmode_of_inheritance\tsubmitter\ngencc\tGENCC_000104-HGNC_1100-OMIM_604370-HP_0000006-GENCC_100002\tBRCA1\tMONDO:0011450\tbreast-ovarian cancer, familial, susceptibility to, 1\tStrong\tAutosomal dominant\tGenomics England PanelApp\ngencc\tGENCC_000104-HGNC_1100-OMIM_614320-HP_0000006-GENCC_100003\tBRCA1\tMONDO:0013685\tpancreatic cancer, susceptibility to, 4\tModerate\tAutosomal dominant\tGenomics England PanelApp\n...",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "resource": {"type": "string"},
                                "uuid": {"type": "string"},
                                "gene_symbol": {"type": "string"},
                                "disease_curie": {"type": "string"},
                                "disease_title": {"type": "string"},
                                "classification": {"type": "string"},
                                "mode_of_inheritance": {"type": "string"},
                                "submitter": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "resource": "gencc",
                            "uuid": "GENCC_000104-HGNC_1100-OMIM_604370-HP_0000006-GENCC_100002",
                            "gene_symbol": "BRCA1",
                            "disease_curie": "MONDO:0011450",
                            "disease_title": "breast-ovarian cancer, familial, susceptibility to, 1",
                            "classification": "Strong",
                            "mode_of_inheritance": "Autosomal dominant",
                            "submitter": "Genomics England PanelApp",
                        },
                        {
                            "resource": "gencc",
                            "uuid": "GENCC_000104-HGNC_1100-OMIM_614320-HP_0000006-GENCC_100003",
                            "gene_symbol": "BRCA1",
                            "disease_curie": "MONDO:0013685",
                            "disease_title": "pancreatic cancer, susceptibility to, 4",
                            "classification": "Moderate",
                            "mode_of_inheritance": "Autosomal dominant",
                            "submitter": "Genomics England PanelApp",
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Gene not found"},
        500: {"description": "Internal server error"},
    },
)
async def get_gene_disease(
    request: Request,
    gene_name: str = Path(..., description="Gene symbol", example="BRCA1"),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    gene_disease_data: GeneDiseaseData = Depends(get_gene_disease_data),
):
    """
    Get gene-to-disease relationships for a specific gene.

    Returns all disease associations for the given gene symbol from the
    submissions export data.
    """
    start_time = time.time()
    request_url = str(request.url)

    try:
        # get filtered data
        filtered_df = gene_disease_data.get_by_gene_symbol(gene_name)

        if filtered_df.is_empty():
            raise HTTPException(
                status_code=404,
                detail=f"No disease associations found for gene {gene_name}",
            )

        logger.debug(
            f"{request_url} found {len(filtered_df)} records for gene {gene_name}"
        )

        if format == "tsv":
            tsv_string = filtered_df.write_csv(separator="\t")

            async def tsv_generator():
                yield tsv_string.encode("utf-8")

            return TimedStreamingResponse(
                tsv_generator(),
                request_url,
                start_time,
                media_type="text/tab-separated-values",
            )

        elif format == "json":
            records = filtered_df.to_dicts()

            return TimedJSONResponse(
                records,
                request_url,
                start_time,
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"{request_url} error processing gene-disease query: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error processing gene-disease query: {str(e)}"
        )
