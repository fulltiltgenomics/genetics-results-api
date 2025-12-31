import time
import logging
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from app.dependencies import (
    get_request_util,
    get_data_access_expression,
    get_gene_name_mapping,
)
from app.core.responses import range_response
from app.core.exceptions import (
    GeneNotFoundException,
    NotFoundException,
)
from app.services.data_access_expression import DataAccessExpression
from app.services.request_util import RequestUtil
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
import app.config.expression as config
import app.config.common as config_common

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/expression_by_gene/{gene}",
    include_in_schema=False,
    summary="Get expression data for a gene across resources",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "resource\tversion\tdataset\tchrom\tgene_start\tgene_end\tgene_name\tgene_id\ttissue_cell\tlevel\ngtex\tv10\tGTEx_v10\t1\t55039447\t55064852\tPCSK9\tENSG00000169174.11\tcells_ebv-transformed_lymphocytes\t0.012993\ngtex\tv10\tGTEx_v10\t1\t55039447\t55064852\tPCSK9\tENSG00000169174.11\tcervix_endocervix\t0.246645\n...",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "resource": {"type": "string"},
                                "version": {"type": "string"},
                                "dataset": {"type": "string"},
                                "chrom": {"type": "integer"},
                                "gene_start": {"type": "integer"},
                                "gene_end": {"type": "integer"},
                                "gene_name": {"type": "string"},
                                "gene_id": {"type": "string"},
                                "tissue_cell": {"type": "string"},
                                "level": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "resource": "gtex",
                            "version": "v10",
                            "dataset": "GTEx_v10",
                            "chrom": 1,
                            "gene_start": 55039447,
                            "gene_end": 55064852,
                            "gene_name": "PCSK9",
                            "gene_id": "ENSG00000169174.11",
                            "tissue_cell": "cells_ebv-transformed_lymphocytes",
                            "level": "0.012993",
                        },
                        {
                            "resource": "gtex",
                            "version": "v10",
                            "dataset": "GTEx_v10",
                            "chrom": 1,
                            "gene_start": 55039447,
                            "gene_end": 55064852,
                            "gene_name": "PCSK9",
                            "gene_id": "ENSG00000169174.11",
                            "tissue_cell": "cervix_endocervix",
                            "level": "0.246645",
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Gene or resources not found"},
        422: {"description": "Invalid format or interval parameter"},
        500: {"description": "Internal server error"},
    },
)
async def expression_by_gene(
    request: Request,
    gene: str = Path(..., description="Gene name, ENSG ID, or comma-separated list of gene names", example="PCSK9"),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available resources are used)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    request_util: RequestUtil = Depends(get_request_util),
    data_access_expression: DataAccessExpression = Depends(get_data_access_expression),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    """
    Get expression data for a gene or comma-separated list of genes across resources.
    """
    start_time = time.time()
    if resources is None:  # use all resources if none given
        resources = [c["resource"] for c in config.expression_data]
    if not request_util.check_expression_resources(resources):
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource in {resources}. Available resources: "
            + ", ".join([c["resource"] for c in config.expression_data]),
        )

    genes = [g.strip() for g in gene.split(",") if g.strip()]
    if not genes:
        raise HTTPException(status_code=422, detail="No valid gene names provided")

    all_coords: dict[str, list] = {}
    for g in genes:
        try:
            coords = gene_name_and_position_mapping.get_coordinates_by_gene_name(g)
            logger.debug(f"Coordinates for gene {g}: {coords}")
            for version in coords.keys():
                if version not in all_coords:
                    all_coords[version] = []
                all_coords[version].extend(coords[version])
        except GeneNotFoundException as e:
            raise HTTPException(status_code=404, detail=str(e))

    try:
        stream = await data_access_expression.stream_range(
            all_coords,
            resources,
            config_common.read_chunk_size,
            config_common.response_chunk_size,
        )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")
    return await range_response(
        str(request.url), stream, config.expression_header_schema, format, start_time
    )
