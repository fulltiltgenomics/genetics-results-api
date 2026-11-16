import logging
import time
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response

import app.config.common as config_common
import app.config.open_chromatin as config
from app.core.range_endpoints import RangeEndpoints, parse_chrom_pos
from app.dependencies import get_data_access_open_chromatin
from app.services.data_access_open_chromatin import (
    DataAccessObjectOpenChromatin,
    DataAccessOpenChromatin,
)

logger = logging.getLogger(__name__)

router = APIRouter()

_OPEN_CHROMATIN_TSV_EXAMPLE = (
    "resource\tchrom\tstart\tend\tpeak_id\tdataset\tcell_type\ttissue\tlife_stage\tcondition\tassay\tscore\tscore_type\tn_cells\tcell_ontology_id\tuberon_id\ttarget_gene\ttarget_gene_id\tversion\n"
    "catlas\t1\t1000000\t1000500\t1-1000000-1000500\tcatlas_open_chromatin\tastrocyte\tbrain\tadult\tunknown\tsnATAC\t0.87\tsignal\t1234\tCL:0000127\tUBERON:0000955\tNA\tNA\t2021\n..."
)


_endpoints = RangeEndpoints(
    config.open_chromatin_data, config.open_chromatin_header_schema, logger
)


@router.get(
    "/open_chromatin/region/{chrom}/{start}/{end}",
    summary="Get open-chromatin peaks overlapping a genomic region",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": _OPEN_CHROMATIN_TSV_EXAMPLE,
                },
            },
        },
        404: {"description": "Resources not found"},
        422: {"description": "Invalid region or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def open_chromatin_by_region(
    request: Request,
    chrom: str = Path(..., description="Chromosome", examples=["chr1", "1", "X"]),
    start: int = Path(..., description="Region start (1-based, inclusive)", ge=1),
    end: int = Path(..., description="Region end (1-based, inclusive)", ge=1),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available resources are used)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access: DataAccessOpenChromatin = Depends(get_data_access_open_chromatin),
) -> Response:
    """Get all open-chromatin peaks overlapping the region [start, end]."""
    return await _endpoints.by_region(
        request, chrom, start, end, resources, format, data_access
    )


@router.get(
    "/open_chromatin/variant/{variant}",
    summary="Get open-chromatin peaks overlapping a variant position",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": _OPEN_CHROMATIN_TSV_EXAMPLE,
                },
            },
        },
        404: {"description": "Resources not found"},
        422: {"description": "Invalid variant or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def open_chromatin_by_variant(
    request: Request,
    variant: str = Path(
        ...,
        description="Variant as chr:pos:ref:alt or chr:pos (separators - _ : | ); only chr and pos are used",
        examples=["1:1000500:A:G", "1:1000500"],
    ),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available resources are used)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access: DataAccessOpenChromatin = Depends(get_data_access_open_chromatin),
) -> Response:
    """Get all open-chromatin peaks overlapping the variant's position."""
    start_time = time.time()

    # only chromosome and position define the overlap; accept both chr:pos:ref:alt
    # and bare chr:pos so callers need not supply alleles
    chrom, pos, _ = parse_chrom_pos(variant)

    resources = _endpoints.resolve_resources(resources)

    return await _endpoints.respond(
        request,
        data_access.stream_by_variant(
            chrom,
            pos,
            resources,
            config_common.read_chunk_size,
            config_common.response_chunk_size,
        ),
        format,
        start_time,
    )


@router.get(
    "/open_chromatin/peak/{peak_id}",
    summary="Get open-chromatin peaks overlapping a peak's region",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": _OPEN_CHROMATIN_TSV_EXAMPLE,
                },
            },
        },
        404: {"description": "Resources not found"},
        422: {"description": "Invalid peak_id or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def open_chromatin_by_peak(
    request: Request,
    peak_id: str = Path(
        ...,
        description="Peak ID in format numchrom-start-end (chrX=23,Y=24,M=26)",
        examples=["1-1000000-1000500", "23-1000000-1000500"],
    ),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available resources are used)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access: DataAccessOpenChromatin = Depends(get_data_access_open_chromatin),
) -> Response:
    """Get all open-chromatin peaks overlapping the region defined by a peak_id."""
    start_time = time.time()

    try:
        DataAccessObjectOpenChromatin.parse_peak_id(peak_id)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    resources = _endpoints.resolve_resources(resources)

    return await _endpoints.respond(
        request,
        data_access.stream_by_peak_id(
            peak_id,
            resources,
            config_common.read_chunk_size,
            config_common.response_chunk_size,
        ),
        format,
        start_time,
    )
