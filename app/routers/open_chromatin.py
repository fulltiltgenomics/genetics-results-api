import time
import logging
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from app.dependencies import get_data_access_open_chromatin
from app.core.responses import range_response
from app.core.exceptions import NotFoundException
from app.core.variant import var_re
from app.services.data_access_open_chromatin import (
    DataAccessOpenChromatin,
    DataAccessObjectOpenChromatin,
)
import app.config.open_chromatin as config
import app.config.common as config_common

logger = logging.getLogger(__name__)

router = APIRouter()

_OPEN_CHROMATIN_TSV_EXAMPLE = (
    "resource\tchrom\tstart\tend\tpeak_id\tdataset\tcell_type\ttissue\tlife_stage\tcondition\tassay\tscore\tscore_type\tn_cells\tcell_ontology_id\tuberon_id\ttarget_gene\ttarget_gene_id\tversion\n"
    "catlas\t1\t1000000\t1000500\t1-1000000-1000500\tcatlas_open_chromatin\tastrocyte\tbrain\tadult\tunknown\tsnATAC\t0.87\tsignal\t1234\tCL:0000127\tUBERON:0000955\tNA\tNA\t2021\n..."
)


def _resolve_resources(resources: list[str] | None) -> list[str]:
    """Default to all configured resources and reject unknown ones."""
    available = [c["resource"] for c in config.open_chromatin_data]
    if resources is None:
        return available
    invalid = [r for r in resources if r not in available]
    if invalid:
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource(s): {', '.join(invalid)}. Available resources: "
            + ", ".join(available),
        )
    return resources


async def _range_stream_response(
    request: Request,
    stream_coro,
    format: Literal["tsv", "json"],
    start_time: float,
) -> Response:
    """Await a data-access stream coroutine and wrap it in a range response."""
    try:
        stream = await stream_coro
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")

    return await range_response(
        str(request.url),
        stream,
        config.open_chromatin_header_schema,
        format,
        start_time,
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
    start_time = time.time()

    if end < start:
        raise HTTPException(status_code=422, detail="end must be >= start")

    resources = _resolve_resources(resources)

    return await _range_stream_response(
        request,
        data_access.stream_by_region(
            chrom,
            start,
            end,
            resources,
            config_common.read_chunk_size,
            config_common.response_chunk_size,
        ),
        format,
        start_time,
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
    parts = var_re.split(variant)
    if len(parts) < 2:
        raise HTTPException(
            status_code=422,
            detail="variant must be chr:pos:ref:alt or chr:pos (separators - _ : |)",
        )
    chrom = parts[0]
    try:
        pos = int(parts[1])
    except ValueError:
        raise HTTPException(status_code=422, detail="position must be an integer")

    resources = _resolve_resources(resources)

    return await _range_stream_response(
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
        description="Peak ID in format numchrom-start-end (chrX=23,Y=24,M=25)",
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

    resources = _resolve_resources(resources)

    return await _range_stream_response(
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
