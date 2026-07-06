import time
import logging
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from app.dependencies import get_data_access_variant_effect, get_gene_name_mapping
from app.core.responses import range_response
from app.core.exceptions import NotFoundException
from app.core.variant import var_re
from app.services.data_access_variant_effect import DataAccessVariantEffect
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
import app.config.variant_effect as config
import app.config.common as config_common

logger = logging.getLogger(__name__)

router = APIRouter()

_VARIANT_EFFECT_TSV_EXAMPLE = (
    "resource\tchrom\tpos\tref\talt\tvariant\trsid\tdataset\tmodel\tcell_type\ttissue\tlife_stage\tscore\tscore_type\tmlog10p\tpredicted_direction\tquantile_rank\tis_significant\tversion\n"
    "marderstein\tchr1\t1000500\tA\tG\tchr1_1000500_A_G\trs123\tmarderstein_chrombpnet\tchrombpnet\texcitatory_neuron\tbrain\tadult\t-1.23\tlog2fc\t4.5\tdecrease\t0.98\ttrue\t2026\n..."
)


def _resolve_resources(resources: list[str] | None) -> list[str]:
    """Default to all configured resources and reject unknown ones.

    Resources are drawn from the per-dataset config entries (both Marderstein
    predictor datasets share the resource "marderstein"), so the set is deduped.
    """
    available = sorted({c["resource"] for c in config.variant_effect_data})
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
        # includes GeneNotFoundException (by-gene resolution) -> 404
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")

    return await range_response(
        str(request.url),
        stream,
        config.variant_effect_header_schema,
        format,
        start_time,
    )


@router.get(
    "/variant_effect/variant/{variant}",
    summary="Get predicted variant effects at a variant position",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": _VARIANT_EFFECT_TSV_EXAMPLE,
                },
            },
        },
        404: {"description": "Resources not found"},
        422: {"description": "Invalid variant or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def variant_effect_by_variant(
    request: Request,
    variant: str = Path(
        ...,
        description="Variant as chr:pos:ref:alt or chr:pos (separators - _ : | ); when ref/alt are given, rows are filtered to that allele pair",
        examples=["1:1000500:A:G", "1:1000500"],
    ),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available resources are used)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access: DataAccessVariantEffect = Depends(get_data_access_variant_effect),
) -> Response:
    """Get predicted variant effects at a variant's position.

    A point read at pos; when ref/alt are supplied the record stream is filtered to
    the matching allele pair, otherwise every model/context row at the position is
    returned.
    """
    start_time = time.time()

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

    ref = parts[2] if len(parts) >= 4 else None
    alt = parts[3] if len(parts) >= 4 else None

    resources = _resolve_resources(resources)

    return await _range_stream_response(
        request,
        data_access.stream_by_variant(
            chrom,
            pos,
            resources,
            config_common.read_chunk_size,
            config_common.response_chunk_size,
            ref,
            alt,
        ),
        format,
        start_time,
    )


@router.get(
    "/variant_effect/region/{chrom}/{start}/{end}",
    summary="Get predicted variant effects for scored variants in a genomic region",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": _VARIANT_EFFECT_TSV_EXAMPLE,
                },
            },
        },
        404: {"description": "Resources not found"},
        422: {"description": "Invalid region or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def variant_effect_by_region(
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
    data_access: DataAccessVariantEffect = Depends(get_data_access_variant_effect),
) -> Response:
    """Get predicted variant effects for every scored variant in the region [start, end]."""
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
    "/variant_effect/gene/{gene}",
    summary="Get predicted variant effects for scored variants in/around a gene",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": _VARIANT_EFFECT_TSV_EXAMPLE,
                },
            },
        },
        404: {"description": "Gene or resources not found"},
        422: {"description": "Invalid window or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def variant_effect_by_gene(
    request: Request,
    gene: str = Path(..., description="Gene name or ENSG ID", examples=["PCSK9"]),
    window: int = Query(
        default=0,
        description="One-sided window in base pairs around the gene (default 0, which means the gene itself)",
        ge=0,
        le=int(config_common.max_gene_window),
    ),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available resources are used)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access: DataAccessVariantEffect = Depends(get_data_access_variant_effect),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(
        get_gene_name_mapping
    ),
) -> Response:
    """Get predicted variant effects for scored variants within a gene (± window)."""
    start_time = time.time()

    resources = _resolve_resources(resources)

    return await _range_stream_response(
        request,
        data_access.stream_by_gene(
            gene,
            gene_name_and_position_mapping,
            resources,
            window,
            config_common.read_chunk_size,
            config_common.response_chunk_size,
        ),
        format,
        start_time,
    )
