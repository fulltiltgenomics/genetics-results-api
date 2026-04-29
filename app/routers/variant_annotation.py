import logging
import time
from typing import AsyncGenerator, Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from pydantic import BaseModel

from app.core.exceptions import GeneNotFoundException, NotFoundException, ParseException
from app.core.responses import range_response
from app.core.variant import Variant
from app.dependencies import get_gene_name_mapping, get_variant_annotation_service
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
from app.services.variant_annotation_service import VariantAnnotationService
import app.config.common as config_common

logger = logging.getLogger(__name__)

router = APIRouter()


def _build_header_schema(header: list[bytes]) -> dict[str, type]:
    """Build header schema for JSON conversion. All columns as str for simplicity."""
    return {h.decode(): str for h in header}


async def _prepend_header_stream(
    header: list[bytes],
    data_stream: AsyncGenerator[bytes, None],
) -> AsyncGenerator[bytes, None]:
    """Prepend a TSV header line to a raw byte stream from tabix."""
    yield b"\t".join(header) + b"\n"
    async for chunk in data_stream:
        yield chunk


def _resolve_gene_coordinates(
    gene: str,
    gene_mapping: GeneNameAndPositionMapping,
) -> tuple[int, int, int]:
    """Resolve gene name to chr, start, end using the first gencode version's first result."""
    coords = gene_mapping.get_coordinates_by_gene_name(gene)
    for version_coords in coords.values():
        if version_coords:
            c = version_coords[0]
            return c["chrom"], c["gene_start"], c["gene_end"]
    raise GeneNotFoundException(f"No coordinates found for gene {gene}")


def _parse_region(region: str) -> tuple[int, int, int]:
    """Parse a region string like '1:13668-14506' into (chr, start, end)."""
    try:
        chr_part, range_part = region.split(":")
        start_str, end_str = range_part.split("-")
        chr_val = int(chr_part.replace("chr", "").replace("Chr", "").replace("CHR", ""))
        return chr_val, int(start_str), int(end_str)
    except (ValueError, AttributeError):
        raise ParseException(
            f"Invalid region format '{region}'. Expected format: chr:start-end"
        )


def _validate_source(source: str, service: VariantAnnotationService) -> None:
    available = service.get_available_sources()
    if source not in available:
        raise NotFoundException(
            f"Unknown annotation source '{source}'. Available: {', '.join(available)}"
        )


@router.get(
    "/variant_annotation/{source}",
    summary="Get variant annotations by variant, region, or gene",
    responses={
        200: {"description": "Successful response"},
        401: {"description": "Not authenticated"},
        404: {"description": "Source or gene not found"},
        422: {"description": "Invalid query parameters"},
    },
)
async def get_variant_annotation(
    request: Request,
    source: str = Path(..., description="Annotation source name", example="finngen"),
    variant: str | None = Query(
        default=None, description="Single variant (e.g. 1:13668:G:A)"
    ),
    region: str | None = Query(
        default=None, description="Genomic region (e.g. 1:13668-14506)"
    ),
    gene: str | None = Query(default=None, description="Gene name (e.g. BRCA2)"),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    service: VariantAnnotationService = Depends(get_variant_annotation_service),
    gene_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    start_time = time.time()

    # exactly one of variant/region/gene required
    provided = sum(x is not None for x in [variant, region, gene])
    if provided != 1:
        raise HTTPException(
            status_code=422,
            detail="Exactly one of 'variant', 'region', or 'gene' must be provided",
        )

    try:
        _validate_source(source, service)
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))

    try:
        if variant is not None:
            var = Variant(variant)
            data_stream = await service.stream_by_variants(source, [var])
        elif region is not None:
            chr_val, start, end = _parse_region(region)
            data_stream = await service.stream_by_range(source, chr_val, start, end)
        else:
            chr_val, start, end = _resolve_gene_coordinates(gene, gene_mapping)
            data_stream = await service.stream_by_range(source, chr_val, start, end)
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))
    except GeneNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))

    header = service.get_header(source)
    header_schema = _build_header_schema(header)
    stream = _prepend_header_stream(header, data_stream)

    return await range_response(str(request.url), stream, header_schema, format, start_time)


class VariantAnnotationRequest(BaseModel):
    variants: list[str]


@router.post(
    "/variant_annotation/{source}",
    summary="Get variant annotations for multiple variants",
    responses={
        200: {"description": "Successful response"},
        401: {"description": "Not authenticated"},
        404: {"description": "Source not found"},
        422: {"description": "Invalid variants"},
    },
)
async def post_variant_annotation(
    request: Request,
    source: str = Path(..., description="Annotation source name", example="finngen"),
    body: VariantAnnotationRequest = ...,
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    service: VariantAnnotationService = Depends(get_variant_annotation_service),
) -> Response:
    start_time = time.time()

    try:
        _validate_source(source, service)
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))

    if not body.variants:
        raise HTTPException(status_code=422, detail="No variants provided")

    if len(body.variants) > config_common.max_query_variants:
        raise HTTPException(
            status_code=422,
            detail=f"Too many variants ({len(body.variants)}). Maximum is {config_common.max_query_variants}",
        )

    variants = []
    for vs in body.variants:
        try:
            variants.append(Variant(vs.strip()))
        except ParseException as e:
            raise HTTPException(status_code=422, detail=f"Invalid variant '{vs}': {e}")

    try:
        data_stream = await service.stream_by_variants(source, variants)
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))

    header = service.get_header(source)
    header_schema = _build_header_schema(header)
    stream = _prepend_header_stream(header, data_stream)

    return await range_response(str(request.url), stream, header_schema, format, start_time)
