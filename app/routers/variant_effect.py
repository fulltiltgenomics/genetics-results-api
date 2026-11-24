import logging
from typing import Literal

from fastapi import APIRouter, Depends, Path, Query, Request, Response

import app.config.common as config_common
import app.config.variant_effect as config
from app.core.range_endpoints import RangeEndpoints
from app.dependencies import get_data_access_variant_effect, get_gene_name_mapping
from app.services.data_access_variant_effect import DataAccessVariantEffect
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping

logger = logging.getLogger(__name__)

router = APIRouter()

_VARIANT_EFFECT_TSV_EXAMPLE = (
    "resource\tchrom\tpos\tref\talt\tvariant\trsid\tdataset\tmodel\tcell_type\ttissue\tlife_stage\tscore\tscore_type\tmlog10p\tpredicted_direction\tquantile_rank\tis_significant\tversion\n"
    "marderstein\t1\t1000500\tA\tG\t1_1000500_A_G\trs123\tmarderstein_chrombpnet\tchrombpnet\texcitatory_neuron\tbrain\tadult\t-1.23\tlog2fc\t4.5\tdecrease\t0.98\ttrue\t2026\n..."
)


_endpoints = RangeEndpoints(
    config.variant_effect_data, config.variant_effect_header_schema, logger, per_dataset=True
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
    return await _endpoints.by_variant(request, variant, resources, format, data_access)


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
    return await _endpoints.by_region(
        request, chrom, start, end, resources, format, data_access
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
    return await _endpoints.by_gene(
        request,
        gene,
        window,
        resources,
        format,
        data_access,
        gene_name_and_position_mapping,
    )
