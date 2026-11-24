import logging
from typing import Literal

from fastapi import APIRouter, Depends, Path, Query, Request, Response

import app.config.common as config_common
import app.config.mpra as config
from app.core.range_endpoints import RangeEndpoints
from app.dependencies import get_data_access_mpra, get_gene_name_mapping
from app.services.data_access_mpra import DataAccessMpra
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping

logger = logging.getLogger(__name__)

router = APIRouter()

# LONG layout: one row per variant x cell_line (cell_line in meta|K562|HEPG2|SKNSH|
# HCT116|A549). log2Skew_se is populated only on the meta row (NA per-cell-line).
_MPRA_TSV_EXAMPLE = (
    "resource\tchrom\tpos\tvariant\tref\talt\tcohort\tcell_line\temVar\tactive\tlog2Skew\tlog2Skew_se\tlog2Skew_mlog10p\tlog2FC\tlog2FC_mlog10p\tmean_RNA_ref\tmean_RNA_alt\n"
    "siraj_mpra\t1\t1000500\t1_1000500_A_G\tA\tG\tfine-mapped\tmeta\ttrue\ttrue\t-0.82\t0.15\t5.1\t1.24\t8.3\t3.10\t2.28\n"
    "siraj_mpra\t1\t1000500\t1_1000500_A_G\tA\tG\tfine-mapped\tK562\ttrue\ttrue\t-0.79\tNA\t4.6\t1.31\t7.9\t3.22\t2.41\n..."
)


_endpoints = RangeEndpoints(
    config.mpra_data, config.mpra_header_schema, logger, per_dataset=True
)


@router.get(
    "/mpra/variant/{variant}",
    summary="Get MPRA functional annotations at a variant position",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": _MPRA_TSV_EXAMPLE,
                },
            },
        },
        404: {"description": "Resources not found"},
        422: {"description": "Invalid variant or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def mpra_by_variant(
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
    data_access: DataAccessMpra = Depends(get_data_access_mpra),
) -> Response:
    """Get MPRA functional annotations at a variant's position.

    A point read at pos; when ref/alt are supplied the record stream is filtered to
    the matching allele pair, otherwise every cell_line row at the position is
    returned.
    """
    return await _endpoints.by_variant(request, variant, resources, format, data_access)


@router.get(
    "/mpra/region/{chrom}/{start}/{end}",
    summary="Get MPRA functional annotations for tested variants in a genomic region",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": _MPRA_TSV_EXAMPLE,
                },
            },
        },
        404: {"description": "Resources not found"},
        422: {"description": "Invalid region or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def mpra_by_region(
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
    data_access: DataAccessMpra = Depends(get_data_access_mpra),
) -> Response:
    """Get MPRA functional annotations for every tested variant in the region [start, end]."""
    return await _endpoints.by_region(
        request, chrom, start, end, resources, format, data_access
    )


@router.get(
    "/mpra/gene/{gene}",
    summary="Get MPRA functional annotations for tested variants in/around a gene",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": _MPRA_TSV_EXAMPLE,
                },
            },
        },
        404: {"description": "Gene or resources not found"},
        422: {"description": "Invalid window or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def mpra_by_gene(
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
    data_access: DataAccessMpra = Depends(get_data_access_mpra),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(
        get_gene_name_mapping
    ),
) -> Response:
    """Get MPRA functional annotations for tested variants within a gene (± window)."""
    return await _endpoints.by_gene(
        request,
        gene,
        window,
        resources,
        format,
        data_access,
        gene_name_and_position_mapping,
    )
