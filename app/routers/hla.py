import logging
import time
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from app.config.hla import (
    HLA_CHROM,
    HLA_DATA_TYPE,
    HLA_GENE_POSITIONS,
    HLA_REGION_END,
    HLA_REGION_START,
    gene_positions,
)
from app.config.summary_stats import get_available_resources_and_types
from app.core.exceptions import NotFoundException, ParseException
from app.core.responses import range_response
from app.dependencies import get_sumstats_data_access
from app.services.gcloud_tabix_base import validate_path_component
from app.services.sumstats_data_access import SumstatsDataAccess
import app.config.common as config_common

logger = logging.getLogger(__name__)

router = APIRouter()

# output column -> python type for the JSON conversion. `pval` underflows to a literal 0
# for the strongest HLA signals (coeliac DQB1*02:01, B*27:05 in spondyloarthritis), so
# `mlog10p` is the field to rank on; both are kept as floats.
_HLA_HEADER_SCHEMA: dict[str, type] = {
    "resource": str,
    "version": str,
    "phenotype": str,
    "chr": int,
    "pos": int,
    "gene": str,
    "allele": str,
    "pval": float,
    "mlog10p": float,
    "beta": float,
    "se": float,
    "af": float,
    "af_cases": float,
    "af_controls": float,
    "info": float,
}


def _hla_resources() -> list[str]:
    """Resources that ship HLA results, from the summary_stats registry."""
    return sorted(
        {r for r, dt in get_available_resources_and_types() if dt == HLA_DATA_TYPE}
    )


@router.get(
    "/hla/genes",
    summary="List the HLA genes carried by the HLA association results",
    responses={
        200: {"description": "Successful response"},
        401: {"description": "Not authenticated"},
    },
)
async def get_hla_genes() -> dict:
    """
    The HLA locus registry: which genes are typed and where their alleles are anchored.

    Every allele of a gene is written at that gene's single anchor position, so these
    are the exact coordinates the per-phenotype tabix indexes are built on rather than
    gene-annotation coordinates. DRB3/DRB4/DRB5 deliberately share one placeholder
    anchor (see app/config/hla.py) and cannot be told apart positionally.
    """
    return {
        "chrom": HLA_CHROM,
        "region": f"{HLA_CHROM}:{HLA_REGION_START}-{HLA_REGION_END}",
        "resources": _hla_resources(),
        "genes": gene_positions(),
    }


@router.get(
    "/hla/{resource}",
    summary="Get classical HLA allele associations for phenotype(s)",
    responses={
        200: {"description": "Successful response"},
        401: {"description": "Not authenticated"},
        404: {"description": "Resource or phenotype not found"},
        422: {"description": "Invalid parameters"},
    },
)
async def get_hla_associations(
    request: Request,
    resource: str,
    phenotypes: str = Query(
        ..., description="Comma-separated phenotype codes, e.g. K11_COELIAC,T1D"
    ),
    genes: str | None = Query(
        default=None,
        description="Optional comma-separated HLA gene filter, e.g. HLA-B,HLA-DQB1. "
        "Omit for every typed gene.",
    ),
    format: Literal["tsv", "json"] = Query(default="tsv", description="Response format"),
    sumstats_access: SumstatsDataAccess = Depends(get_sumstats_data_access),
) -> Response:
    """
    Every imputed classical HLA allele tested against the given phenotype(s).

    One read returns the trait's whole HLA profile (~187 alleles across 10 genes), so
    there is no by-variant form: an allele has no chrom_pos_ref_alt identity to look up.
    Rows carry the imputation `info` for the allele, which is what separates a real
    association from a rare, badly-imputed allele with a huge unstable beta.

    The reverse question — which phenotypes an allele is associated with — spans every
    per-phenotype file and is answered from the BigQuery `hla_associations_v` view, not
    from here.
    """
    start_time = time.time()

    available = get_available_resources_and_types()
    if (resource, HLA_DATA_TYPE) not in available:
        raise HTTPException(
            status_code=404,
            detail=f"No HLA results for resource '{resource}'. "
            f"Available: {_hla_resources()}",
        )

    phenotype_list = [p.strip() for p in phenotypes.split(",") if p.strip()]
    if not phenotype_list:
        raise HTTPException(status_code=422, detail="At least one phenotype is required")

    try:
        for p in phenotype_list:
            validate_path_component(p)
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))

    positions: list[int] | None = None
    if genes is not None:
        gene_list = [g.strip() for g in genes.split(",") if g.strip()]
        if not gene_list:
            raise HTTPException(
                status_code=422, detail="genes was given but contained no gene names"
            )
        unknown = [g for g in gene_list if g not in HLA_GENE_POSITIONS]
        if unknown:
            raise HTTPException(
                status_code=422,
                detail=f"Unknown HLA gene(s) {unknown}. "
                f"Available: {sorted(HLA_GENE_POSITIONS)}",
            )
        # read the selected anchors as discrete points, not as the span between them,
        # so asking for HLA-A and HLA-DPB1 does not also return the eight genes lying
        # between the two. DRB3/4/5 share one anchor and still come back together.
        positions = sorted({HLA_GENE_POSITIONS[g] for g in gene_list})

    try:
        if positions is not None:
            stream = await sumstats_access.stream_sumstats_positions(
                resource,
                HLA_DATA_TYPE,
                phenotype_list,
                HLA_CHROM,
                positions,
                config_common.read_chunk_size,
                config_common.response_chunk_size,
            )
        else:
            stream = await sumstats_access.stream_sumstats_range(
                resource,
                HLA_DATA_TYPE,
                phenotype_list,
                HLA_CHROM,
                HLA_REGION_START,
                HLA_REGION_END,
                config_common.read_chunk_size,
                config_common.response_chunk_size,
            )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))

    return await range_response(
        str(request.url), stream, _HLA_HEADER_SCHEMA, format, start_time
    )
