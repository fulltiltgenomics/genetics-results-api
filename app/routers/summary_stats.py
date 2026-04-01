import time
import logging
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel

from app.config.summary_stats import get_available_resources_and_types
from app.core.exceptions import NotFoundException, ParseException
from app.core.responses import range_response
from app.core.variant import Variant
from app.dependencies import get_sumstats_data_access
from app.services.sumstats_data_access import SumstatsDataAccess
import app.config.common as config_common

logger = logging.getLogger(__name__)

router = APIRouter()


def _build_sumstats_header_schema(
    resource: str, data_type: str
) -> dict[str, type]:
    """Build a header schema for JSON conversion from the column mapping config."""
    from app.config.summary_stats import get_data_files_by_resource_and_type

    data_file_configs = get_data_files_by_resource_and_type(resource, data_type)
    if not data_file_configs:
        return {}

    # use first config's mapping to build schema
    mapping = data_file_configs[0]["column_mapping"]

    # infer types from output column names
    int_columns = {"chr", "pos"}
    float_columns = {
        "pval", "mlog10p", "beta", "se", "af", "af_cases", "af_controls",
        "het_p", "fg_beta", "fg_se", "fg_pval", "fg_af",
        "mvp_eur_beta", "mvp_eur_se", "mvp_eur_pval", "mvp_eur_af",
        "mvp_afr_beta", "mvp_afr_se", "mvp_afr_pval", "mvp_afr_af",
        "mvp_amr_beta", "mvp_amr_se", "mvp_amr_pval", "mvp_amr_af",
        "ukbb_beta", "ukbb_se", "ukbb_pval", "ukbb_af",
        "leave_fg_beta", "leave_fg_se", "leave_fg_pval", "leave_fg_mlog10p",
        "leave_mvp_eur_beta", "leave_mvp_eur_se", "leave_mvp_eur_pval", "leave_mvp_eur_mlog10p",
        "leave_mvp_afr_beta", "leave_mvp_afr_se", "leave_mvp_afr_pval", "leave_mvp_afr_mlog10p",
        "leave_mvp_amr_beta", "leave_mvp_amr_se", "leave_mvp_amr_pval", "leave_mvp_amr_mlog10p",
        "leave_ukbb_beta", "leave_ukbb_se", "leave_ukbb_pval", "leave_ukbb_mlog10p",
    }

    schema = {"resource": str, "version": str, "phenotype": str}
    for out_col in mapping.values():
        if out_col in int_columns:
            schema[out_col] = int
        elif out_col in float_columns:
            schema[out_col] = float
        else:
            schema[out_col] = str
    return schema


@router.get(
    "/summary_stats/{resource}/{data_type}",
    summary="Get summary statistics for variant(s) and phenotype(s)",
    responses={
        200: {"description": "Successful response"},
        401: {"description": "Not authenticated"},
        404: {"description": "Resource, data type, or phenotype not found"},
        422: {"description": "Invalid variant or parameters"},
    },
)
async def get_summary_stats(
    request: Request,
    resource: str,
    data_type: str,
    variants: str = Query(
        ..., description="Comma-separated variant(s), e.g. 1-1000-A-T,2-2000-C-G"
    ),
    phenotypes: str = Query(
        ..., description="Comma-separated phenotype codes, e.g. T2D,BMI"
    ),
    format: Literal["tsv", "json"] = Query(default="tsv", description="Response format"),
    sumstats_access: SumstatsDataAccess = Depends(get_sumstats_data_access),
) -> Response:
    start_time = time.time()

    available = get_available_resources_and_types()
    if (resource, data_type) not in available:
        raise HTTPException(
            status_code=404,
            detail=f"No summary stats for resource '{resource}', data type '{data_type}'. "
            f"Available: {[f'{r}/{dt}' for r, dt in available]}",
        )

    try:
        variant_list = [Variant(v.strip()) for v in variants.split(",") if v.strip()]
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))

    if not variant_list:
        raise HTTPException(status_code=422, detail="At least one variant is required")

    phenotype_list = [p.strip() for p in phenotypes.split(",") if p.strip()]
    if not phenotype_list:
        raise HTTPException(status_code=422, detail="At least one phenotype is required")

    try:
        stream = await sumstats_access.stream_sumstats(
            resource,
            data_type,
            phenotype_list,
            variant_list,
            config_common.read_chunk_size,
            config_common.response_chunk_size,
        )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))

    header_schema = _build_sumstats_header_schema(resource, data_type)
    return await range_response(str(request.url), stream, header_schema, format, start_time)


class SummaryStatsRequest(BaseModel):
    variants: list[str]
    phenotypes: list[str]


@router.post(
    "/summary_stats/{resource}/{data_type}",
    summary="Get summary statistics for multiple variants and phenotypes",
    responses={
        200: {"description": "Successful response"},
        401: {"description": "Not authenticated"},
        404: {"description": "Resource, data type, or phenotype not found"},
        422: {"description": "Invalid variant or parameters"},
    },
)
async def post_summary_stats(
    request: Request,
    resource: str,
    data_type: str,
    body: SummaryStatsRequest,
    format: Literal["tsv", "json"] = Query(default="tsv", description="Response format"),
    sumstats_access: SumstatsDataAccess = Depends(get_sumstats_data_access),
) -> Response:
    start_time = time.time()

    available = get_available_resources_and_types()
    if (resource, data_type) not in available:
        raise HTTPException(
            status_code=404,
            detail=f"No summary stats for resource '{resource}', data type '{data_type}'. "
            f"Available: {[f'{r}/{dt}' for r, dt in available]}",
        )

    try:
        variant_list = [Variant(v.strip()) for v in body.variants if v.strip()]
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))

    if not variant_list:
        raise HTTPException(status_code=422, detail="At least one variant is required")

    phenotype_list = [p.strip() for p in body.phenotypes if p.strip()]
    if not phenotype_list:
        raise HTTPException(status_code=422, detail="At least one phenotype is required")

    try:
        stream = await sumstats_access.stream_sumstats(
            resource,
            data_type,
            phenotype_list,
            variant_list,
            config_common.read_chunk_size,
            config_common.response_chunk_size,
        )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))

    header_schema = _build_sumstats_header_schema(resource, data_type)
    return await range_response(str(request.url), stream, header_schema, format, start_time)
