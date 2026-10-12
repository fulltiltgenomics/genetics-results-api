import logging
import time
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from pydantic import BaseModel

import app.config.common as config_common
from app.config.summary_stats import get_available_resources_and_types
from app.core.exceptions import NotFoundException, ParseException
from app.core.responses import range_response
from app.core.variant import Variant
from app.dependencies import get_request_util, get_sumstats_data_access
from app.services.gcloud_tabix_base import validate_path_component
from app.services.request_util import RequestUtil
from app.services.sumstats_data_access import SumstatsDataAccess

logger = logging.getLogger(__name__)

router = APIRouter()


# typed by suffix so a new cohort or ancestry block in a meta-analysis is numeric without
# being listed; anything unmatched stays a string, which never fails to parse
_INT_COLUMNS = {"chr", "pos", "meta_n"}
_INT_SUFFIXES = ("_n",)
_FLOAT_COLUMNS = {
    "pval", "mlog10p", "beta", "se", "af", "af_cases", "af_controls", "info", "het_p",
}
_FLOAT_SUFFIXES = ("_beta", "_se", "_sebeta", "_pval", "_af", "_af_alt", "_mlog10p", "_het_p", "_r2")


def _infer_sumstats_type(out_col: str) -> type:
    if out_col in _INT_COLUMNS or out_col.endswith(_INT_SUFFIXES):
        return int
    if out_col in _FLOAT_COLUMNS or out_col.endswith(_FLOAT_SUFFIXES):
        return float
    return str


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

    schema = {"resource": str, "version": str, "phenotype": str}
    for out_col in mapping.values():
        schema[out_col] = _infer_sumstats_type(out_col)
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
        for p in phenotype_list:
            validate_path_component(p)
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))

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


@router.get(
    "/summary_stats_by_region/{resource}/{data_type}/{region}",
    summary="Get summary statistics for a genomic region and phenotype(s)",
    responses={
        200: {"description": "Successful response"},
        401: {"description": "Not authenticated"},
        404: {"description": "Resource, data type, or phenotype not found"},
        422: {"description": "Invalid region or parameters"},
    },
)
async def get_summary_stats_by_region(
    request: Request,
    resource: str,
    data_type: str,
    region: str = Path(
        ..., description="Chromosome region", examples=["1:1000000-1000100"]
    ),
    phenotypes: str = Query(
        ..., description="Comma-separated phenotype codes, e.g. T2D,BMI"
    ),
    format: Literal["tsv", "json"] = Query(default="tsv", description="Response format"),
    request_util: RequestUtil = Depends(get_request_util),
    sumstats_access: SumstatsDataAccess = Depends(get_sumstats_data_access),
) -> Response:
    """
    Get every summary stat record in a genomic region for the given phenotype(s).

    Unlike credible_sets_by_region, phenotypes are required: summary stats live in
    per-phenotype files, so there is no single file spanning a region across all traits.
    Region size is capped by config.common.max_range_size_stream/max_range_size_json.
    """
    start_time = time.time()

    available = get_available_resources_and_types()
    if (resource, data_type) not in available:
        raise HTTPException(
            status_code=404,
            detail=f"No summary stats for resource '{resource}', data type '{data_type}'. "
            f"Available: {[f'{r}/{dt}' for r, dt in available]}",
        )

    try:
        (chr, start, end) = request_util.validate_range(region, format)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    phenotype_list = [p.strip() for p in phenotypes.split(",") if p.strip()]
    if not phenotype_list:
        raise HTTPException(status_code=422, detail="At least one phenotype is required")

    try:
        for p in phenotype_list:
            validate_path_component(p)
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))

    try:
        stream = await sumstats_access.stream_sumstats_range(
            resource,
            data_type,
            phenotype_list,
            chr,
            start,
            end,
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
        for p in phenotype_list:
            validate_path_component(p)
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))

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
