import time
import logging
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from pydantic import BaseModel
from app.dependencies import (
    get_request_util,
    get_data_access,
    get_gene_name_mapping,
    get_credible_set_stats_service,
)
from app.core.responses import TimedStreamingResponse, TimedJSONResponse, range_response
from app.core.streams import filter_stream_by_cs_id
from app.core.variant import Variant
from app.core.exceptions import (
    GeneNotFoundException,
    NotFoundException,
    ParseException,
)
from app.services import config_util
from app.services.data_access import DataAccess
from app.services.request_util import RequestUtil
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
import app.config.credible_sets as config_credible_sets
import app.config.common as config_common

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/credible_sets_by_phenotype/{resource}/{phenotype_or_study}",
    summary="Get credible sets for a phenotype or study",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "dataset\tdata_type\ttrait\ttrait_original\tcell_type\tchr\tpos\tref\talt\tmlog10p\tbeta\tse\tpip\tcs_id\tcs_size\tcs_min_r2\taaf\tmost_severe\tgene_most_severe\nFinnGen_R13\tGWAS\tT2D_WIDE\tT2D_WIDE\tNA\t1\t20402396\tT\tA\t8.19\t-3.946e-02\t6.797e-03\t0.06\tchr1:18906883-21906883_1\t12\t0.9919\t0.5241\tintron_variant\tLINC01141\n...",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "dataset": {"type": "string"},
                                "data_type": {"type": "string"},
                                "trait": {"type": "string"},
                                "trait_original": {"type": "string"},
                                "cell_type": {"type": ["string", "null"]},
                                "chr": {"type": "integer"},
                                "pos": {"type": "integer"},
                                "ref": {"type": "string"},
                                "alt": {"type": "string"},
                                "mlog10p": {"type": "number"},
                                "beta": {"type": "number"},
                                "se": {"type": "number"},
                                "pip": {"type": "number"},
                                "cs_id": {"type": "string"},
                                "cs_size": {"type": "integer"},
                                "cs_min_r2": {"type": "number"},
                                "aaf": {"type": "number"},
                                "most_severe": {"type": "string"},
                                "gene_most_severe": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "dataset": "FinnGen_R13",
                            "data_type": "GWAS",
                            "trait": "T2D_WIDE",
                            "trait_original": "T2D_WIDE",
                            "cell_type": None,
                            "chr": 1,
                            "pos": 20402396,
                            "ref": "T",
                            "alt": "A",
                            "mlog10p": 8.19,
                            "beta": -0.03946,
                            "se": 0.006797,
                            "pip": 0.06,
                            "cs_id": "chr1:18906883-21906883_1",
                            "cs_size": 12,
                            "cs_min_r2": 0.9919,
                            "aaf": 0.5241,
                            "most_severe": "intron_variant",
                            "gene_most_severe": "LINC01141",
                        },
                        {"...": "..."},
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Resource or phenotype not found"},
        422: {"description": "Invalid interval or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def credible_sets_by_phenotype(
    request: Request,
    resource: str = Path(..., description="Data resource", example="finngen"),
    phenotype_or_study: str = Path(
        ..., description="Phenotype or study code", example="T2D_WIDE"
    ),
    interval: int = Query(
        default=95, description="Credible set threshold (95 or 99)", ge=95, le=99
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access: DataAccess = Depends(get_data_access),
) -> Response:
    """
    Get credible sets for a phenotype or study.
    """
    start_time = time.time()
    if interval not in (95, 99):
        raise HTTPException(status_code=422, detail="Interval must be 95 or 99")
    if interval == 99:
        raise HTTPException(status_code=422, detail="Interval 99 is not supported yet")
    if format == "tsv":
        try:
            # TODO find a better way to catch not founds than first checking if the file exists
            if not await data_access.check_phenotype_exists(
                resource, phenotype_or_study, interval
            ):
                logger.error(f"File not found: {phenotype_or_study}")
                raise NotFoundException(f"File not found: {phenotype_or_study}")
            stream = await data_access.stream_phenotype(
                resource, phenotype_or_study, interval, config_common.read_chunk_size
            )
            return TimedStreamingResponse(
                stream, request.url, start_time, media_type="text/tab-separated-values"
            )
        except NotFoundException as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(
                f"Error streaming phenotype {phenotype_or_study} from {resource}: {e}"
            )
            raise HTTPException(
                status_code=500, detail="Error streaming phenotype data"
            )
    elif format == "json":
        try:
            rows = await data_access.json_phenotype(
                resource,
                phenotype_or_study,
                interval,
                config_credible_sets.cs_header_schema,
                "cs",
                config_common.read_chunk_size,
            )
            return TimedJSONResponse(rows, request.url, start_time)
        except NotFoundException as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/credible_sets_by_id/{resource}/{phenotype_or_study}/{cs_id:path}",
    summary="Get credible set variants by cs_id",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "dataset\tdata_type\ttrait\ttrait_original\tcell_type\tchr\tpos\tref\talt\tmlog10p\tbeta\tse\tpip\tcs_id\tcs_size\tcs_min_r2\taaf\tmost_severe\tgene_most_severe\nFinnGen_R13\tGWAS\tK11_IBD_STRICT\tK11_IBD_STRICT\tNA\t1\t7972861\tA\tG\t11.13\t-1.052e-01\t1.536e-02\t0.0234\tchr1:6535440-9535440_1\t65\t0.7531\t0.2748\tintron_variant\tPARK7\n...",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "dataset": {"type": "string"},
                                "data_type": {"type": "string"},
                                "trait": {"type": "string"},
                                "trait_original": {"type": "string"},
                                "cell_type": {"type": ["string", "null"]},
                                "chr": {"type": "integer"},
                                "pos": {"type": "integer"},
                                "ref": {"type": "string"},
                                "alt": {"type": "string"},
                                "mlog10p": {"type": "number"},
                                "beta": {"type": "number"},
                                "se": {"type": "number"},
                                "pip": {"type": "number"},
                                "cs_id": {"type": "string"},
                                "cs_size": {"type": "integer"},
                                "cs_min_r2": {"type": "number"},
                                "aaf": {"type": "number"},
                                "most_severe": {"type": "string"},
                                "gene_most_severe": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "dataset": "FinnGen_R13",
                            "data_type": "GWAS",
                            "trait": "K11_IBD_STRICT",
                            "trait_original": "K11_IBD_STRICT",
                            "cell_type": None,
                            "chr": 1,
                            "pos": 7972861,
                            "ref": "A",
                            "alt": "G",
                            "mlog10p": 11.1309,
                            "beta": -0.1052,
                            "se": 0.01536,
                            "pip": 0.0234,
                            "cs_id": "chr1:6535440-9535440_1",
                            "cs_size": 65,
                            "cs_min_r2": 0.7531,
                            "aaf": 0.2748,
                            "most_severe": "intron_variant",
                            "gene_most_severe": "PARK7",
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Resource, phenotype, or cs_id not found"},
        422: {"description": "Invalid interval or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def credible_sets_by_id(
    request: Request,
    resource: str = Path(..., description="Data resource", example="finngen"),
    phenotype_or_study: str = Path(
        ..., description="Phenotype or study code", example="K11_IBD_STRICT"
    ),
    cs_id: str = Path(
        ..., description="Credible set ID", example="chr1:6535440-9535440_1"
    ),
    interval: int = Query(
        default=95, description="Credible set threshold (95 or 99)", ge=95, le=99
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access: DataAccess = Depends(get_data_access),
) -> Response:
    """
    Get all variants in a specific credible set by its cs_id.
    """
    start_time = time.time()
    if interval not in (95, 99):
        raise HTTPException(status_code=422, detail="Interval must be 95 or 99")
    if interval == 99:
        raise HTTPException(status_code=422, detail="Interval 99 is not supported yet")

    if not await data_access.check_phenotype_exists(resource, phenotype_or_study, interval):
        raise HTTPException(status_code=404, detail=f"Phenotype not found: {phenotype_or_study}")

    if format == "tsv":
        try:
            raw_stream = await data_access.stream_phenotype(
                resource, phenotype_or_study, interval, config_common.read_chunk_size
            )
            filtered_stream = filter_stream_by_cs_id(raw_stream, cs_id)
            return TimedStreamingResponse(
                filtered_stream, request.url, start_time, media_type="text/tab-separated-values"
            )
        except NotFoundException as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(f"Error streaming cs_id {cs_id}: {e}")
            raise HTTPException(status_code=500, detail="Error streaming credible set data")

    elif format == "json":
        try:
            rows = await data_access.json_phenotype(
                resource,
                phenotype_or_study,
                interval,
                config_credible_sets.cs_header_schema,
                "cs",
                config_common.read_chunk_size,
            )
            filtered_rows = [r for r in rows if r.get("cs_id") == cs_id]
            if not filtered_rows:
                raise HTTPException(status_code=404, detail=f"cs_id not found: {cs_id}")
            return TimedJSONResponse(filtered_rows, request.url, start_time)
        except NotFoundException as e:
            raise HTTPException(status_code=404, detail=str(e))
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting JSON for cs_id {cs_id}: {e}")
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/credible_sets_by_region/{region}",
    summary="Get credible sets across resources in a region",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "resource\tversion\tdataset\tdata_type\ttrait\ttrait_original\tcell_type\tchr\tpos\tref\talt\tmlog10p\tbeta\tse\tpip\tcs_id\tcs_size\tcs_min_r2\taaf\tmost_severe\tgene_most_severe\neqtl_catalogue\tR7\tQTD000608\teQTL\tISG15\tENSG00000187608|ge\tB_cell|naive\t1\t1000018\tG\tA\t7.3668\t-7.921e-01\t1.434e-01\t0.0914\tENSG00000187608_L1\t25\t0.7468\t0.3012\t5_prime_UTR_variant\tHES4\n...",
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
                                "data_type": {"type": "string"},
                                "trait": {"type": "string"},
                                "trait_original": {"type": "string"},
                                "cell_type": {"type": ["string", "null"]},
                                "chr": {"type": "integer"},
                                "pos": {"type": "integer"},
                                "ref": {"type": "string"},
                                "alt": {"type": "string"},
                                "mlog10p": {"type": "number"},
                                "beta": {"type": "number"},
                                "se": {"type": "number"},
                                "pip": {"type": "number"},
                                "cs_id": {"type": "string"},
                                "cs_size": {"type": "integer"},
                                "cs_min_r2": {"type": "number"},
                                "aaf": {"type": "number"},
                                "most_severe": {"type": "string"},
                                "gene_most_severe": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "resource": "eqtl_catalogue",
                            "version": "R7",
                            "dataset": "QTD000608",
                            "data_type": "eQTL",
                            "trait": "ISG15",
                            "trait_original": "ENSG00000187608|ge",
                            "cell_type": "B_cell|naive",
                            "chr": 1,
                            "pos": 1000018,
                            "ref": "G",
                            "alt": "A",
                            "mlog10p": 7.3668,
                            "beta": -0.7921,
                            "se": 0.1434,
                            "pip": 0.0914,
                            "cs_id": "ENSG00000187608_L1",
                            "cs_size": 25,
                            "cs_min_r2": 0.7468,
                            "aaf": 0.3012,
                            "most_severe": "5_prime_UTR_variant",
                            "gene_most_severe": "HES4",
                        },
                        {"...": "..."},
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Resources not found"},
        422: {"description": "Invalid region, interval or format"},
        500: {"description": "Internal server error"},
    },
)
async def credible_sets_by_region(
    request: Request,
    region: str = Path(
        ..., description="Chromosome region", example="1:1000000-1000100"
    ),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available resources are used)",
    ),
    interval: int = Query(
        default=95, description="Credible set threshold (95 or 99)", ge=95, le=99
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    request_util: RequestUtil = Depends(get_request_util),
    data_access: DataAccess = Depends(get_data_access),
) -> Response:
    """
    Get credible sets across resources in a region.
    """
    start_time = time.time()
    if interval not in (95, 99):
        raise HTTPException(status_code=422, detail="Interval must be 95 or 99")
    if interval == 99:
        raise HTTPException(status_code=422, detail="Interval 99 is not supported yet")

    try:
        (chr, start, end) = request_util.validate_range(region, format)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    if resources is None:  # use all credible set resources if none given
        resources = config_util.get_resources(data_type="cs")
    if not request_util.check_resources(resources):
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource in {resources}. Available credible set resources: "
            + ", ".join(config_util.get_resources(data_type="cs")),
        )

    stream = await data_access.stream_range(
        chr,
        start,
        end,
        resources,
        "cs",
        config_common.read_chunk_size,
        config_common.response_chunk_size,
    )
    return await range_response(
        str(request.url),
        stream,
        config_credible_sets.cs_header_schema,
        format,
        start_time,
    )


@router.get(
    "/credible_sets_by_variant/{variant}",
    summary="Get credible sets across resources for a variant",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "resource\tversion\tdataset\tdata_type\ttrait\ttrait_original\tcell_type\tchr\tpos\tref\talt\tmlog10p\tbeta\tse\tpip\tcs_id\tcs_size\tcs_min_r2\taaf\tmost_severe\tgene_most_severe\neqtl_catalogue\tR7\tQTD000608\teQTL\tISG15\tENSG00000187608|ge\tB_cell|naive\t1\t1000018\tG\tA\t7.3668\t-7.921e-01\t1.434e-01\t0.0914\tENSG00000187608_L1\t25\t0.7468\t0.3012\t5_prime_UTR_variant\tHES4\n...",
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
                                "data_type": {"type": "string"},
                                "trait": {"type": "string"},
                                "trait_original": {"type": "string"},
                                "cell_type": {"type": ["string", "null"]},
                                "chr": {"type": "integer"},
                                "pos": {"type": "integer"},
                                "ref": {"type": "string"},
                                "alt": {"type": "string"},
                                "mlog10p": {"type": "number"},
                                "beta": {"type": "number"},
                                "se": {"type": "number"},
                                "pip": {"type": "number"},
                                "cs_id": {"type": "string"},
                                "cs_size": {"type": "integer"},
                                "cs_min_r2": {"type": "number"},
                                "aaf": {"type": "number"},
                                "most_severe": {"type": "string"},
                                "gene_most_severe": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "resource": "eqtl_catalogue",
                            "version": "R7",
                            "dataset": "QTD000608",
                            "data_type": "eQTL",
                            "trait": "ISG15",
                            "trait_original": "ENSG00000187608|ge",
                            "cell_type": "B_cell|naive",
                            "chr": 1,
                            "pos": 1000018,
                            "ref": "G",
                            "alt": "A",
                            "mlog10p": 7.3668,
                            "beta": -0.7921,
                            "se": 0.1434,
                            "pip": 0.0914,
                            "cs_id": "ENSG00000187608_L1",
                            "cs_size": 25,
                            "cs_min_r2": 0.7468,
                            "aaf": 0.3012,
                            "most_severe": "5_prime_UTR_variant",
                            "gene_most_severe": "HES4",
                        },
                        {"...": "..."},
                    ],
                },
            },
            401: {"description": "Not authenticated"},
            404: {"description": "Resources not found"},
            422: {"description": "Invalid variant, interval or format"},
            500: {"description": "Internal server error"},
        },
    },
)
async def credible_sets_by_variant(
    request: Request,
    variant: str = Path(
        ..., description="Variant (chr-pos-ref-alt)", example="19-44908684-T-C"
    ),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available resources are used)",
    ),
    interval: int = Query(
        default=95, description="Credible set threshold (95 or 99)", ge=95, le=99
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    request_util: RequestUtil = Depends(get_request_util),
    data_access: DataAccess = Depends(get_data_access),
) -> Response:
    """
    Get credible sets across resources for a variant.
    """
    start_time = time.time()
    if interval not in (95, 99):
        raise HTTPException(status_code=422, detail="Interval must be 95 or 99")
    if interval == 99:
        raise HTTPException(status_code=422, detail="Interval 99 is not supported yet")

    try:
        var = Variant(variant)
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))

    if resources is None:  # use all credible set resources if none given
        resources = config_util.get_resources(data_type="cs")
    if not request_util.check_resources(resources):
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource in {resources}. Available credible set resources: "
            + ", ".join(config_util.get_resources(data_type="cs")),
        )

    stream = await data_access.stream_range(
        var.chr,
        var.pos,
        var.pos,
        resources,
        "cs",
        config_common.read_chunk_size,
        config_common.response_chunk_size,
        var,
    )
    return await range_response(
        str(request.url),
        stream,
        config_credible_sets.cs_header_schema,
        format,
        start_time,
    )


class VariantsRequest(BaseModel):
    variants: str


@router.post(
    "/credible_sets_by_variant",
    summary="Get credible sets across resources for multiple variants",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {"type": "object"},
                    },
                },
            },
        },
        401: {"description": "Not authenticated"},
        422: {"description": "Invalid variant, interval or format"},
        500: {"description": "Internal server error"},
    },
)
async def credible_sets_by_variant_post(
    request: Request,
    body: VariantsRequest,
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available resources are used)",
    ),
    interval: int = Query(
        default=95, description="Credible set threshold (95 or 99)", ge=95, le=99
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    request_util: RequestUtil = Depends(get_request_util),
    data_access: DataAccess = Depends(get_data_access),
) -> Response:
    """
    Get credible sets across resources for multiple variants.
    Accepts a newline-separated list of variants in the request body.
    """
    start_time = time.time()
    if interval not in (95, 99):
        raise HTTPException(status_code=422, detail="Interval must be 95 or 99")
    if interval == 99:
        raise HTTPException(status_code=422, detail="Interval 99 is not supported yet")

    variant_strings = [v.strip() for v in body.variants.strip().split("\n") if v.strip()]
    if not variant_strings:
        raise HTTPException(status_code=422, detail="No variants provided")

    variants = []
    for vs in variant_strings:
        try:
            variants.append(Variant(vs))
        except ParseException as e:
            raise HTTPException(status_code=422, detail=f"Invalid variant '{vs}': {e}")

    if resources is None:
        resources = config_util.get_resources(data_type="cs")
    if not request_util.check_resources(resources):
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource in {resources}. Available credible set resources: "
            + ", ".join(config_util.get_resources(data_type="cs")),
        )

    stream = await data_access.stream_range_variants(
        variants,
        resources,
        "cs",
        config_common.read_chunk_size,
        config_common.response_chunk_size,
    )
    return await range_response(
        str(request.url),
        stream,
        config_credible_sets.cs_header_schema,
        format,
        start_time,
    )


@router.get(
    "/credible_sets_by_gene/{gene}",
    summary="Get credible sets across resources in a region around a gene",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "resource\tversion\tdataset\tdata_type\ttrait\ttrait_original\tcell_type\tchr\tpos\tref\talt\tmlog10p\tbeta\tse\tpip\tcs_id\tcs_size\tcs_min_r2\taaf\tmost_severe\tgene_most_severe\neqtl_catalogue\tR7\tQTD000572\teQTL\tCPSF1\tENSG00000071894.grp_2.contained.ENST00000533492|txrev\tneuron|naive\t8\t144503051\tG\tA\t2.6248\t-1.001e+00\t3.150e-01\t0.0217\tENSG00000071894.grp_2.contained.ENST00000533492_L2\t75\t0.6995\t0.01023\t5_prime_UTR_variant\tGPT\n...",
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
                                "data_type": {"type": "string"},
                                "trait": {"type": "string"},
                                "trait_original": {"type": "string"},
                                "cell_type": {"type": ["string", "null"]},
                                "chr": {"type": "integer"},
                                "pos": {"type": "integer"},
                                "ref": {"type": "string"},
                                "alt": {"type": "string"},
                                "mlog10p": {"type": "number"},
                                "beta": {"type": "number"},
                                "se": {"type": "number"},
                                "pip": {"type": "number"},
                                "cs_id": {"type": "string"},
                                "cs_size": {"type": "integer"},
                                "cs_min_r2": {"type": "number"},
                                "aaf": {"type": "number"},
                                "most_severe": {"type": "string"},
                                "gene_most_severe": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "resource": "eqtl_catalogue",
                            "version": "R7",
                            "dataset": "QTD000572",
                            "data_type": "eQTL",
                            "trait": "CPSF1",
                            "trait_original": "ENSG00000071894.grp_2.contained.ENST00000533492|txrev",
                            "cell_type": "neuron|naive",
                            "chr": 8,
                            "pos": 144503051,
                            "ref": "G",
                            "alt": "A",
                            "mlog10p": 2.6248,
                            "beta": -1.001,
                            "se": 0.315,
                            "pip": 0.0217,
                            "cs_id": "ENSG00000071894.grp_2.contained.ENST00000533492_L2",
                            "cs_size": 75,
                            "cs_min_r2": 0.6995,
                            "aaf": 0.01023,
                            "most_severe": "5_prime_UTR_variant",
                            "gene_most_severe": "GPT",
                        },
                        {"...": "..."},
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Gene or resources not found"},
        422: {"description": "Invalid window, interval or format"},
        500: {"description": "Internal server error"},
    },
)
async def credible_sets_by_gene(
    request: Request,
    gene: str = Path(..., description="Gene name or comma-separated list of gene names", example="GPT"),
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
    interval: int = Query(
        default=95, description="Credible set threshold (95 or 99)", ge=95, le=99
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    request_util: RequestUtil = Depends(get_request_util),
    data_access: DataAccess = Depends(get_data_access),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    """
    Get credible sets across resources in a region around a gene or comma-separated list of genes.
    """
    start_time = time.time()
    if interval not in (95, 99):
        raise HTTPException(status_code=422, detail="Interval must be 95 or 99")
    if interval == 99:
        raise HTTPException(status_code=422, detail="Interval 99 is not supported yet")

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

    for version in all_coords.keys():
        all_coords[version] = [
            {
                "chrom": pos["chrom"],
                "gene_start": pos["gene_start"] - window,
                "gene_end": pos["gene_end"] + window,
            }
            for pos in all_coords[version]
        ]

    if resources is None:  # use all credible set resources if none given
        resources = config_util.get_resources(data_type="cs")
    if not request_util.check_resources(resources):
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource in {resources}. Available credible set resources: "
            + ", ".join(config_util.get_resources(data_type="cs")),
        )

    stream = await data_access.stream_range_by_coords(
        coords=all_coords,
        resources=resources,
        data_type="cs",
        in_chunk_size=config_common.read_chunk_size,
        out_chunk_size=config_common.response_chunk_size,
    )
    return await range_response(
        str(request.url),
        stream,
        config_credible_sets.cs_header_schema,
        format,
        start_time,
    )


@router.get(
    "/credible_sets_by_qtl_gene/{gene}",
    summary="Get credible sets across resources for a QTL gene (returns credible sets anywhere in the genome associated with the gene)",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "resource\tversion\tdataset\tdata_type\ttrait\ttrait_original\tcell_type\tchr\tpos\tref\talt\tmlog10p\tbeta\tse\tpip\tcs_id\tcs_size\tcs_min_r2\taaf\tmost_severe\tgene_most_severe\ttrait_chr\ttrait_start\ttrait_end\neqtl_catalogue\tR7\tQTD000425\teQTL\tADAM17\tENSG00000151694.14_2_9494637_9494767|exon\tmonocyte|R848_6h\t2\t8538794\tT\tC\t5.6502\t0.347\t0.07094\t0.998\tENSG00000151694.14_2_9494637_9494767_L2\t1\t1.0\t0.1234\tintron_variant\tLINC01814\t2\t9488486\t9556732",
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
                                "data_type": {"type": "string"},
                                "trait": {"type": "string"},
                                "trait_original": {"type": "string"},
                                "cell_type": {"type": ["string", "null"]},
                                "chr": {"type": "integer"},
                                "pos": {"type": "integer"},
                                "ref": {"type": "string"},
                                "alt": {"type": "string"},
                                "mlog10p": {"type": "number"},
                                "beta": {"type": "number"},
                                "se": {"type": "number"},
                                "pip": {"type": "number"},
                                "cs_id": {"type": "string"},
                                "cs_size": {"type": "integer"},
                                "cs_min_r2": {"type": "number"},
                                "aaf": {"type": "number"},
                                "most_severe": {"type": "string"},
                                "gene_most_severe": {"type": "string"},
                                "trait_chr": {"type": "integer"},
                                "trait_start": {"type": "integer"},
                                "trait_end": {"type": "integer"},
                            },
                        },
                    },
                    "example": [
                        {
                            "resource": "eqtl_catalogue",
                            "version": "R7",
                            "dataset": "QTD000608",
                            "data_type": "eQTL",
                            "trait": "ISG15",
                            "trait_original": "ENSG00000187608|ge",
                            "cell_type": "B_cell|naive",
                            "chr": 1,
                            "pos": 1000018,
                            "ref": "G",
                            "alt": "A",
                            "mlog10p": 7.3668,
                            "beta": -0.7921,
                            "se": 0.1434,
                            "pip": 0.0914,
                            "cs_id": "ENSG00000187608_L1",
                            "cs_size": 25,
                            "cs_min_r2": 0.7468,
                            "aaf": 0.3012,
                            "most_severe": "5_prime_UTR_variant",
                            "gene_most_severe": "HES4",
                            "trait_chr": 1,
                            "trait_start": 1000000,
                            "trait_end": 2000000,
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
async def credible_sets_by_qtl_gene(
    request: Request,
    gene: str = Path(..., description="Gene name, ENSG ID, or comma-separated list of gene names", example="PCSK9"),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available resources are used)",
    ),
    interval: int = Query(
        default=95, description="Credible set threshold (95 or 99)", ge=95, le=99
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    request_util: RequestUtil = Depends(get_request_util),
    data_access: DataAccess = Depends(get_data_access),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    """
    Get credible sets across resources for a QTL gene or comma-separated list of genes (returns credible sets anywhere in the genome associated with the genes).
    """
    start_time = time.time()
    if resources is None:  # use all credible set resources if none given
        resources = config_util.get_resources(data_type="cs")
    if not request_util.check_resources(resources):
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource in {resources}. Available credible set resources: "
            + ", ".join(config_util.get_resources(data_type="cs")),
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
        stream = await data_access.stream_qtl_gene(
            all_coords,
            resources,
            "cs",
            config_common.read_chunk_size,
            config_common.response_chunk_size,
            interval,
        )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")
    return await range_response(
        str(request.url),
        stream,
        config_credible_sets.cs_qtl_header_schema,
        format,
        start_time,
    )


@router.get(
    "/credible_sets/{id_or_resource}/stats",
    summary="Get statistics for a credible set data file or resource",
    responses={
        200: {
            "description": "Statistics for the data file(s)",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "trait\ttrait_original\tdataset\tdata_type\tn_risk_cs\tn_risk_cs_with_coding\tn_risk_cs_with_coding_pip_gt_0_05\tn_risk_cs_with_lof\tn_risk_cs_with_lof_pip_gt_0_05\tn_protective_cs\tn_protective_cs_with_coding\tn_protective_cs_with_coding_pip_gt_0_05\tn_protective_cs_with_lof\tn_protective_cs_with_lof_pip_gt_0_05\nT2D_WIDE\tT2D_WIDE\tFinnGen_R13\tGWAS\t50\t20\t15\t5\t3\t30\t10\t8\t2\t1\n...",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "trait": {"type": "string"},
                                "trait_original": {"type": "string"},
                                "dataset": {"type": "string"},
                                "data_type": {"type": "string"},
                                "n_risk_cs": {"type": "integer"},
                                "n_risk_cs_with_coding": {"type": "integer"},
                                "n_risk_cs_with_coding_pip_gt_0_05": {"type": "integer"},
                                "n_risk_cs_with_lof": {"type": "integer"},
                                "n_risk_cs_with_lof_pip_gt_0_05": {"type": "integer"},
                                "n_protective_cs": {"type": "integer"},
                                "n_protective_cs_with_coding": {"type": "integer"},
                                "n_protective_cs_with_coding_pip_gt_0_05": {"type": "integer"},
                                "n_protective_cs_with_lof": {"type": "integer"},
                                "n_protective_cs_with_lof_pip_gt_0_05": {"type": "integer"},
                            },
                        },
                    },
                    "example": [
                        {
                            "trait": "T2D_WIDE",
                            "trait_original": "T2D_WIDE",
                            "dataset": "FinnGen_R13",
                            "data_type": "GWAS",
                            "n_risk_cs": 50,
                            "n_risk_cs_with_coding": 20,
                            "n_risk_cs_with_coding_pip_gt_0_05": 15,
                            "n_risk_cs_with_lof": 5,
                            "n_risk_cs_with_lof_pip_gt_0_05": 3,
                            "n_protective_cs": 30,
                            "n_protective_cs_with_coding": 10,
                            "n_protective_cs_with_coding_pip_gt_0_05": 8,
                            "n_protective_cs_with_lof": 2,
                            "n_protective_cs_with_lof_pip_gt_0_05": 1,
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Data file/resource not found or no stats available"},
        500: {"description": "Internal server error"},
    },
)
async def get_credible_set_stats(
    id_or_resource: str = Path(
        ...,
        description="Data file ID (e.g., finngen_gwas) or resource name (e.g., finngen)",
        example="finngen_gwas",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    stats_service: "CredibleSetStatsService" = Depends(get_credible_set_stats_service),
) -> Response:
    """
    Get statistics for a credible set data file or resource.

    If a data file ID is provided, returns stats for that file.
    If a resource name is provided, returns combined stats for all data files in that resource.
    """
    from fastapi.responses import PlainTextResponse, JSONResponse

    try:
        result = stats_service.get_stats(id_or_resource, format)

        if format == "tsv":
            return PlainTextResponse(result, media_type="text/tab-separated-values")
        else:
            return JSONResponse(result)

    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching stats for {id_or_resource}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
