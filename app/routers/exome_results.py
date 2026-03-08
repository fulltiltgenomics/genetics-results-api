import time
import logging
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from app.dependencies import (
    get_request_util,
    get_data_access,
    get_gene_name_mapping,
)
from app.core.responses import TimedStreamingResponse, TimedJSONResponse, range_response
from app.core.variant import Variant
from app.core.exceptions import (
    GeneNotFoundException,
    NotFoundException,
    ParseException,
)
from app.services.data_access import DataAccess
from app.services.request_util import RequestUtil
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
import app.config.exome_results as config_exome_results
import app.config.common as config_common

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/exome_results_by_phenotype/{resource}/{phenotype_or_study}",
    summary="Get exome results for a phenotype or study",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "dataset\tchr\tpos\tref\talt\tgene\tannotation\tmlog10p\tbeta\tse\taf_overall\taf_cases\taf_controls\tac\tan\theritability\ttrait\ngenebass\t1\t925947\tC\tT\tSAMD11\tsynonymous\t5.0214\t6.564e+00\t1.483e+00\t1.140e-05\t3.456e-03\t7.606e-06\t9\t789602\t2.868e-02\tcategorical_41210_both_sexes_S068_\n...",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "dataset": {"type": "string"},
                                "chr": {"type": "integer"},
                                "pos": {"type": "integer"},
                                "ref": {"type": "string"},
                                "alt": {"type": "string"},
                                "gene": {"type": "string"},
                                "annotation": {"type": "string"},
                                "mlog10p": {"type": "number"},
                                "beta": {"type": "number"},
                                "se": {"type": "number"},
                                "af_overall": {"type": "number"},
                                "af_cases": {"type": "number"},
                                "af_controls": {"type": "number"},
                                "ac": {"type": "integer"},
                                "an": {"type": "integer"},
                                "heritability": {"type": "number"},
                                "trait": {"type": "string"},
                            },
                        },
                    },
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Resource or phenotype not found"},
        422: {"description": "Invalid format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def exome_results_by_phenotype(
    request: Request,
    resource: str = Path(..., description="Data resource", example="genebass"),
    phenotype_or_study: str = Path(
        ...,
        description="Phenotype or study code",
        example="categorical_41210_both_sexes_S068_",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access: DataAccess = Depends(get_data_access),
) -> Response:
    """
    Get exome results for a phenotype or study.
    """
    start_time = time.time()
    logger.info(
        f"Getting exome results for phenotype or study: {phenotype_or_study} from resource: {resource}"
    )
    if format == "tsv":
        try:
            if not await data_access.check_phenotype_exists(
                resource, phenotype_or_study, None, "exome"
            ):
                logger.error(f"File not found: {phenotype_or_study}")
                raise NotFoundException(f"File not found: {phenotype_or_study}")
            stream = await data_access.stream_phenotype(
                resource,
                phenotype_or_study,
                None,
                config_common.read_chunk_size,
                "exome",
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
            if not await data_access.check_phenotype_exists(
                resource, phenotype_or_study, None, "exome"
            ):
                raise NotFoundException(f"File not found: {phenotype_or_study}")
            logger.info(
                f"Getting exome results for phenotype or study: {phenotype_or_study} from resource: {resource} in JSON format"
            )
            rows = await data_access.json_phenotype(
                resource,
                phenotype_or_study,
                None,
                config_exome_results.exome_header_schema,
                "exome",
                config_common.read_chunk_size,
            )
            logger.info(
                f"Got {len(rows)} exome results for phenotype or study: {phenotype_or_study} from resource: {resource} in JSON format in {time.time() - start_time} seconds"
            )
            return TimedJSONResponse(rows, request.url, start_time)
        except NotFoundException as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            logger.error(
                f"Error getting JSON phenotype {phenotype_or_study} from {resource}: {e}"
            )
            raise HTTPException(status_code=500, detail="Internal server error")


@router.get(
    "/exome_results_by_region/{region}",
    summary="Get exome results across resources in a region",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "resource\tversion\tdataset\tchr\tpos\tref\talt\tgene\tannotation\tmlog10p\tbeta\tse\taf_overall\taf_cases\taf_controls\tac\tan\theritability\ttrait\ngenebass\tNA\tgenebass\t1\t55050000\tC\tT\tPCSK9\tmissense\t5.0214\t6.564e+00\t1.483e+00\t1.140e-05\t3.456e-03\t7.606e-06\t9\t789602\t2.868e-02\tcategorical_41210_both_sexes_S068_\n...",
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
                                "chr": {"type": "integer"},
                                "pos": {"type": "integer"},
                                "ref": {"type": "string"},
                                "alt": {"type": "string"},
                                "gene": {"type": "string"},
                                "annotation": {"type": "string"},
                                "mlog10p": {"type": "number"},
                                "beta": {"type": "number"},
                                "se": {"type": "number"},
                                "af_overall": {"type": "number"},
                                "af_cases": {"type": "number"},
                                "af_controls": {"type": "number"},
                                "ac": {"type": "integer"},
                                "an": {"type": "integer"},
                                "heritability": {"type": "number"},
                                "trait": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "resource": "genebass",
                            "version": "NA",
                            "dataset": "genebass",
                            "chr": 1,
                            "pos": 55050000,
                            "ref": "C",
                            "alt": "T",
                            "gene": "PCSK9",
                            "annotation": "missense",
                            "mlog10p": 5.0214,
                            "beta": 6.564,
                            "se": 1.483,
                            "af_overall": 0.0000114,
                            "af_cases": 0.003456,
                            "af_controls": 0.000007606,
                            "ac": 9,
                            "an": 789602,
                            "heritability": 0.02868,
                            "trait": "categorical_41210_both_sexes_S068_",
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Resources not found"},
        422: {"description": "Invalid region or format"},
        500: {"description": "Internal server error"},
    },
)
async def exome_results_by_region(
    request: Request,
    region: str = Path(
        ..., description="Chromosome region", example="1:1000000-1000100"
    ),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available exome resources are used)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    request_util: RequestUtil = Depends(get_request_util),
    data_access: DataAccess = Depends(get_data_access),
) -> Response:
    """
    Get exome results across resources in a region.
    """
    start_time = time.time()

    try:
        (chr, start, end) = request_util.validate_range(region, format)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    if resources is None:  # use all exome resources if none given
        resources = list(config_exome_results.resource_to_exome_data_file_ids.keys())
    if not request_util.check_resources(resources):
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource in {resources}. Available exome resources: "
            + ", ".join(config_exome_results.resource_to_exome_data_file_ids.keys()),
        )

    stream = await data_access.stream_range(
        chr,
        start,
        end,
        resources,
        "exome",
        config_common.read_chunk_size,
        config_common.response_chunk_size,
    )
    return await range_response(
        str(request.url),
        stream,
        config_exome_results.exome_header_schema,
        format,
        start_time,
    )


@router.get(
    "/exome_results_by_variant/{variant}",
    summary="Get exome results across resources for a variant",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "resource\tversion\tdataset\tchr\tpos\tref\talt\tgene\tannotation\tmlog10p\tbeta\tse\taf_overall\taf_cases\taf_controls\tac\tan\theritability\ttrait\ngenebass\tNA\tgenebass\t1\t925947\tC\tT\tSAMD11\tsynonymous\t5.0214\t6.564e+00\t1.483e+00\t1.140e-05\t3.456e-03\t7.606e-06\t9\t789602\t2.868e-02\tcategorical_41210_both_sexes_S068_\n...",
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
                                "chr": {"type": "integer"},
                                "pos": {"type": "integer"},
                                "ref": {"type": "string"},
                                "alt": {"type": "string"},
                                "gene": {"type": "string"},
                                "annotation": {"type": "string"},
                                "mlog10p": {"type": "number"},
                                "beta": {"type": "number"},
                                "se": {"type": "number"},
                                "af_overall": {"type": "number"},
                                "af_cases": {"type": "number"},
                                "af_controls": {"type": "number"},
                                "ac": {"type": "integer"},
                                "an": {"type": "integer"},
                                "heritability": {"type": "number"},
                                "trait": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "resource": "genebass",
                            "version": "NA",
                            "dataset": "genebass",
                            "chr": 1,
                            "pos": 925947,
                            "ref": "C",
                            "alt": "T",
                            "gene": "SAMD11",
                            "annotation": "synonymous",
                            "mlog10p": 5.0214,
                            "beta": 6.564,
                            "se": 1.483,
                            "af_overall": 0.0000114,
                            "af_cases": 0.003456,
                            "af_controls": 0.000007606,
                            "ac": 9,
                            "an": 789602,
                            "heritability": 0.02868,
                            "trait": "categorical_41210_both_sexes_S068_",
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Resources not found"},
        422: {"description": "Invalid variant or format"},
        500: {"description": "Internal server error"},
    },
)
async def exome_results_by_variant(
    request: Request,
    variant: str = Path(
        ..., description="Variant (chr-pos-ref-alt)", example="1-925947-C-T"
    ),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available exome resources are used)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    request_util: RequestUtil = Depends(get_request_util),
    data_access: DataAccess = Depends(get_data_access),
) -> Response:
    """
    Get exome results across resources for a variant.
    """
    start_time = time.time()

    try:
        var = Variant(variant)
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))

    if resources is None:  # use all exome resources if none given
        resources = list(config_exome_results.resource_to_exome_data_file_ids.keys())
    if not request_util.check_resources(resources):
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource in {resources}. Available exome resources: "
            + ", ".join(config_exome_results.resource_to_exome_data_file_ids.keys()),
        )

    stream = await data_access.stream_range(
        var.chr,
        var.pos,
        var.pos,
        resources,
        "exome",
        config_common.read_chunk_size,
        config_common.response_chunk_size,
        var,
    )
    return await range_response(
        str(request.url),
        stream,
        config_exome_results.exome_header_schema,
        format,
        start_time,
    )


@router.get(
    "/exome_results_by_gene/{gene}",
    summary="Get exome results across resources in a region around a gene",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "resource\tversion\tdataset\tchr\tpos\tref\talt\tgene\tannotation\tmlog10p\tbeta\tse\taf_overall\taf_cases\taf_controls\tac\tan\theritability\ttrait\ngenebass\tNA\tgenebass\t1\t925947\tC\tT\tSAMD11\tsynonymous\t5.0214\t6.564e+00\t1.483e+00\t1.140e-05\t3.456e-03\t7.606e-06\t9\t789602\t2.868e-02\tcategorical_41210_both_sexes_S068_\n...",
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
                                "chr": {"type": "integer"},
                                "pos": {"type": "integer"},
                                "ref": {"type": "string"},
                                "alt": {"type": "string"},
                                "gene": {"type": "string"},
                                "annotation": {"type": "string"},
                                "mlog10p": {"type": "number"},
                                "beta": {"type": "number"},
                                "se": {"type": "number"},
                                "af_overall": {"type": "number"},
                                "af_cases": {"type": "number"},
                                "af_controls": {"type": "number"},
                                "ac": {"type": "integer"},
                                "an": {"type": "integer"},
                                "heritability": {"type": "number"},
                                "trait": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "resource": "genebass",
                            "version": "NA",
                            "dataset": "genebass",
                            "chr": 1,
                            "pos": 925947,
                            "ref": "C",
                            "alt": "T",
                            "gene": "SAMD11",
                            "annotation": "synonymous",
                            "mlog10p": 5.0214,
                            "beta": 6.564,
                            "se": 1.483,
                            "af_overall": 0.0000114,
                            "af_cases": 0.003456,
                            "af_controls": 0.000007606,
                            "ac": 9,
                            "an": 789602,
                            "heritability": 0.02868,
                            "trait": "categorical_41210_both_sexes_S068_",
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Gene or resources not found"},
        422: {"description": "Invalid window or format"},
        500: {"description": "Internal server error"},
    },
)
async def exome_results_by_gene(
    request: Request,
    gene: str = Path(..., description="Gene name or comma-separated list of gene names", example="SAMD11"),
    window: int = Query(
        default=0,
        description="One-sided window in base pairs around the gene (default 0, which means the gene itself)",
        ge=0,
        le=int(config_common.max_gene_window),
    ),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available exome resources are used)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
    request_util: RequestUtil = Depends(get_request_util),
    data_access: DataAccess = Depends(get_data_access),
) -> Response:
    """
    Get exome results across resources in a region around a gene or comma-separated list of genes.
    """
    start_time = time.time()

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

    if resources is None:  # use all exome resources if none given
        resources = list(config_exome_results.resource_to_exome_data_file_ids.keys())
    if not request_util.check_resources(resources):
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource in {resources}. Available exome resources: "
            + ", ".join(config_exome_results.resource_to_exome_data_file_ids.keys()),
        )

    try:
        stream = await data_access.stream_range_by_coords(
            coords=all_coords,
            resources=resources,
            data_type="exome",
            in_chunk_size=config_common.read_chunk_size,
            out_chunk_size=config_common.response_chunk_size,
        )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    return await range_response(
        str(request.url),
        stream,
        config_exome_results.exome_header_schema,
        format,
        start_time,
    )
