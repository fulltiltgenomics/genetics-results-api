import time
import logging
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from app.dependencies import get_data_access_coloc
from app.core.responses import range_response
from app.core.variant import Variant
from app.core.exceptions import (
    DataException,
    NotFoundException,
    ParseException,
)
from app.services.data_access_coloc import DataAccessColoc
import app.config.coloc as coloc_config

import app.config.common as config_common

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/colocalization_by_variant/{variant}",
    summary="Get colocalization trait pairs where the given variant is in a credible set of either trait.",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "dataset1\tdataset2\ttrait1\ttrait2\tcs1_id\tcs2_id\thit1\thit2\tchr\tregion_start_min\tregion_end_max\tPP.H0.abf\tPP.H1.abf\tPP.H2.abf\tPP.H3.abf\tPP.H4.abf\tnsnps\tnsnps1\tnsnps2\tcs1_log10bf\tcs2_log10bf\tclpp\tclpa\tcs1_size\tcs2_size\tcs_overlap\ttopInOverlap\tnFinnGen_R13\tFinnGen_kanta\tE4_FH\tnonHDL\tchr1:53539974-56539974_1\tchr1:47813785-56873172_1\t1:55039974:G:T\t1:55039974:G:T\t1\t47813785\t56873172\t0.000e+00\t0.000e+00\t2.992e-15\t2.643e-06\t1.000e+00\t21673\t21725\t56697\t11.4888\tinf\t9.987e-01\t9.987e-01\t1\t1\t1\t1,1",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "resource1": {"type": "string"},
                                "version1": {"type": "string"},
                                "resource2": {"type": "string"},
                                "version2": {"type": "string"},
                                "dataset1": {"type": "string"},
                                "dataset2": {"type": "string"},
                                "data_type1": {"type": "string"},
                                "data_type2": {"type": "string"},
                                "trait1": {"type": "string"},
                                "trait1_original": {"type": "string"},
                                "trait2": {"type": "string"},
                                "trait2_original": {"type": "string"},
                                "cell_type1": {"type": "string"},
                                "cell_type2": {"type": "string"},
                                "cs1_id": {"type": "string"},
                                "cs2_id": {"type": "string"},
                                "hit1": {"type": "string"},
                                "hit2": {"type": "string"},
                                "hit1_beta": {"type": "number"},
                                "hit1_mlog10p": {"type": "number"},
                                "hit2_beta": {"type": "number"},
                                "hit2_mlog10p": {"type": "number"},
                                "chr": {"type": "integer"},
                                "region_start_min": {"type": "integer"},
                                "region_end_max": {"type": "integer"},
                                "PP.H0.abf": {"type": "number"},
                                "PP.H1.abf": {"type": "number"},
                                "PP.H2.abf": {"type": "number"},
                                "PP.H3.abf": {"type": "number"},
                                "PP.H4.abf": {"type": "number"},
                                "nsnps": {"type": "integer"},
                                "nsnps1": {"type": "integer"},
                                "nsnps2": {"type": "integer"},
                                "cs1_log10bf": {"type": "number"},
                                "cs2_log10bf": {"type": "number"},
                                "clpp": {"type": "number"},
                                "clpa": {"type": "number"},
                                "cs1_size": {"type": "integer"},
                                "cs2_size": {"type": "integer"},
                                "cs_overlap": {"type": "integer"},
                                "topInOverlap": {"type": "string"},
                                "variant_dataset": {
                                    "type": "string",
                                    "description": "Present when include_variants=true",
                                },
                                "variant_data_type": {
                                    "type": "string",
                                    "description": "Present when include_variants=true",
                                },
                                "variant_trait": {
                                    "type": "string",
                                    "description": "Present when include_variants=true",
                                },
                                "variant_trait_original": {
                                    "type": "string",
                                    "description": "Present when include_variants=true",
                                },
                                "variant_cell_type": {
                                    "type": ["string", "null"],
                                    "description": "Present when include_variants=true",
                                },
                                "variant_pos": {
                                    "type": "integer",
                                    "description": "Present when include_variants=true",
                                },
                                "variant_ref": {
                                    "type": "string",
                                    "description": "Present when include_variants=true",
                                },
                                "variant_alt": {
                                    "type": "string",
                                    "description": "Present when include_variants=true",
                                },
                                "variant_mlog10p": {
                                    "type": "number",
                                    "description": "Present when include_variants=true",
                                },
                                "variant_beta": {
                                    "type": "number",
                                    "description": "Present when include_variants=true",
                                },
                                "variant_se": {
                                    "type": "number",
                                    "description": "Present when include_variants=true",
                                },
                                "variant_pip": {
                                    "type": "number",
                                    "description": "Present when include_variants=true",
                                },
                            },
                        },
                    },
                    "example": [
                        {
                            "resource1": "finngen",
                            "version1": "R13",
                            "resource2": "finngen",
                            "version2": "kanta",
                            "dataset1": "FinnGen_R13",
                            "dataset2": "FinnGen_kanta",
                            "data_type1": "GWAS",
                            "data_type2": "GWAS",
                            "trait1": "E4_FH",
                            "trait1_original": "E4_FH",
                            "trait2": "nonHDL",
                            "trait2_original": "nonHDL",
                            "cell_type1": "NA",
                            "cell_type2": "NA",
                            "cs1_id": "chr1:53539974-56539974_1",
                            "cs2_id": "chr1:47813785-56873172_1",
                            "hit1": "1:55039974:G:T",
                            "hit2": "1:55039974:G:T",
                            "hit1_beta": 1,
                            "hit1_mlog10p": 100,
                            "hit2_beta": 2,
                            "hit2_mlog10p": 9.2,
                            "chr": 1,
                            "region_start_min": 47813785,
                            "region_end_max": 56873172,
                            "PP.H0.abf": 0,
                            "PP.H1.abf": 0,
                            "PP.H2.abf": 2.992e-15,
                            "PP.H3.abf": 2.643e-06,
                            "PP.H4.abf": 1,
                            "nsnps": 21673,
                            "nsnps1": 21725,
                            "nsnps2": 56697,
                            "cs1_log10bf": 11.4888,
                            "cs2_log10bf": 1e308,
                            "clpp": 0.9987,
                            "clpa": 0.9987,
                            "cs1_size": 1,
                            "cs2_size": 1,
                            "cs_overlap": 1,
                            "topInOverlap": "1,1",
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Variant not found in colocalization data"},
        422: {"description": "Invalid variant, include_variants or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def colocalization_by_variant(
    request: Request,
    variant: str = Path(
        ..., description="Variant (chr-pos-ref-alt)", example="19-44908684-T-C"
    ),
    include_variants: bool = Query(
        default=False,
        description="Include credible set variant statistics in the response",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access_coloc: DataAccessColoc = Depends(get_data_access_coloc),
) -> Response:
    """
    Get colocalization trait pairs where the given variant is in a credible set of either trait.

    Response schema depends on the include_variants parameter:
    - include_variants=false (default): Returns colocalization pairs only (one row per pair)
    - include_variants=true: Returns colocalization pairs plus statistics of all variants in each credible set (one row per variant)
    """
    start_time = time.time()
    try:
        variant = Variant(variant)
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))
    try:
        if include_variants:
            stream = await data_access_coloc.stream_coloc_variants_by_variant(
                variant,
                config_common.read_chunk_size,
                config_common.response_chunk_size,
            )
        else:
            stream = await data_access_coloc.stream_coloc_by_variant(
                variant,
                config_common.read_chunk_size,
                config_common.response_chunk_size,
            )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")
    schema = (
        {
            **{
                f"variant_{k}": v
                for k, v in coloc_config.coloc_credset_header_schema.items()
            },
            **coloc_config.coloc_header_schema,
        }
        if include_variants
        else coloc_config.coloc_header_schema
    )
    return await range_response(str(request.url), stream, schema, format, start_time)


@router.get(
    "/colocalization_by_variant/{variant}/{resource}/{phenotype}",
    summary="Get colocalized credible sets for a variant filtered by resource and phenotype.",
)
async def colocalization_by_variant_filtered(
    request: Request,
    variant: str = Path(
        ..., description="Variant (chr-pos-ref-alt)", example="19-44908684-T-C"
    ),
    resource: str = Path(
        ..., description="Resource name to filter by", example="finngen"
    ),
    phenotype: str = Path(
        ..., description="Phenotype or study to filter by", example="I9_HYPERLIPID"
    ),
    include_variants: bool = Query(
        default=False,
        description="Include credible set variant statistics in the response",
    ),
    dual_format: bool = Query(
        default=False,
        description="Return dual format response (including columns for both traits)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access_coloc: DataAccessColoc = Depends(get_data_access_coloc),
) -> Response:
    """
    Get colocalization trait pairs where the given variant is in a credible set
    of either trait, filtered by resource and phenotype.

    Response schema depends on the include_variants parameter:
    - include_variants=false (default): Returns colocalization pairs only (one row per pair)
    - include_variants=true: Returns colocalization pairs plus statistics of all variants in each credible set (one row per variant)
    """
    start_time = time.time()
    try:
        variant = Variant(variant)
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))
    try:
        if include_variants:
            raise HTTPException(
                status_code=400,
                detail="include_variants is not supported with resource/phenotype filtering",
            )

        stream = await data_access_coloc.stream_coloc_by_variant(
            variant,
            config_common.read_chunk_size,
            config_common.response_chunk_size,
            resource=resource,
            phenotype_or_study=phenotype,
            simple=not dual_format,
        )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")

    # use simple schema when dual_format is False
    schema = (
        coloc_config.coloc_header_schema_simple
        if not dual_format
        else coloc_config.coloc_header_schema
    )
    return await range_response(str(request.url), stream, schema, format, start_time)


# TODO use phenotype specific file when available
@router.get(
    "/colocalization_by_credible_set_id/{resource}/{phenotype_or_study}/{credible_set_id}",
    summary="Get colocalized credible sets for a credible set id.",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "resource1\tversion1\tresource2\tversion2\tdataset1\tdataset2\tdata_type1\tdata_type2\ttrait1\ttrait1_original\ttrait2\ttrait2_original\tcell_type1\tcell_type2\tcs1_id\tcs2_id\thit1\thit2\tchr\tregion_start_min\tregion_end_max\tPP.H0.abf\tPP.H1.abf\tPP.H2.abf\tPP.H3.abf\tPP.H4.abf\tnsnps\tnsnps1\tnsnps2\tcs1_log10bf\tcs2_log10bf\tclpp\tclpa\tcs1_size\tcs2_size\tcs_overlap\ttopInOverlap\tnFinnGen_R13\tFinnGen_kanta\tE4_FH\tnonHDL\tchr1:53539974-56539974_1\tchr1:47813785-56873172_1\t1:55039974:G:T\t1:55039974:G:T\t1\t47813785\t56873172\t0.000e+00\t0.000e+00\t2.992e-15\t2.643e-06\t1.000e+00\t21673\t21725\t56697\t11.4888\tinf\t9.987e-01\t9.987e-01\t1\t1\t1\t1,1",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "resource1": {"type": "string"},
                                "version1": {"type": "string"},
                                "resource2": {"type": "string"},
                                "version2": {"type": "string"},
                                "dataset1": {"type": "string"},
                                "dataset2": {"type": "string"},
                                "data_type1": {"type": "string"},
                                "data_type2": {"type": "string"},
                                "trait1": {"type": "string"},
                                "trait1_original": {"type": "string"},
                                "trait2": {"type": "string"},
                                "trait2_original": {"type": "string"},
                                "cell_type1": {"type": "string"},
                                "cell_type2": {"type": "string"},
                                "cs1_id": {"type": "string"},
                                "cs2_id": {"type": "string"},
                                "hit1": {"type": "string"},
                                "hit2": {"type": "string"},
                                "chr": {"type": "integer"},
                                "region_start_min": {"type": "integer"},
                                "region_end_max": {"type": "integer"},
                                "PP.H0.abf": {"type": "number"},
                                "PP.H1.abf": {"type": "number"},
                                "PP.H2.abf": {"type": "number"},
                                "PP.H3.abf": {"type": "number"},
                                "PP.H4.abf": {"type": "number"},
                                "nsnps": {"type": "integer"},
                                "nsnps1": {"type": "integer"},
                                "nsnps2": {"type": "integer"},
                                "cs1_log10bf": {"type": "number"},
                                "cs2_log10bf": {"type": "number"},
                                "clpp": {"type": "number"},
                                "clpa": {"type": "number"},
                                "cs1_size": {"type": "integer"},
                                "cs2_size": {"type": "integer"},
                                "cs_overlap": {"type": "integer"},
                                "topInOverlap": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "resource1": "finngen",
                            "version1": "R13",
                            "resource2": "finngen",
                            "version2": "kanta",
                            "dataset1": "FinnGen_R13",
                            "dataset2": "FinnGen_kanta",
                            "trait1": "E4_FH",
                            "trait2": "nonHDL",
                            "cs1_id": "chr1:53539974-56539974_1",
                            "cs2_id": "chr1:47813785-56873172_1",
                            "hit1": "1:55039974:G:T",
                            "hit2": "1:55039974:G:T",
                            "chr": 1,
                            "region_start_min": 47813785,
                            "region_end_max": 56873172,
                            "PP.H0.abf": 0,
                            "PP.H1.abf": 0,
                            "PP.H2.abf": 2.992e-15,
                            "PP.H3.abf": 2.643e-06,
                            "PP.H4.abf": 1,
                            "nsnps": 21673,
                            "nsnps1": 21725,
                            "nsnps2": 56697,
                            "cs1_log10bf": 11.4888,
                            "cs2_log10bf": 1e308,
                            "clpp": 0.9987,
                            "clpa": 0.9987,
                            "cs1_size": 1,
                            "cs2_size": 1,
                            "cs_overlap": 1,
                            "topInOverlap": "1,1",
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        # TODO what about cs_id not found
        # 404: {"description": "Resource or phenotype not found"},
        422: {"description": "Invalid format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def colocalization_by_credible_set_id(
    request: Request,
    resource: str = Path(..., description="Resource", example="finngen"),
    phenotype_or_study: str = Path(
        ..., description="Phenotype or study", example="K11_IBD_STRICT"
    ),
    credible_set_id: str = Path(
        ..., description="Credible set id", example="chr1:65744548-68744548_3"
    ),
    dual_format: bool = Query(
        default=False,
        description="Return dual format response (including columns for both traits)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access_coloc: DataAccessColoc = Depends(get_data_access_coloc),
) -> Response:
    """
    Get colocalized credible sets for a credible set id.
    """
    start_time = time.time()
    try:
        stream = await data_access_coloc.stream_coloc_by_credible_set_id(
            resource,
            phenotype_or_study,
            credible_set_id,
            config_common.read_chunk_size,
            config_common.response_chunk_size,
            simple=not dual_format,
        )
    except NotFoundException as e:
        logger.warning(e)
        raise HTTPException(status_code=404, detail=str(e))
    except DataException as e:
        logger.warning(e)
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")
    schema = (
        coloc_config.coloc_header_schema_simple
        if not dual_format
        else coloc_config.coloc_header_schema
    )
    return await range_response(str(request.url), stream, schema, format, start_time)
