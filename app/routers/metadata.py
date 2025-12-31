import logging
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from app.dependencies import get_data_access
from app.services import config_util
from app.services.data_access import DataAccess

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get(
    "/resource_metadata/{resource}",
    summary="Get harmonized phenotype metadata for a resource. Returns metadata in unified format across all resources: "
    + ", ".join(config_util.get_resources_with_metadata()),
    responses={
        200: {
            "description": "Successful response with harmonized metadata",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "phenotype_code\tphenotype_string\tn_samples\tn_cases\tn_controls\ttrait_type\tauthor\tdate\tresource\tversion\nI9_HYPERLIPID\tHyperlipidaemia\t156438\t78219\t78219\tbinary\tFinnGen\t2025-09-01\tfinngen\tR13\n",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "phenotype_code": {"type": "string"},
                                "phenotype_string": {"type": "string"},
                                "n_samples": {"type": "integer"},
                                "n_cases": {"type": "integer"},
                                "n_controls": {"type": "integer"},
                                "trait_type": {
                                    "type": "string",
                                    "enum": ["binary", "quantitative"],
                                },
                                "author": {"type": "string"},
                                "date": {"type": "string", "format": "date"},
                                "resource": {"type": "string"},
                                "version": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "phenotype_code": "I9_HYPERLIPID",
                            "phenotype_string": "Hyperlipidaemia",
                            "n_samples": 156438,
                            "n_cases": 78219,
                            "n_controls": 78219,
                            "trait_type": "binary",
                            "author": "FinnGen",
                            "date": "2025-09-01",
                            "resource": "finngen",
                            "version": "R13",
                        },
                        {
                            "phenotype_code": "QTD000001",
                            "phenotype_string": "Alasoo_2018 - macrophage - naive",
                            "n_samples": 84,
                            "n_cases": 0,
                            "n_controls": 0,
                            "trait_type": "quantitative",
                            "author": "Alasoo_2018",
                            "date": "2020-01-01",
                            "resource": "eqtl_catalogue",
                            "version": "R7",
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Resource not found"},
        422: {"description": "Invalid format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def resource_metadata(
    resource: str = Path(..., description="Data resource", example="finngen"),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access: DataAccess = Depends(get_data_access),
) -> Response:
    """
    Get harmonized phenotype metadata for a resource in unified format.

    All resources return data with the same schema:
    - phenotype_code: Phenotype or study identifier
    - phenotype_string: Human-readable phenotype name
    - n_samples: Total number of samples
    - n_cases: Number of cases (0 for quantitative traits)
    - n_controls: Number of controls (0 for quantitative traits)
    - trait_type: "binary" or "quantitative"
    - author: Study author or consortium name
    - date: Publication or release date (YYYY-MM-DD)
    - resource: Resource name
    - version: Resource version
    """
    from app.services.config_util import get_resources

    if resource not in get_resources():
        raise HTTPException(status_code=404, detail=f"Resource {resource} not found")
    try:
        meta = data_access.get_harmonized_metadata(resource)
        if not meta:
            raise HTTPException(
                status_code=404,
                detail=f"No metadata available for resource {resource}",
            )

        if format == "tsv":

            def generate_tsv():
                yield "\t".join(meta[0].keys()) + "\n"
                for row in meta:
                    yield "\t".join(map(str, row.values())) + "\n"

            return StreamingResponse(
                generate_tsv(), media_type="text/tab-separated-values"
            )
        elif format == "json":
            return JSONResponse(meta)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reading harmonized metadata for {resource}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Error reading harmonized metadata for {resource}",
        )


@router.get(
    "/trait_name_mapping",
    summary="Get trait name mapping",
    responses={
        200: {
            "description": "Successful response",
            "content": {"application/json": {"schema": {"type": "object"}}},
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Trait name mapping not found"},
        500: {"description": "Internal server error"},
    },
    include_in_schema=False,
)
async def trait_name_mapping(
    request: Request,
    data_access: DataAccess = Depends(get_data_access),
) -> Response:
    """
    Get trait name mapping.
    """
    resources = config_util.get_resources_with_metadata()
    trait_map = {}
    metadata = []
    for resource in resources:
        meta = data_access.get_resource_metadata(resource)
        if resource == "finngen":
            # finngen includes multiple data files (e.g. core, kanta) with different schemas
            for row in meta:
                if "phenocode" in row:
                    trait_map[row["phenocode"]] = row["phenostring"]
                elif "OMOPID" in row:
                    trait_map[row["OMOPID"]] = row["phenostring"]
        elif resource == "open_targets":
            for row in meta:
                trait_map[row["studyId"]] = row["traitFromSource"]
        elif resource == "eqtl_catalogue":
            # eqtl_catalogue might have dataset_id field
            for row in meta:
                if "dataset_id" in row and "sample_group" in row:
                    trait_map[row["dataset_id"]] = row["sample_group"]
        elif resource == "genebass":
            # genebass trait names are: trait_type_phenocode_pheno_sex_coding_modifier
            for row in meta:
                trait_id = "_".join([
                    str(row.get("trait_type", "")),
                    str(row.get("phenocode", "")),
                    str(row.get("pheno_sex", "")),
                    str(row.get("coding", "")),
                    str(row.get("modifier", "")),
                ])
                description = row.get("description", "")
                coding_description = row.get("coding_description", "")
                if not description or description == "NA":
                    trait_map[trait_id] = str(row.get("phenocode", ""))
                elif coding_description and coding_description != "NA":
                    trait_map[trait_id] = f"{description}: {coding_description}"
                else:
                    trait_map[trait_id] = description
        metadata.extend(meta)
    return JSONResponse(trait_map)
