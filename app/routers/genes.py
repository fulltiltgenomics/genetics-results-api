import logging
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from app.dependencies import get_gene_name_mapping
from app.core.variant import Variant
from app.core.exceptions import (
    ParseException,
)
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/genes_in_region/{chr}/{start}/{end}",
    include_in_schema=False,
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "gene_name\tchrom\tgene_start\tgene_end\tgene_strand\tgene_type\thgnc_symbol\thgnc_name\thgnc_alias_symbol\thgnc_prev_symbol\nBSND\t1\t54998933\t55017172\t+\tprotein_coding\tBSND\tbarttin CLCNK type accessory subunit beta\tBART\tDFNB73\nPCSK9\t1\t55039445\t55064852\t+\tprotein_coding\tPCSK9\tproprotein convertase subtilisin/kexin type 9\tNARC-1|FH3\tHCHOLA3\n...",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "gene_name": {"type": "string"},
                                "chrom": {"type": "integer"},
                                "gene_start": {"type": "integer"},
                                "gene_end": {"type": "integer"},
                                "gene_strand": {"type": "string"},
                                "gene_type": {"type": "string"},
                                "hgnc_symbol": {"type": "string"},
                                "hgnc_name": {"type": "string"},
                                "hgnc_alias_symbol": {"type": "string"},
                                "hgnc_prev_symbol": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "gene_name": "BSND",
                            "chrom": 1,
                            "gene_start": 54998933,
                            "gene_end": 55017172,
                            "gene_strand": "+",
                            "gene_type": "protein_coding",
                            "hgnc_symbol": "BSND",
                            "hgnc_name": "barttin CLCNK type accessory subunit beta",
                            "hgnc_alias_symbol": "BART",
                            "hgnc_prev_symbol": "DFNB73",
                        },
                        {
                            "gene_name": "PCSK9",
                            "chrom": 1,
                            "gene_start": 55039445,
                            "gene_end": 55064852,
                            "gene_strand": "+",
                            "gene_type": "protein_coding",
                            "hgnc_symbol": "PCSK9",
                            "hgnc_name": "proprotein convertase subtilisin/kexin type 9",
                            "hgnc_alias_symbol": "NARC-1|FH3",
                            "hgnc_prev_symbol": "HCHOLA3",
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "No genes found"},
        422: {"description": "Invalid chromosome"},
        500: {"description": "Internal server error"},
    },
)
async def genes_in_region(
    chr: str,
    start: int,
    end: int,
    gene_type: Literal["protein_coding", "all"] = Query(
        default="protein_coding", description="Type of genes to return"
    ),
    gencode_version: str = Query(default=None, description="Gencode version to use"),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    try:
        chr = int(
            chr.lower()
            .replace("chr", "")
            .replace("x", "23")
            .replace("y", "24")
            .replace("mt", "25")
            .strip()
        )
    except ValueError:
        raise HTTPException(status_code=422, detail="invalid chromosome")
    genes = gene_name_and_position_mapping.get_genes_in_region(
        chr, start, end, gene_type=gene_type, gencode_version=gencode_version
    )
    if format == "tsv":
        if not genes:
            raise HTTPException(
                status_code=404,
                detail=f"No genes in type {gene_type} in gencode version {gencode_version} found within {start}-{end} on chromosome {chr}",
            )
        else:
            header = "\t".join(genes[0].keys())
            rows = "\n".join(
                "\t".join(str(v) if v is not None else "NA" for v in gene.values())
                for gene in genes
            )
            tsv = f"{header}\n{rows}\n"
        return PlainTextResponse(tsv, media_type="text/tab-separated-values")
    else:
        return genes


@router.get(
    "/nearest_genes/{variant}",
    summary="Get nearest genes to a variant",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "gene_name\tchrom\tgene_start\tgene_end\tgene_strand\tgene_type\tdistance\thgnc_symbol\thgnc_name\thgnc_alias_symbol\thgnc_prev_symbol\nPCSK9\t1\t55039445\t55064852\t+\tprotein_coding\t0\tPCSK9\tproprotein convertase subtilisin/kexin type 9\tNARC-1|FH3\tHCHOLA3\nUSP24\t1\t55066359\t55215753\t-\tprotein_coding\t16359\tUSP24\tubiquitin specific peptidase 24\tKIAA1057\tNone\n...",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "gene_name": {"type": "string"},
                                "chrom": {"type": "integer"},
                                "gene_start": {"type": "integer"},
                                "gene_end": {"type": "integer"},
                                "gene_strand": {"type": "string"},
                                "gene_type": {"type": "string"},
                                "distance": {"type": "integer"},
                                "hgnc_symbol": {"type": "string"},
                                "hgnc_name": {"type": "string"},
                                "hgnc_alias_symbol": {"type": "string"},
                                "hgnc_prev_symbol": {"type": ["string", "null"]},
                            },
                        },
                    },
                    "example": [
                        {
                            "gene_name": "PCSK9",
                            "chrom": 1,
                            "gene_start": 55039445,
                            "gene_end": 55064852,
                            "gene_strand": "+",
                            "gene_type": "protein_coding",
                            "distance": 0,
                            "hgnc_symbol": "PCSK9",
                            "hgnc_name": "proprotein convertase subtilisin/kexin type 9",
                            "hgnc_alias_symbol": "NARC-1|FH3",
                            "hgnc_prev_symbol": "HCHOLA3",
                        },
                        {
                            "gene_name": "USP24",
                            "chrom": 1,
                            "gene_start": 55066359,
                            "gene_end": 55215753,
                            "gene_strand": "-",
                            "gene_type": "protein_coding",
                            "distance": 16359,
                            "hgnc_symbol": "USP24",
                            "hgnc_name": "ubiquitin specific peptidase 24",
                            "hgnc_alias_symbol": "KIAA1057",
                            "hgnc_prev_symbol": "NA",
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "No genes found"},
        422: {"description": "Invalid variant"},
        500: {"description": "Internal server error"},
    },
    include_in_schema=True,
)
async def nearest_genes(
    variant: str = Path(
        ..., description="Variant (chr-pos-ref-alt)", examples=["7-5397122-C-T"]
    ),
    gene_type: Literal["protein_coding", "all"] = Query(
        default="protein_coding", description="Type of genes to return"
    ),
    n: int = Query(
        default=3,
        description="Maximum number of genes to return (default 3)",
        ge=1,
        le=20,
    ),
    max_distance: int = Query(
        default=1000000,
        description="Maximum distance from variant position to consider (default 1 million base pairs)",
        ge=0,
        le=10000000,
    ),
    gencode_version: str = Query(default=None, description="Gencode version to use"),
    return_hgnc_symbol_if_only_ensg: bool = Query(
        default=False,
        description="If true, returns HGNC symbol if for a gene gencode has only ENSG id and HGNC symbol is available",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    try:
        var = Variant(variant)
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))
    genes = gene_name_and_position_mapping.get_nearest_genes(
        var.chr,
        var.pos,
        n=n,
        gene_type=gene_type,
        max_distance=max_distance,
        gencode_version=gencode_version,
        return_hgnc_symbol_if_only_ensg=return_hgnc_symbol_if_only_ensg,
    )
    if format == "tsv":
        if not genes:
            raise HTTPException(
                status_code=404,
                detail=f"No genes found within {max_distance} base pairs of {variant}",
            )
        else:
            header = "\t".join(genes[0].keys())
            rows = "\n".join("\t".join(str(v) for v in gene.values()) for gene in genes)
            tsv = f"{header}\n{rows}\n"
        return PlainTextResponse(tsv, media_type="text/tab-separated-values")
    else:
        return genes


class NearestGenesRequest(BaseModel):
    variants: str


@router.post(
    "/nearest_genes",
    summary="Get nearest genes to multiple variants",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "gene_name\tchrom\tgene_start\tgene_end\tgene_strand\tgene_type\tdistance\thgnc_symbol\thgnc_name\thgnc_alias_symbol\thgnc_prev_symbol\tvariant\nPCSK9\t1\t55039445\t55064852\t+\tprotein_coding\t0\tPCSK9\tproprotein convertase subtilisin/kexin type 9\tNARC-1|FH3\tHCHOLA3\t1-55050000-C-T\n...",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "gene_name": {"type": "string"},
                                "chrom": {"type": "integer"},
                                "gene_start": {"type": "integer"},
                                "gene_end": {"type": "integer"},
                                "gene_strand": {"type": "string"},
                                "gene_type": {"type": "string"},
                                "distance": {"type": "integer"},
                                "hgnc_symbol": {"type": "string"},
                                "hgnc_name": {"type": "string"},
                                "hgnc_alias_symbol": {"type": "string"},
                                "hgnc_prev_symbol": {"type": ["string", "null"]},
                                "variant": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "gene_name": "PCSK9",
                            "chrom": 1,
                            "gene_start": 55039445,
                            "gene_end": 55064852,
                            "gene_strand": "+",
                            "gene_type": "protein_coding",
                            "distance": 0,
                            "hgnc_symbol": "PCSK9",
                            "hgnc_name": "proprotein convertase subtilisin/kexin type 9",
                            "hgnc_alias_symbol": "NARC-1|FH3",
                            "hgnc_prev_symbol": "HCHOLA3",
                            "variant": "1-55050000-C-T",
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        422: {"description": "Invalid variant"},
        500: {"description": "Internal server error"},
    },
)
async def nearest_genes_post(
    body: NearestGenesRequest,
    gene_type: Literal["protein_coding", "all"] = Query(
        default="protein_coding", description="Type of genes to return"
    ),
    n: int = Query(
        default=3,
        description="Maximum number of genes to return per variant (default 3)",
        ge=1,
        le=20,
    ),
    max_distance: int = Query(
        default=1000000,
        description="Maximum distance from variant position to consider (default 1 million base pairs)",
        ge=0,
        le=10000000,
    ),
    gencode_version: str = Query(default=None, description="Gencode version to use"),
    return_hgnc_symbol_if_only_ensg: bool = Query(
        default=False,
        description="If true, returns HGNC symbol if for a gene gencode has only ENSG id and HGNC symbol is available",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    """
    Get nearest genes to multiple variants.
    Accepts a newline-separated list of variants in the request body.
    """
    variant_strings = [v.strip() for v in body.variants.strip().split("\n") if v.strip()]
    if not variant_strings:
        raise HTTPException(status_code=422, detail="No variants provided")

    variants = []
    for vs in variant_strings:
        try:
            variants.append(Variant(vs))
        except ParseException as e:
            raise HTTPException(status_code=422, detail=f"Invalid variant '{vs}': {e}")

    all_genes = []
    for var in variants:
        genes = gene_name_and_position_mapping.get_nearest_genes(
            var.chr,
            var.pos,
            n=n,
            gene_type=gene_type,
            max_distance=max_distance,
            gencode_version=gencode_version,
            return_hgnc_symbol_if_only_ensg=return_hgnc_symbol_if_only_ensg,
        )
        for gene in genes:
            gene["variant"] = var.varid
        all_genes.extend(genes)

    if format == "tsv":
        if not all_genes:
            raise HTTPException(
                status_code=404,
                detail=f"No genes found within {max_distance} base pairs of any variant",
            )
        header = "\t".join(all_genes[0].keys())
        rows = "\n".join("\t".join(str(v) for v in gene.values()) for gene in all_genes)
        tsv = f"{header}\n{rows}\n"
        return PlainTextResponse(tsv, media_type="text/tab-separated-values")
    else:
        return all_genes


# TODO implement by gencode version
# @router.get(
#     "/gene_model/{chr}/{start}/{end}",
#     include_in_schema=False,
#     responses={
#         200: {
#             "description": "Successful response",
#             "content": {"application/octet-stream": {"schema": {"type": "string"}}},
#         },
#         401: {"description": "Not authenticated"},
#         404: {"description": "Gene model not found"},
#         500: {"description": "Internal server error"},
#     },
# )
# async def gene_model(
#     chr: str,
#     start: int,
#     end: int,
#     user: str = Depends(auth_required),
#     request_util: RequestUtil = Depends(get_request_util),
# ) -> Response:
#     return request_util.stream_tabix_response( # TODO replace stream_tabix_response with something else
#         config.genes["model_file"], f"{chr}:{start}-{end}"
#     )


# TODO implement by gencode version
# @router.get(
#     "/gene_model_by_gene/{gene}/{padding}",
#     include_in_schema=False,
#     responses={
#         200: {
#             "description": "Successful response",
#             "content": {"application/octet-stream": {"schema": {"type": "string"}}},
#         },
#         401: {"description": "Not authenticated"},
#         404: {"description": "Gene not found"},
#         422: {"description": "Invalid padding parameter"},
#         500: {"description": "Internal server error"},
#     },
# )
# async def gene_model_by_gene(
#     gene: str,
#     padding: int,
#     user: str = Depends(auth_required),
#     request_util: RequestUtil = Depends(get_request_util),
# ) -> Response:
#     if padding < 0:
#         raise HTTPException(status_code=422, detail="padding must be non-negative")
#     try:
#         chr, start, end = request_util.get_gene_range(gene.upper()) # TODO replace get_gene_range with get_coordinates_by_gene_name
#     except GeneNotFoundException as e:
#         raise HTTPException(status_code=404, detail=str(e))
#     return request_util.stream_tabix_response(
#         config.genes["model_file"], f"{chr}:{start-padding}-{end+padding}"
#     )


