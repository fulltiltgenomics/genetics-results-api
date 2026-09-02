import logging
from typing import Any, Literal
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel
from app.dependencies import get_gene_name_mapping
from app.core.variant import Variant
from app.core.exceptions import (
    ParseException,
)
from app.core.responses import verified_columns_header
from app.services.gene_name_and_position_mapping import (
    GENES_IN_REGION_COLUMNS,
    NEAREST_GENES_COLUMNS,
    GeneNameAndPositionMapping,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def _tsv_value(value: Any) -> str:
    """Render one cell of the genes TSV.

    The exon columns are lists, and a list has no TSV spelling, so they are comma-joined.
    The four lists are positional against each other, so an untranslated exon holds its
    place with NA rather than being dropped — cds_starts[i] belongs to exon i or to no
    exon at all. An empty field is a gene with no exon structure for the requested GENCODE
    version, which is every version but the newest.
    """
    if value is None:
        return "NA"
    if isinstance(value, list):
        return ",".join("NA" if v is None else str(v) for v in value)
    return str(value)


@router.get(
    "/genes_in_region/{chr}/{start}/{end}",
    include_in_schema=False,
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "gene_name\tchrom\tgene_start\tgene_end\tgene_strand\tgene_type\thgnc_symbol\thgnc_name\thgnc_alias_symbol\thgnc_prev_symbol\texon_starts\texon_ends\tcds_starts\tcds_ends\nPCSK9\t1\t55039445\t55064852\t+\tprotein_coding\tPCSK9\tproprotein convertase subtilisin/kexin type 9\tNARC-1|FH3\tHCHOLA3\t55039445,55043843\t55039763,55044063\tNA,55043843\tNA,55044063\n...",
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
                                "exon_starts": {
                                    "type": "array",
                                    "items": {"type": "integer"},
                                },
                                "exon_ends": {
                                    "type": "array",
                                    "items": {"type": "integer"},
                                },
                                "cds_starts": {
                                    "type": "array",
                                    "items": {"type": ["integer", "null"]},
                                },
                                "cds_ends": {
                                    "type": "array",
                                    "items": {"type": ["integer", "null"]},
                                },
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
                            "hgnc_symbol": "PCSK9",
                            "hgnc_name": "proprotein convertase subtilisin/kexin type 9",
                            "hgnc_alias_symbol": "NARC-1|FH3",
                            "hgnc_prev_symbol": "HCHOLA3",
                            # the four lists are positional: exon 1 here is entirely 5' UTR,
                            # so its coding bounds are null while exon 2 carries a CDS
                            "exon_starts": [55039445, 55043843],
                            "exon_ends": [55039763, 55044063],
                            "cds_starts": [None, 55043843],
                            "cds_ends": [None, 55044063],
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
                "\t".join(_tsv_value(v) for v in gene.values()) for gene in genes
            )
            tsv = f"{header}\n{rows}\n"
        return PlainTextResponse(tsv, media_type="text/tab-separated-values")
    else:
        # a region with no genes is an ordinary answer, not an error, and JSON says so with
        # `[]` — which carries no schema, so the columns go in the header (8a1)
        # jsonable_encoder, because returning a Response directly skips the encoding
        # FastAPI would otherwise do for a returned value: a dtype json.dumps cannot
        # serialize would become a 500 instead of a string
        return JSONResponse(
            jsonable_encoder(genes),
            headers=verified_columns_header(GENES_IN_REGION_COLUMNS, genes),
        )


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
        return JSONResponse(
            jsonable_encoder(genes),
            headers=verified_columns_header(NEAREST_GENES_COLUMNS, genes),
        )


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
        # deliberately NOT advertising columns like the GET route above: these rows carry an
        # extra `variant` key appended per row, so they are NEAREST_GENES_COLUMNS + one, and
        # no SDK function reaches this route (genetics-results-suite-8a1 covers the GET).
        # Advertising here means declaring that wider shape, not reusing the GET's list
        return all_genes




