import logging
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from app.core.responses import (
    ColumnDeclarationError,
    columns_header,
    verified_columns_header,
)
from app.dependencies import get_search_index
from app.services.search_service import RESULT_COLUMNS_BY_TYPE, SearchIndex

logger = logging.getLogger(__name__)

router = APIRouter()

# `types=` is plural on the wire, singular in a result row's `type`
_RESULT_TYPE_BY_QUERY_TYPE = {"phenotypes": "phenotype", "genes": "gene"}


def _verified_search_columns(
    results: list[dict], type_list: list[str] | None
) -> dict[str, str]:
    """Verify every row against its declared columns, and advertise them when they are one set.

    A JSON search result is a bare array, so an empty one tells a client nothing about the
    columns it would have had. The declaration lives next to the rows in
    services/search_service.py and is REFUSED here when a row contradicts it — a search
    result dict is assembled from a live index, so this response is the only place the
    declaration can be checked against reality (genetics-results-suite-8a1).

    Advertised only when the caller pinned a single type: a mixed phenotype+gene result has
    two different column sets and no honest single answer, so the header is omitted rather
    than guessed. Every row is still verified in that case.
    """
    for row in results:
        declared = RESULT_COLUMNS_BY_TYPE.get(row.get("type"))
        if declared is None:
            raise ColumnDeclarationError(
                f"search returned an undeclared result type {row.get('type')!r}"
            )
        verified_columns_header(declared, [row])
    if type_list and len(type_list) == 1:
        return columns_header(
            list(RESULT_COLUMNS_BY_TYPE[_RESULT_TYPE_BY_QUERY_TYPE[type_list[0]]])
        )
    return {}


@router.get(
    "/search",
    summary="Search and autocomplete for phenotypes and genes (supports comma-separated queries)",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": {"type": "string"},
                                "code": {"type": "string", "description": "Phenotype results only"},
                                "name": {"type": "string"},
                                "resource": {"type": "string", "description": "Phenotype results only"},
                                "data_type": {"type": "string", "description": "Phenotype results only; pass to summary_stats/{resource}/{data_type}"},
                                "sample_size": {"type": "integer", "description": "Phenotype results only"},
                                "n_cases": {"type": ["integer", "string"], "description": "Phenotype results only, int or 'NA'"},
                                "n_controls": {"type": ["integer", "string"], "description": "Phenotype results only, int or 'NA'"},
                                "has_summary_stats": {"type": "boolean", "description": "Phenotype results only; whether summary stats are available for (resource, data_type)"},
                                "has_credible_sets": {"type": "boolean", "description": "Phenotype results only; whether credible sets are available for (resource, data_type)"},
                                "symbol": {"type": "string", "description": "Gene results only"},
                                "aliases": {"type": "array", "description": "Gene results only"},
                                "ensembl_id": {"type": "string", "description": "Gene results only"},
                                "chrom": {"type": "integer", "description": "Gene results only"},
                                "gene_start": {"type": "integer", "description": "Gene results only"},
                                "gene_end": {"type": "integer", "description": "Gene results only"},
                                "search_strings": {"type": "array", "items": {"type": "string"}},
                                "match_type": {"type": "string"},
                                "match_score": {"type": "number"},
                                "rank_score": {"type": "number"},
                                "matched_key": {"type": "string"},
                            },
                        },
                    },
                    "example": [
                        {
                            "type": "gene",
                            "symbol": "PCSK9",
                            "name": "proprotein convertase subtilisin/kexin type 9",
                            "aliases": ["NARC-1", "FH3", "HCHOLA3"],
                            "ensembl_id": "ENSG00000169174",
                            "chrom": 1,
                            "gene_start": 55039447,
                            "gene_end": 55064852,
                            "search_strings": ["pcsk9", "proprotein convertase subtilisin/kexin type 9", "narc-1", "fh3", "hchola3", "ensg00000169174"],
                            "match_type": "exact",
                            "match_score": 100,
                            "rank_score": 1200,
                            "matched_key": "PCSK9",
                        },
                        {
                            "type": "phenotype",
                            "code": "I9_HYPERLIPID",
                            "name": "Hyperlipidaemia",
                            "resource": "finngen",
                            "data_type": "gwas",
                            "sample_size": 156438,
                            "n_cases": 56438,
                            "n_controls": 100000,
                            "has_summary_stats": True,
                            "has_credible_sets": True,
                            "search_strings": ["i9_hyperlipid", "hyperlipidaemia"],
                            "match_type": "prefix",
                            "match_score": 95,
                            "rank_score": 965,
                            "matched_key": "I9_HYPERLIPID",
                        },
                    ],
                },
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "type\tsymbol\tname\taliases\tensembl_id\tchrom\tgene_start\tgene_end\tmatch_type\tmatch_score\trank_score\tmatched_key\ngene\tPCSK9\tproprotein convertase subtilisin/kexin type 9\tNARC-1|FH3\tENSG00000169174\t1\t55039447\t55064852\texact\t100\t1200\tPCSK9\n\ntype\tcode\tname\tresource\tdata_type\tsample_size\tn_cases\tn_controls\thas_summary_stats\thas_credible_sets\tmatch_type\tmatch_score\trank_score\tmatched_key\nphenotype\tI9_HYPERLIPID\tHyperlipidaemia\tfinngen\tgwas\t156438\t56438\t100000\ttrue\ttrue\tprefix\t95\t965\tI9_HYPERLIPID\n...",
                },
            },
        },
        401: {"description": "Not authenticated"},
        422: {"description": "Invalid parameters (empty query, invalid types)"},
        500: {"description": "Internal server error"},
    },
)
async def search_autocomplete(
    q: str = Query(
        ...,
        description="Search query (comma-separated for multiple terms, e.g. 'SLC26A3,CLCA')",
        min_length=1,
    ),
    limit: int = Query(
        default=10, description="Maximum results per query term", ge=1, le=100
    ),
    types: str | None = Query(
        default=None,
        description="Comma-separated types to search: 'phenotypes', 'genes' (default: both)",
    ),
    format: Literal["json", "tsv"] = Query(
        default="json", description="Response format"
    ),
    gencode_version: int | None = Query(
        default=None,
        description="GENCODE version to use for gene coordinates (default: latest available)",
    ),
    has_summary_stats: bool = Query(
        default=False,
        description="If true, drop phenotype results that have no summary statistics available",
    ),
    has_credible_sets: bool = Query(
        default=False,
        description="If true, drop phenotype results that have no credible sets available",
    ),
    search_index: SearchIndex = Depends(get_search_index),
):
    """
    Search and autocomplete for phenotypes and genes with fuzzy matching.

    Supports comma-separated queries (e.g., 'SLC26A3,CLCA,PCSK9') to search
    for multiple terms in a single request.

    A phenotype may appear once per (resource, data_type): the same FinnGen
    phenocode exists across the finngen, finngen_ukbb and finngen_mvp_ukbb
    resources, and a code can have multiple result types (e.g. genebass exome and
    gene_based), so the same code can occur in more than one result row.

    Results are ranked by:
    1. Exact matches first
    2. For genes: official symbols > aliases
    3. For phenotypes: larger sample size
    4. Alphabetical within each tier

    Supports typo tolerance via fuzzy matching.
    """
    try:
        # parse types parameter
        type_list = None
        if types:
            type_list = [t.strip() for t in types.split(",")]
            valid_types = {"phenotypes", "genes"}
            invalid = set(type_list) - valid_types
            if invalid:
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid types: {invalid}. Valid types: {valid_types}",
                )

        # split query by comma and search for each term
        query_terms = [term.strip() for term in q.split(",") if term.strip()]
        if not query_terms:
            raise HTTPException(status_code=422, detail="Empty query")

        # collect results from all query terms, avoiding duplicates
        seen_ids = set()
        results = []
        for term in query_terms:
            term_results = search_index.search(query=term, limit=limit, types=type_list, gencode_version=gencode_version)
            for result in term_results:
                # use code (phenotype) or symbol (gene) as unique identifier;
                # include resource AND data_type so a phenotype shared across
                # resources (the same FinnGen phenocode exists in finngen,
                # finngen_ukbb, finngen_mvp_ukbb) or with multiple result types
                # (e.g. genebass exome + gene_based) survives as separate rows
                result_id = (
                    result["type"],
                    result.get("code") or result.get("symbol"),
                    result.get("resource"),
                    result.get("data_type"),
                )
                if result_id not in seen_ids:
                    seen_ids.add(result_id)
                    results.append(result)

        # drop phenotypes without summary stats / credible sets when requested; gene results
        # have neither flag and are always kept
        if has_summary_stats:
            results = [
                r
                for r in results
                if r["type"] != "phenotype" or r.get("has_summary_stats")
            ]
        if has_credible_sets:
            results = [
                r
                for r in results
                if r["type"] != "phenotype" or r.get("has_credible_sets")
            ]

        # format response
        if format == "json":
            return JSONResponse(
                results, headers=_verified_search_columns(results, type_list)
            )
        elif format == "tsv":
            # for TSV, require a type filter to avoid mixed columns
            if not type_list or len(type_list) > 1:
                raise HTTPException(
                    status_code=422,
                    detail="TSV format requires a single type filter (types=genes or types=phenotypes)",
                )
            # generate TSV (guaranteed to have single type due to check above)
            if not results:
                return PlainTextResponse(
                    "", media_type="text/tab-separated-values"
                )

            # determine format based on type filter
            if type_list[0] == "genes":
                header = "type\tsymbol\tname\taliases\tensembl_id\tchrom\tgene_start\tgene_end\tmatch_type\tmatch_score\trank_score\tmatched_key"
                rows = []
                for r in results:
                    aliases_str = "|".join(r.get("aliases", []))
                    row = (
                        f"{r['type']}\t{r['symbol']}\t{r.get('name', '')}\t"
                        f"{aliases_str}\t{r.get('ensembl_id', '')}\t"
                        f"{r.get('chrom') or ''}\t{r.get('gene_start') or ''}\t{r.get('gene_end') or ''}\t"
                        f"{r['match_type']}\t{r['match_score']}\t{r['rank_score']}\t{r['matched_key']}"
                    )
                    rows.append(row)
            else:  # phenotypes
                header = "type\tcode\tname\tresource\tdata_type\tsample_size\tn_cases\tn_controls\thas_summary_stats\thas_credible_sets\tmatch_type\tmatch_score\trank_score\tmatched_key"
                rows = []
                for r in results:
                    row = (
                        f"{r['type']}\t{r.get('code', '')}\t{r.get('name', '')}\t"
                        f"{r.get('resource', '')}\t{r.get('data_type', '')}\t{r.get('sample_size', 0)}\t"
                        f"{r.get('n_cases', 'NA')}\t{r.get('n_controls', 'NA')}\t"
                        f"{str(r.get('has_summary_stats', False)).lower()}\t"
                        f"{str(r.get('has_credible_sets', False)).lower()}\t"
                        f"{r['match_type']}\t{r['match_score']}\t{r['rank_score']}\t{r['matched_key']}"
                    )
                    rows.append(row)

            tsv = f"{header}\n" + "\n".join(rows) + "\n"
            return PlainTextResponse(tsv, media_type="text/tab-separated-values")

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error performing search: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
