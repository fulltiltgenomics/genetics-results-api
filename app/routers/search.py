import logging
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from app.dependencies import get_search_index
from app.services.search_service import SearchIndex

logger = logging.getLogger(__name__)

router = APIRouter()


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
                                "code": {"type": "string"},
                                "name": {"type": "string"},
                                "resource": {"type": "string"},
                                "sample_size": {"type": "integer"},
                                "symbol": {"type": "string"},
                                "aliases": {"type": "array"},
                                "ensembl_id": {"type": "string"},
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
                            "aliases": ["NARC-1", "FH3"],
                            "ensembl_id": "ENSG00000169174",
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
                            "sample_size": 156438,
                            "match_type": "prefix",
                            "match_score": 95,
                            "rank_score": 965,
                            "matched_key": "I9_HYPERLIPID",
                        },
                    ],
                },
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "type\tsymbol\tname\taliases\tensembl_id\tmatch_type\tmatch_score\trank_score\tmatched_key\ngene\tPCSK9\tproprotein convertase subtilisin/kexin type 9\tNARC-1|FH3\tENSG00000169174\texact\t100\t1200\tPCSK9\nphenotype\tI9_HYPERLIPID\tHyperlipidaemia\t\t\tprefix\t95\t965\tI9_HYPERLIPID\n...",
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
    search_index: SearchIndex = Depends(get_search_index),
):
    """
    Search and autocomplete for phenotypes and genes with fuzzy matching.

    Supports comma-separated queries (e.g., 'SLC26A3,CLCA,PCSK9') to search
    for multiple terms in a single request.

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
            term_results = search_index.search(query=term, limit=limit, types=type_list)
            for result in term_results:
                # use code (phenotype) or symbol (gene) as unique identifier
                result_id = (
                    result["type"],
                    result.get("code") or result.get("symbol"),
                )
                if result_id not in seen_ids:
                    seen_ids.add(result_id)
                    results.append(result)

        # format response
        if format == "json":
            return JSONResponse(results)
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
                header = "type\tsymbol\tname\taliases\tensembl_id\tmatch_type\tmatch_score\trank_score\tmatched_key"
                rows = []
                for r in results:
                    aliases_str = "|".join(r.get("aliases", []))
                    row = (
                        f"{r['type']}\t{r['symbol']}\t{r.get('name', '')}\t"
                        f"{aliases_str}\t{r.get('ensembl_id', '')}\t"
                        f"{r['match_type']}\t{r['match_score']}\t{r['rank_score']}\t{r['matched_key']}"
                    )
                    rows.append(row)
            else:  # phenotypes
                header = "type\tcode\tname\tresource\tsample_size\tmatch_type\tmatch_score\trank_score\tmatched_key"
                rows = []
                for r in results:
                    row = (
                        f"{r['type']}\t{r.get('code', '')}\t{r.get('name', '')}\t"
                        f"{r.get('resource', '')}\t{r.get('sample_size', 0)}\t"
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
