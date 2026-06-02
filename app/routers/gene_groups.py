import logging

from fastapi import APIRouter, Depends, HTTPException, Query

from app.dependencies import get_gene_group_service, get_search_index
from app.services.gene_group_service import GeneGroupService
from app.services.search_service import SearchIndex

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/gene_group/members",
    summary="Get member genes of an HGNC gene group",
    responses={
        200: {"description": "Successful response"},
        400: {"description": "Exactly one of group_id / group_name is required"},
        401: {"description": "Not authenticated"},
        404: {"description": "Unknown gene group"},
        500: {"description": "Internal server error"},
    },
)
async def gene_group_members(
    group_id: int | None = Query(
        default=None, description="HGNC gene-group id (leaf, intermediate, or root)"
    ),
    group_name: str | None = Query(
        default=None, description="HGNC gene-group name (case-insensitive)"
    ),
    gene_group_service: GeneGroupService = Depends(get_gene_group_service),
    search_index: SearchIndex = Depends(get_search_index),
) -> dict:
    """
    Return all member genes of an HGNC gene group, resolved by lineage:
    any group id (leaf, intermediate, or root) returns every gene whose
    full lineage contains it, i.e. all descendant members.

    Supply exactly one of `group_id` or `group_name` (case-insensitive).

    Note: olfactory receptors are included and dominate large families
    (e.g. the G protein-coupled receptor group), so member counts for
    high-level groups can be very large.
    """
    if (group_id is None) == (group_name is None):
        raise HTTPException(
            status_code=400,
            detail="Provide exactly one of group_id or group_name",
        )

    # data not loaded yet (e.g. HGNC group files not uploaded to GCS): degrade
    # gracefully with an empty member list rather than failing, mirroring the
    # resilient loading in GeneGroupService / SearchIndex.
    if not gene_group_service.is_loaded():
        logger.warning(
            "gene_group/members requested but gene-group data is not loaded; "
            "returning empty members"
        )
        return {
            "group_id": group_id,
            "group_name": group_name,
            "count": 0,
            "members": [],
        }

    if group_name is not None:
        resolved_id = gene_group_service.resolve_group_id(group_name)
        if resolved_id is None:
            raise HTTPException(
                status_code=404, detail=f"Unknown gene group: {group_name}"
            )
        group_id = resolved_id

    resolved_name = gene_group_service.group_name(group_id)
    if resolved_name is None:
        raise HTTPException(status_code=404, detail=f"Unknown gene group: {group_id}")

    hgnc_ids = gene_group_service.members_of_group(group_id=group_id)

    members = []
    for hgnc_id in sorted(hgnc_ids):
        gene = search_index.get_gene_by_hgnc_id(hgnc_id)
        if gene is None:
            # member without a search/coords record is still listed with nulls
            members.append(
                {
                    "hgnc_id": hgnc_id,
                    "symbol": None,
                    "ensembl_id": None,
                    "chr": None,
                    "gene_start": None,
                    "gene_end": None,
                }
            )
            continue
        members.append(
            {
                "hgnc_id": hgnc_id,
                "symbol": gene.get("symbol"),
                "ensembl_id": gene.get("ensembl_id") or None,
                "chr": gene.get("chrom"),
                "gene_start": gene.get("gene_start"),
                "gene_end": gene.get("gene_end"),
            }
        )

    members.sort(key=lambda m: (m["symbol"] is None, m["symbol"] or m["hgnc_id"]))

    return {
        "group_id": group_id,
        "group_name": resolved_name,
        "count": len(members),
        "members": members,
    }


@router.get(
    "/gene/normalize",
    summary="Normalize gene symbols to current approved HGNC symbols",
    responses={
        200: {"description": "Successful response"},
        400: {"description": "symbols parameter is required"},
        401: {"description": "Not authenticated"},
        500: {"description": "Internal server error"},
    },
)
async def normalize_gene_symbols(
    symbols: str = Query(
        ...,
        description="Comma-separated gene symbols, aliases, or previous symbols",
    ),
    search_index: SearchIndex = Depends(get_search_index),
) -> dict:
    """
    Resolve each input gene symbol to its current approved HGNC symbol via
    EXACT case-insensitive matching against the in-memory HGNC index
    (approved symbol, alias, or previous symbol). No fuzzy matching is used,
    so the result never contains ranked near-miss false positives.

    `matched_on` is 'approved' when the input is already an approved symbol,
    or 'alias_or_previous' when matched through an alias / previous symbol
    (the HGNC load merges those two lists without keeping their origin).
    Inputs with no exact match are returned in `unresolved`.
    """
    inputs = [s.strip() for s in symbols.split(",")]
    inputs = [s for s in inputs if s]

    mappings = []
    unresolved = []
    for input_symbol in inputs:
        resolved = search_index.normalize_symbol(input_symbol)
        if resolved is None:
            unresolved.append(input_symbol)
            continue
        approved, matched_on = resolved
        mappings.append(
            {
                "input": input_symbol,
                "approved": approved,
                "matched_on": matched_on,
            }
        )

    return {"mappings": mappings, "unresolved": unresolved}
