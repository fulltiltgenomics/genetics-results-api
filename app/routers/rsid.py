"""
Router for rsID to variant conversion.
"""

import re
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from app.dependencies import get_rsid_db, is_public

RSID_PATTERN = re.compile(r"^rs\d+$", re.IGNORECASE)

router = APIRouter()


def parse_and_validate_rsids(rsids_input: str) -> list[str]:
    """Parse comma-separated rsids and validate format.

    Args:
        rsids_input: Comma-separated string of rsids

    Returns:
        List of validated rsids (original case preserved)

    Raises:
        HTTPException: If input is empty or any rsid is invalid
    """
    if not rsids_input or not rsids_input.strip():
        raise HTTPException(
            status_code=422,
            detail="rsids parameter is required and cannot be empty",
        )

    rsids = [r.strip() for r in rsids_input.split(",") if r.strip()]

    if not rsids:
        raise HTTPException(
            status_code=422,
            detail="rsids parameter is required and cannot be empty",
        )

    invalid = [r for r in rsids if not RSID_PATTERN.match(r)]
    if invalid:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid rsid format: {', '.join(invalid)}. rsids must start with 'rs' followed by digits.",
        )

    return rsids


@router.get("/rsid/variants")
@is_public
async def get_rsid_variants(
    rsids: str = Query(
        ...,
        description="Comma-separated list of rsids (e.g., rs1234567,rs7654321)",
        example="rs1234567,rs7654321",
    ),
    rsid_db=Depends(get_rsid_db),
):
    """Convert rsids to variants.

    Returns a list of objects containing the rsid and its corresponding variants.
    If an rsid is not found, it is included with an empty variants array.
    """
    validated_rsids = parse_and_validate_rsids(rsids)
    variants_map = rsid_db.get_variants_by_rsids(validated_rsids)

    seen = set()
    result = []
    for rsid in validated_rsids:
        key = rsid.lower()
        if key not in seen:
            seen.add(key)
            result.append({"rsid": key, "variants": variants_map.get(key, [])})

    return JSONResponse(result)


@router.post("/rsid/variants")
@is_public
async def post_rsid_variants(
    request: Request,
    rsid_db=Depends(get_rsid_db),
):
    """Convert rsids to variants (POST version).

    Accepts comma-separated rsids in the request body.
    Returns a list of objects containing the rsid and its corresponding variants.
    If an rsid is not found, it is included with an empty variants array.
    """
    body = await request.body()
    rsids_input = body.decode("utf-8")

    validated_rsids = parse_and_validate_rsids(rsids_input)
    variants_map = rsid_db.get_variants_by_rsids(validated_rsids)

    seen = set()
    result = []
    for rsid in validated_rsids:
        key = rsid.lower()
        if key not in seen:
            seen.add(key)
            result.append({"rsid": key, "variants": variants_map.get(key, [])})

    return JSONResponse(result)
