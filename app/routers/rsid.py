"""
Router for rsID to variant conversion.
"""

import re
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from app.dependencies import get_rsid_db, is_public

RSID_PATTERN = re.compile(r"^rs\d+$", re.IGNORECASE)

# Applied to GET and POST alike and to every caller, with no sandbox special case — the
# uniformity is the point. This route is `@is_public`, so it is relaxed by `app.core.limits`;
# with nothing bounding the id count, a script that *omitted* its sandbox token got an
# unbounded response where the same script presenting the token got 16 MiB, i.e. the weaker
# credential bought the looser limit. `k8s/network-policies/sandbox-policy.yaml` lets the
# sandbox reach results-api:4000 directly, so auth-gateway's client_max_body_size never sees
# the POST body either.
#
# 5000 comes from what the GET already tolerates: uvicorn's h11 caps the request line plus
# headers at 16 KiB, and the shortest possible id costs 4 bytes in the query string ("rs1,"),
# so no GET that works today can carry more than 4096 ids — 5000 regresses nothing that
# currently succeeds. It is a generous bound on the response too (a few hundred KB, well under
# the 16 MiB sandbox cap), and no bulk POST caller exists today.
MAX_RSIDS = 5000
# an id and its separator cost at most 32 bytes, so this bounds the body the pod materializes
# without bounding anything a legitimate caller sends
MAX_RSID_BODY_BYTES = MAX_RSIDS * 32

router = APIRouter()


def parse_and_validate_rsids(rsids_input: str) -> list[str]:
    """Parse comma-separated rsids, and validate their count and format.

    Args:
        rsids_input: Comma-separated string of rsids

    Returns:
        List of validated rsids (original case preserved)

    Raises:
        HTTPException: If input is empty, over `MAX_RSIDS`, or any rsid is invalid
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

    # checked before the format scan, so an oversized request is not walked and does not
    # produce an error message listing every id in it
    if len(rsids) > MAX_RSIDS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Too many rsids: {len(rsids)} requested, at most {MAX_RSIDS} per request. "
                "Split the list across several requests."
            ),
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
        examples=["rs1234567,rs7654321"],
    ),
    rsid_db=Depends(get_rsid_db),
):
    """Convert rsids to variants.

    Returns a list of objects containing the rsid and its corresponding variants.
    If an rsid is not found, it is included with an empty variants array.
    """
    validated_rsids = parse_and_validate_rsids(rsids)
    variants_map = await rsid_db.get_variants_by_rsids(validated_rsids)

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
    # read incrementally rather than with request.body(): the count check below can only run
    # once the whole body is in memory, which is too late if the body itself is the payload
    body = bytearray()
    async for chunk in request.stream():
        body += chunk
        if len(body) > MAX_RSID_BODY_BYTES:
            raise HTTPException(
                status_code=413,
                detail=f"Request body too large: at most {MAX_RSID_BODY_BYTES} bytes.",
            )
    rsids_input = body.decode("utf-8")

    validated_rsids = parse_and_validate_rsids(rsids_input)
    variants_map = await rsid_db.get_variants_by_rsids(validated_rsids)

    seen = set()
    result = []
    for rsid in validated_rsids:
        key = rsid.lower()
        if key not in seen:
            seen.add(key)
            result.append({"rsid": key, "variants": variants_map.get(key, [])})

    return JSONResponse(result)
