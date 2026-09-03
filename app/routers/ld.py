"""LD proxy. See app/services/ld_service.py for why this endpoint exists at all."""

import logging
import re
import time

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request

from app.config import ld as ld_config
from app.core.responses import TimedJSONResponse
from app.dependencies import get_ld_service
from app.services.ld_service import LDUpstreamError, LDService

logger = logging.getLogger(__name__)

router = APIRouter()

# chr:pos:ref:alt, the spelling every caller in the suite already uses. Validated here rather
# than left to the upstream because this value goes into an outbound query string: shape-checking
# it is what keeps an arbitrary caller-supplied string out of a request results-api makes.
VARIANT_PATTERN = re.compile(r"^(chr)?[0-9XYMTxymt]{1,5}:\d{1,12}:[ACGTNacgtn]{1,1000}:[ACGTNacgtn]{1,1000}$")

# Shape, not membership: the panel set belongs to the upstream and an enumeration here would
# go stale silently, so a name that does not exist is the upstream's 4xx to give. What this
# refuses is a value that is not a name at all.
PANEL_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")


@router.get(
    "/ld/{variant}",
    summary="Variants in LD with a query variant",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "application/json": {
                    "example": {
                        "variant": "12:49048170:A:C",
                        "window": 1500000,
                        "r2_threshold": 0.6,
                        "panel": "sisu42",
                        "ld": [
                            {
                                "variation1": "12:49048170:A:C",
                                "variation2": "12:49048999:G:T",
                                "r2": 0.91,
                                "d_prime": 0.98,
                            }
                        ],
                    }
                }
            },
        },
        401: {"description": "Not authenticated"},
        422: {"description": "Invalid variant, window, threshold or panel"},
        502: {"description": "The upstream LD server failed or could not be reached"},
    },
)
async def variants_in_ld(
    request: Request,
    variant: str = Path(
        ..., description="Query variant as chr:pos:ref:alt", examples=["12:49048170:A:C"]
    ),
    window: int = Query(
        default=ld_config.ld_default_window,
        description="Base pairs either side of the query variant",
    ),
    r2_threshold: float = Query(
        default=0.6, description="Minimum r² for a variant to be returned", ge=0.0, le=1.0
    ),
    panel: str = Query(default=ld_config.ld_default_panel, description="Reference panel name"),
    ld_service: LDService = Depends(get_ld_service),
) -> TimedJSONResponse:
    """Proxy the FinnGen LD server for callers that cannot reach it.

    The entries come back with the upstream's own field names and are not reinterpreted here;
    which of `variation1`/`variation2` is the query variant is the caller's to decide, as it
    always was. An empty `ld` list means the upstream had nothing above the threshold, which is
    an answer rather than an error.
    """
    start_time = time.time()

    if not VARIANT_PATTERN.match(variant):
        raise HTTPException(
            status_code=422,
            detail=f"Invalid variant '{variant}'. Expected chr:pos:ref:alt, e.g. 12:49048170:A:C",
        )
    if not PANEL_PATTERN.match(panel):
        raise HTTPException(
            status_code=422,
            detail=f"Invalid panel '{panel}'. Expected a name of letters, digits, '.', '_' or '-'",
        )
    if window < 1 or window > ld_config.ld_max_window:
        # refused rather than clamped: a silently narrowed window returns fewer variants and
        # reads as a sparse locus rather than as a limit
        raise HTTPException(
            status_code=422,
            detail=(
                f"window {window} is outside 1..{ld_config.ld_max_window}. Ask for a smaller "
                "region rather than a wider window."
            ),
        )

    try:
        entries = await ld_service.variants_in_ld(
            variant, window=window, r2_threshold=r2_threshold, panel=panel
        )
    except LDUpstreamError as exc:
        # 502 and not 4xx: nothing the caller sent is wrong, and a script that reads this as
        # its own fault will rewrite a correct request instead of backing off
        raise HTTPException(status_code=502, detail=str(exc))

    return TimedJSONResponse(
        {
            "variant": variant,
            "window": window,
            "r2_threshold": r2_threshold,
            "panel": panel,
            "ld": entries,
        },
        request.url,
        start_time,
    )
