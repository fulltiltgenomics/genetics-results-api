"""LD: the suite's own panels (app/services/ld_panel_service.py) and the upstream proxy
(app/services/ld_service.py), behind one route family. The panel name decides which serves:
a name the active profile configures is read from our matrices, any other name is relayed
to the FinnGen LD server, so a caller changes nothing but `panel` to move between them."""

import logging
import re
import time

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request

from app.config import ld as ld_config
from app.core.exceptions import ParseException
from app.core.responses import TimedJSONResponse
from app.core.variant import Variant
from app.dependencies import get_ld_panel_service, get_ld_service
from app.services.ld_panel_service import LDPanelService
from app.services.ld_service import (
    UPSTREAM_FIELDS,
    LDPairTooFar,
    LDPanelUnknown,
    LDService,
    LDUpstreamError,
    LDVariantNotInPanel,
)

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

_FIELDS_DOC = (
    "Comma-separated subset of r, r2, d_prime to return on each entry; default every "
    "statistic the panel carries. Asking for one the panel lacks is a 422 naming what it has."
)

_ENTRY_EXAMPLE = {
    "variation1": "12:49048170:A:C",
    "variation2": "12:49048999:G:T",
    "r": -0.9539,
    "r2": 0.91,
}


def _check_variant(variant: str) -> None:
    if not VARIANT_PATTERN.match(variant):
        raise HTTPException(
            status_code=422,
            detail=f"Invalid variant '{variant}'. Expected chr:pos:ref:alt, e.g. 12:49048170:A:C",
        )


def _check_panel(panel: str) -> None:
    if not PANEL_PATTERN.match(panel):
        raise HTTPException(
            status_code=422,
            detail=f"Invalid panel '{panel}'. Expected a name of letters, digits, '.', '_' or '-'",
        )


def _check_window(window: int) -> None:
    if window < ld_config.ld_min_window or window > ld_config.ld_max_window:
        # refused rather than clamped: a silently narrowed window returns fewer variants and
        # reads as a sparse locus rather than as a limit
        raise HTTPException(
            status_code=422,
            detail=(
                f"window {window} is outside {ld_config.ld_min_window}.."
                f"{ld_config.ld_max_window}, the bounds the LD server accepts."
            ),
        )


def _parse_fields(fields: str | None, available: tuple[str, ...], panel: str) -> list[str]:
    """The statistics to put on each entry: the caller's subset, or everything the panel has.

    Validated against what the panel carries, not just against the known names, so a script
    asking the upstream for `r` learns on the first call that it has to name a panel that has
    it, instead of reading a missing key as a null correlation.
    """
    if fields is None:
        return list(available)
    wanted: list[str] = []
    for token in fields.split(","):
        name = token.strip()
        if not name:
            continue
        if name not in ld_config.LD_STATISTICS:
            raise HTTPException(
                status_code=422,
                detail=f"Unknown LD field '{name}'. Fields are: {', '.join(ld_config.LD_STATISTICS)}",
            )
        if name not in wanted:
            wanted.append(name)
    if not wanted:
        raise HTTPException(
            status_code=422,
            detail=f"fields must name at least one of {', '.join(ld_config.LD_STATISTICS)}",
        )
    missing = [name for name in wanted if name not in available]
    if missing:
        raise HTTPException(
            status_code=422,
            detail=(
                f"LD panel {panel} does not carry {', '.join(missing)}; it carries "
                f"{', '.join(available)}"
            ),
        )
    return wanted


def _trim(entries: list[dict], fields: list[str]) -> list[dict]:
    keep = ("variation1", "variation2", *fields)
    return [{k: e.get(k) for k in keep} for e in entries]


def _source(
    panel: str, panels: LDPanelService
) -> tuple[bool, tuple[str, ...], dict | None]:
    """(served locally?, statistics available, local panel config or None)."""
    if panels.has_panel(panel):
        cfg = panels.panel_config(panel)
        return True, tuple(cfg["fields"]), cfg
    return False, UPSTREAM_FIELDS, None


def _raise_for(exc: Exception) -> None:
    if isinstance(exc, ParseException):
        raise HTTPException(status_code=422, detail=str(exc))
    if isinstance(exc, LDVariantNotInPanel):
        # an answer about the variant, not a failure: 404 so a caller stops retrying and can
        # say why there is no LD rather than report an outage
        raise HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, LDPanelUnknown):
        # the caller's name was wrong and the upstream said which ones are right: 422 with
        # that list, the same shape as the window refusal, so the next call can be correct
        raise HTTPException(status_code=422, detail=str(exc))
    if isinstance(exc, LDPairTooFar):
        raise HTTPException(status_code=422, detail=str(exc))
    if isinstance(exc, LDUpstreamError):
        # 502 and not 4xx: nothing the caller sent is wrong, and a script that reads this as
        # its own fault will rewrite a correct request instead of backing off
        raise HTTPException(status_code=502, detail=str(exc))
    raise exc


@router.get(
    "/ld/panels",
    summary="LD panels this API serves itself, and their semantics",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "application/json": {
                    "example": {
                        "panels": [
                            {
                                "panel": "finngen_r12",
                                "fields": ["r", "r2"],
                                "r2_floor": 0.01,
                                "max_distance": 3000000,
                                "n_samples": 520210,
                                "description": "In-sample LD of the FinnGen R12 release ...",
                            }
                        ],
                        "default_panel": "sisu42",
                        "upstream_fields": ["r2", "d_prime"],
                    }
                }
            },
        },
        401: {"description": "Not authenticated"},
    },
)
async def ld_panels(
    request: Request, panels: LDPanelService = Depends(get_ld_panel_service)
) -> TimedJSONResponse:
    """The panels read from this API's own matrices, with the floor and cap each was computed
    with. Any other panel name is relayed to the FinnGen LD server, whose own panel list is
    learnt from its refusal of an unknown name; `default_panel` is where a request with no
    `panel` goes."""
    start_time = time.time()
    return TimedJSONResponse(
        {
            "panels": [
                {
                    "panel": cfg["panel"],
                    "fields": list(cfg["fields"]),
                    "r2_floor": cfg["r2_floor"],
                    "max_distance": cfg["max_distance"],
                    "n_samples": cfg.get("n_samples"),
                    "description": cfg.get("description", ""),
                }
                for name in panels.panel_names()
                for cfg in [panels.panel_config(name)]
            ],
            "default_panel": ld_config.ld_default_panel,
            "upstream_fields": list(UPSTREAM_FIELDS),
        },
        request.url,
        start_time,
    )


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
                        "panel": "finngen_r12",
                        "fields": ["r", "r2"],
                        "r2_floor": 0.01,
                        "max_distance": 3000000,
                        "ld": [_ENTRY_EXAMPLE],
                    }
                }
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "The panel does not carry the query variant"},
        422: {"description": "Invalid variant, window, threshold, panel or fields"},
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
        description="Total span in base pairs, centred on the query variant",
    ),
    r2_threshold: float = Query(
        default=0.6, description="Minimum r² for a variant to be returned", ge=0.0, le=1.0
    ),
    panel: str = Query(
        default=ld_config.ld_default_panel,
        description="Panel name: one of `GET /ld/panels` to read this API's own matrices, "
        "anything else goes to the FinnGen LD server",
    ),
    fields: str | None = Query(default=None, description=_FIELDS_DOC),
    ld_service: LDService = Depends(get_ld_service),
    panels: LDPanelService = Depends(get_ld_panel_service),
) -> TimedJSONResponse:
    """Every variant in LD with the query variant, from this API's own matrices or the
    FinnGen LD server depending on `panel`.

    Entries carry the query as `variation1` and the partner as `variation2`, partner position
    ascending, with the statistics in `fields`. An empty `ld` list means nothing reached the
    threshold, which is an answer rather than an error. A local panel's `r2_floor` and
    `max_distance` say what its files hold at all: a pair below the floor or beyond the cap
    is absent whatever the threshold. They are null for the upstream, which does not state
    its own.
    """
    start_time = time.time()

    _check_variant(variant)
    _check_panel(panel)
    _check_window(window)
    local, available, cfg = _source(panel, panels)
    wanted = _parse_fields(fields, available, panel)

    try:
        if local:
            entries = await panels.variants_in_ld(
                variant, window=window, r2_threshold=r2_threshold, panel=panel
            )
        else:
            entries = await ld_service.variants_in_ld(
                variant, window=window, r2_threshold=r2_threshold, panel=panel
            )
    except Exception as exc:
        _raise_for(exc)

    return TimedJSONResponse(
        {
            "variant": variant,
            "window": window,
            "r2_threshold": r2_threshold,
            "panel": panel,
            "fields": wanted,
            "r2_floor": cfg["r2_floor"] if cfg else None,
            "max_distance": cfg["max_distance"] if cfg else None,
            "ld": _trim(entries, wanted),
        },
        request.url,
        start_time,
    )


@router.get(
    "/ld/{variant}/{other}",
    summary="LD between two variants",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "application/json": {
                    "example": {
                        "variant": "12:49048170:A:C",
                        "other": "12:49048999:G:T",
                        "r2_threshold": 0.0,
                        "panel": "finngen_r12",
                        "fields": ["r", "r2"],
                        "r2_floor": 0.01,
                        "max_distance": 3000000,
                        "ld": [_ENTRY_EXAMPLE],
                    }
                }
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "The panel does not carry one of the variants"},
        422: {
            "description": "Invalid variant, threshold, panel or fields; variants on different "
            "chromosomes; or further apart than the panel computes LD for"
        },
        502: {"description": "The upstream LD server failed or could not be reached"},
    },
)
async def ld_between(
    request: Request,
    variant: str = Path(..., description="First variant as chr:pos:ref:alt"),
    other: str = Path(..., description="Second variant as chr:pos:ref:alt"),
    r2_threshold: float = Query(
        default=0.0, description="Minimum r² for the pair to be returned", ge=0.0, le=1.0
    ),
    panel: str = Query(
        default=ld_config.ld_default_panel,
        description="Panel name: one of `GET /ld/panels` to read this API's own matrices, "
        "anything else goes to the FinnGen LD server",
    ),
    fields: str | None = Query(default=None, description=_FIELDS_DOC),
    ld_service: LDService = Depends(get_ld_service),
    panels: LDPanelService = Depends(get_ld_panel_service),
) -> TimedJSONResponse:
    """LD between two variants on one chromosome.

    `ld` holds one entry, with `variation1` the first variant and `variation2` the second as
    the caller spelled them, or nothing. On a local panel an empty list means the pair is
    below the panel's `r2_floor` (or the threshold): both variants were found, so the pair
    is independent rather than unknown. A pair further apart than the panel's `max_distance`
    is refused with 422, since the files do not hold it. On the upstream the second variant
    cannot be checked for membership, so an empty list there means below the threshold or
    absent. A variant against itself is 1 by definition and answered as such.
    """
    start_time = time.time()

    _check_variant(variant)
    _check_variant(other)
    _check_panel(panel)
    local, available, cfg = _source(panel, panels)
    wanted = _parse_fields(fields, available, panel)

    try:
        v, o = Variant(variant), Variant(other)
    except ParseException as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    if v.chr != o.chr:
        raise HTTPException(
            status_code=422,
            detail=(
                f"LD is computed within one chromosome; {variant} and {other} are on "
                "different chromosomes (independent by definition)"
            ),
        )
    if cfg and abs(v.pos - o.pos) > cfg["max_distance"]:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{variant} and {other} are {abs(v.pos - o.pos):,} bp apart; LD panel {panel} "
                f"holds pairs up to {cfg['max_distance']:,} bp apart"
            ),
        )

    try:
        if local:
            entries = await panels.pair(
                variant, other, r2_threshold=r2_threshold, panel=panel
            )
        else:
            entries = await ld_service.pair(
                variant, other, r2_threshold=r2_threshold, panel=panel
            )
    except Exception as exc:
        _raise_for(exc)

    return TimedJSONResponse(
        {
            "variant": variant,
            "other": other,
            "r2_threshold": r2_threshold,
            "panel": panel,
            "fields": wanted,
            "r2_floor": cfg["r2_floor"] if cfg else None,
            "max_distance": cfg["max_distance"] if cfg else None,
            "ld": _trim(entries, wanted),
        },
        request.url,
        start_time,
    )
