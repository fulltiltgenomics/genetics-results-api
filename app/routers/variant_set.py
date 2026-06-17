"""
Router for named curated variant sets (e.g. the annotation tool's example/priority lists).

GET /variant_sets         -> list configured set names
GET /variant_sets/{name}  -> { "name", "variants": ["chr:pos:ref:alt", ...] }
"""

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import JSONResponse

from app.core.exceptions import NotFoundException
from app.dependencies import get_variant_set_service, is_public
from app.services.variant_set_service import VariantSetService

router = APIRouter()


@router.get("/variant_sets")
@is_public
async def list_variant_sets(
    service: VariantSetService = Depends(get_variant_set_service),
):
    """List the names of all configured curated variant sets."""
    return JSONResponse(service.list_names())


@router.get("/variant_sets/{name}")
@is_public
async def get_variant_set(
    name: str = Path(..., description="Variant set name", examples=["FinnGen_enriched_202505"]),
    service: VariantSetService = Depends(get_variant_set_service),
):
    """Expand a named curated variant set into its canonical variant ids."""
    try:
        variants = service.get_variants(name)
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    return JSONResponse({"name": name, "variants": variants})
