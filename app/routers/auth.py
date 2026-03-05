"""Authentication status endpoint."""

import logging
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from app.core.auth import get_authenticated_user
from app.dependencies import is_public

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/auth", include_in_schema=False)
@is_public
async def auth(request: Request):
    """Return current authentication status."""
    user = get_authenticated_user(request)
    return JSONResponse({
        "authenticated": user is not None,
        "user": user,
    })
