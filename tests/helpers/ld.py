"""The LD router driven offline, with the upstream LD service replaced by a stub."""

from fastapi import FastAPI

from app.dependencies import get_ld_panel_service, get_ld_service
from app.routers import ld as ld_router
from helpers.asgi import AsgiResponse, call_asgi


class StubLDService:
    """Stands in for the upstream LD service and records what it was asked."""

    def __init__(self, entries=None, raises=None):
        self.entries = entries if entries is not None else []
        self.raises = raises
        self.calls = []

    async def variants_in_ld(self, variant, *, window, r2_threshold, panel):
        self.calls.append(("scan", variant, window, r2_threshold, panel))
        if self.raises:
            raise self.raises
        return self.entries

    async def pair(self, variant, other, *, r2_threshold, panel):
        self.calls.append(("pair", variant, other, r2_threshold, panel))
        return self.entries


def call_ld_router(path, query="", *, upstream, panel_service=None) -> AsgiResponse:
    """Drive the router's ASGI app for one GET; without `panel_service` the app's own is used."""
    api = FastAPI()
    api.include_router(ld_router.router, prefix="/api/v1")
    api.dependency_overrides[get_ld_service] = lambda: upstream
    if panel_service is not None:
        api.dependency_overrides[get_ld_panel_service] = lambda: panel_service
    return call_asgi(api, "GET", path, query=query, disconnect_after_body=True)
