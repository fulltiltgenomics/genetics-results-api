import logging
from fastapi import Depends, FastAPI
from fastapi.routing import APIRoute
from fastapi.responses import JSONResponse
from app.dependencies import auth_required, is_public
from app.middleware import setup_middleware
from app.services import config_util
from app.core.logging_config import setup_logging
from app.routers import (
    auth,
    metadata,
    credible_sets,
    colocalization,
    expression,
    genes,
    gene_disease,
    gene_based,
    chromatin_peaks,
    exome_results,
    search,
    phenotype,
    resources,
)


setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Genetics Results API",
    description="API for accessing genetic association data and annotations. Available resources: "
    + ", ".join(config_util.get_resources()),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={"defaultModelsExpandDepth": -1},  # hide schemas by default
    dependencies=[Depends(auth_required)],
)

setup_middleware(app)

app.include_router(auth.router, prefix="/api/v1", tags=["authentication"])
app.include_router(metadata.router, prefix="/api/v1", tags=["metadata"])
app.include_router(search.router, prefix="/api/v1", tags=["search"])
app.include_router(credible_sets.router, prefix="/api/v1", tags=["credible-sets"])
app.include_router(colocalization.router, prefix="/api/v1", tags=["colocalization"])
app.include_router(expression.router, prefix="/api/v1", tags=["expression"])
app.include_router(genes.router, prefix="/api/v1", tags=["genes"])
app.include_router(gene_disease.router, prefix="/api/v1", tags=["gene-disease"])
app.include_router(gene_based.router, prefix="/api/v1", tags=["gene-based"])
app.include_router(chromatin_peaks.router, prefix="/api/v1", tags=["chromatin-peaks"])
app.include_router(exome_results.router, prefix="/api/v1", tags=["exome-results"])
app.include_router(phenotype.router, prefix="/api/v1", tags=["phenotype"])
app.include_router(resources.router, prefix="/api/v1", tags=["resources"])


def get_all_endpoints():
    """Collect all endpoints from registered routes, organized by tag."""
    endpoints = {}

    endpoints["health"] = "/healthz"
    endpoints["documentation"] = "/docs"
    endpoints["redoc"] = "/redoc"

    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue

        if hasattr(route, "include_in_schema") and not route.include_in_schema:
            continue
        if route.path == "/api/v1":
            continue

        http_methods = sorted(
            [
                m
                for m in getattr(route, "methods", set())
                if m in {"GET", "POST", "PUT", "DELETE", "PATCH"}
            ]
        )
        if not http_methods:
            continue

        tags = getattr(route, "tags", [])
        if not tags:
            tag = "other"
        else:
            tag = tags[0]

        tag_key = tag.replace("-", "_")

        if tag_key not in endpoints:
            endpoints[tag_key] = {}

        path = route.path
        endpoints[tag_key][path] = http_methods

    return endpoints


@app.get("/api/v1", include_in_schema=False)
@is_public
async def root():
    endpoints = get_all_endpoints()
    return JSONResponse(
        {
            "name": "Genetics Results API",
            "status": "ok",
            "endpoints": endpoints,
        }
    )


@app.get("/healthz", include_in_schema=False)
@is_public
async def healthz():
    return JSONResponse(
        content={"status": "ok!"},
        status_code=200,
        headers={
            "Content-Type": "application/json",
            "Cache-Control": "no-cache, no-store, must-revalidate",
        },
    )
