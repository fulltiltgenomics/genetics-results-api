import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI
from fastapi.routing import APIRoute
from fastapi.responses import JSONResponse
from app.dependencies import auth_required, is_public
from app.middleware import setup_middleware
from app.services import config_util
from app.core.logging_config import setup_logging
from app.core.service_container import container
from app.routers import (
    auth,
    metadata,
    credible_sets,
    colocalization,
    datasets,
    expression,
    genes,
    gene_groups,
    gene_disease,
    gene_based,
    chromatin_peaks,
    open_chromatin,
    variant_effect,
    mpra,
    exome_results,
    search,
    phenotype,
    resources,
    rsid,
    summary_stats,
    variant_annotation,
    variant_set,
)


setup_logging()
logger = logging.getLogger(__name__)


async def _smoke_query_range():
    """Stream a fixed cross-resource range query to confirm the query path works.

    Hits chr23:1M-2M for the "cs" data type across every resource except genebass,
    the same heterogeneous-file merge that recent schema-alignment work touches.
    """
    import app.config.common as config_common

    data_access = container.get("data_access")
    resources = [r for r in config_util.get_resources() if r != "genebass"]
    stream = await data_access.stream_range(
        23,
        1_000_000,
        2_000_000,
        resources,
        "cs",
        config_common.read_chunk_size,
        config_common.response_chunk_size,
    )
    async for _chunk in stream:
        pass


@asynccontextmanager
async def lifespan(app):
    # Warm everything the request path would otherwise load lazily on first use,
    # so no request pays a blocking, network-bound GCS read (which would also wedge
    # the worker and take down /healthz). Everything below runs concurrently and
    # startup takes the slowest branch, not their sum:
    #   - gene-disease data (independent) loads in its own worker thread;
    #   - gene name/position maps (needed by every *_by_gene endpoint and by the
    #     search index) then the phenotype/gene search index load in a second
    #     worker thread — the index depends on the maps, so they share one chain;
    #   - async tabix header/.tbi warming (warm_all) runs on the loop.
    # ServiceContainer.get is thread-safe (per-name locks), so these threads can
    # construct independent singletons in parallel and safely share data_access.
    # This runs in the serving process. Failures abort startup loudly, as intended
    # (per-tabix-file failures are swallowed inside warm_all; verify_all_data_files
    # is the authoritative reachability gate).
    logger.info("Warming services (gene maps, search index, gene-disease, tabix cache)")

    def _preload_search_index():
        container.get("gene_name_mapping")
        container.get("search_index")

    await asyncio.gather(
        asyncio.to_thread(container.get, "gene_disease_data"),
        asyncio.to_thread(_preload_search_index),
        container.get("data_access").warm_all(),
        container.get("data_access_coloc").warm_all(),
        container.get("data_access_expression").warm_all(),
        container.get("data_access_chromatin_peaks").warm_all(),
        container.get("data_access_open_chromatin").warm_all(),
        container.get("data_access_variant_effect").warm_all(),
        container.get("data_access_mpra").warm_all(),
    )
    logger.info("Startup warming complete")
    # end-to-end smoke test of the multi-resource range/merge path on the serving
    # loop. warm_all only opens headers; this exercises cross-resource schema
    # alignment and the concurrent range-read path. raises to abort startup if broken.
    await _smoke_query_range()
    logger.info("Startup smoke query passed")
    yield
    # close aiohttp sessions in all GCloudTabixBase-derived services
    for name, instance in list(container._instances.items()):
        if hasattr(instance, "cleanup"):
            logger.info(f"Cleaning up service: {name}")
            await instance.cleanup()


app = FastAPI(
    title="Genetics Results API",
    description="API for accessing genetic association data and annotations. Available resources: "
    + ", ".join(config_util.get_resources()),
    version="0.1.0",
    docs_url="/api/v1/docs",
    redoc_url="/api/v1/redoc",
    openapi_url="/api/v1/openapi.json",
    swagger_ui_parameters={"defaultModelsExpandDepth": -1},  # hide schemas by default
    dependencies=[Depends(auth_required)],
    lifespan=lifespan,
)

setup_middleware(app)

app.include_router(auth.router, prefix="/api/v1", tags=["authentication"])
app.include_router(metadata.router, prefix="/api/v1", tags=["metadata"])
app.include_router(search.router, prefix="/api/v1", tags=["search"])
app.include_router(credible_sets.router, prefix="/api/v1", tags=["credible-sets"])
app.include_router(colocalization.router, prefix="/api/v1", tags=["colocalization"])
app.include_router(expression.router, prefix="/api/v1", tags=["expression"])
app.include_router(genes.router, prefix="/api/v1", tags=["genes"])
app.include_router(gene_groups.router, prefix="/api/v1", tags=["gene-groups"])
app.include_router(gene_disease.router, prefix="/api/v1", tags=["gene-disease"])
app.include_router(gene_based.router, prefix="/api/v1", tags=["gene-based"])
app.include_router(chromatin_peaks.router, prefix="/api/v1", tags=["chromatin-peaks"])
app.include_router(open_chromatin.router, prefix="/api/v1", tags=["open-chromatin"])
app.include_router(variant_effect.router, prefix="/api/v1", tags=["variant-effect"])
app.include_router(mpra.router, prefix="/api/v1", tags=["mpra"])
app.include_router(exome_results.router, prefix="/api/v1", tags=["exome-results"])
app.include_router(phenotype.router, prefix="/api/v1", tags=["phenotype"])
app.include_router(resources.router, prefix="/api/v1", tags=["resources"])
app.include_router(datasets.router, prefix="/api/v1", tags=["datasets"])
app.include_router(rsid.router, prefix="/api/v1", tags=["rsid"])
app.include_router(summary_stats.router, prefix="/api/v1", tags=["summary-stats"])
app.include_router(variant_annotation.router, prefix="/api/v1", tags=["variant-annotation"])
app.include_router(variant_set.router, prefix="/api/v1", tags=["variant-sets"])


def get_all_endpoints():
    """Collect all endpoints from registered routes, organized by tag."""
    endpoints = {}

    endpoints["health"] = "/healthz"
    endpoints["documentation"] = "/api/v1/docs"
    endpoints["redoc"] = "/api/v1/redoc"

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
        if path in endpoints[tag_key]:
            existing = set(endpoints[tag_key][path])
            existing.update(http_methods)
            endpoints[tag_key][path] = sorted(existing)
        else:
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
