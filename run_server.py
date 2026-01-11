import asyncio
import glob
import os
import sys
import app.config.common as config_common
import app.config.credible_sets as config_credible_sets
from app.core.variant import Variant
from app.core.service_container import container
import uvicorn


def _get_data_access():
    return container.get("data_access")


def _get_data_access_coloc():
    return container.get("data_access_coloc")


def _get_gene_name_mapping():
    return container.get("gene_name_mapping")


def _get_gene_disease_data():
    return container.get("gene_disease_data")


def _validate_metadata_files():
    """Validate that all metadata files can be loaded."""
    from app.services.config_util import get_resources

    data_access = _get_data_access()
    for resource in get_resources():
        try:
            data_access.get_resource_metadata(resource)
        except FileNotFoundError:
            # skip resources without metadata files
            pass


async def _validata_example_phenos():
    """Validate that all example phenos exist."""
    data_access = _get_data_access()
    for df in config_credible_sets.data_files:
        if "example_pheno_or_study" in df:
            await data_access.check_phenotype_exists(
                df["id"],
                df["example_pheno_or_study"],
                95,
            )


async def _validate_range():
    """Validate that a range can be queried across resources."""
    from app.services.config_util import get_resources

    data_access = _get_data_access()
    resources = [resource for resource in get_resources() if resource != "genebass"]
    async for chunk in await data_access.stream_range(
        23,
        1000000,
        2000000,
        resources,
        "cs",
        config_common.read_chunk_size,
        config_common.response_chunk_size,
    ):
        pass


async def _validate_qtl_gene():
    """Validate that a QTL gene can be queried across resources."""
    data_access = _get_data_access()
    gene_name_mapping = _get_gene_name_mapping()
    resources = [
        df["resource"] for df in config_credible_sets.data_files if "resource" in df
    ]
    coords = gene_name_mapping.get_coordinates_by_gene_name("PCSK9")

    async for chunk in await data_access.stream_qtl_gene(
        coords,
        resources,
        "cs",
        config_common.read_chunk_size,
        config_common.response_chunk_size,
        95,
    ):
        pass


async def _validate_coloc():
    """Validate that colocalization can be queried."""
    data_access_coloc = _get_data_access_coloc()
    stream = await data_access_coloc.stream_coloc_by_variant(
        Variant("1:55039974:G:T"),
        config_common.read_chunk_size,
        config_common.response_chunk_size,
    )
    async for chunk in stream:
        pass
    stream = await data_access_coloc.stream_coloc_variants_by_variant(
        Variant("1:55039974:G:T"),
        config_common.read_chunk_size,
        config_common.response_chunk_size,
    )
    async for chunk in stream:
        pass


def _validate_gene_disease():
    """Validate that gene-disease data is loaded."""
    gene_disease_data = _get_gene_disease_data()
    # check that data is loaded and has records
    if gene_disease_data.data.is_empty():
        raise Exception("Gene-disease data is empty")

    # test a lookup
    _ = gene_disease_data.get_by_gene_symbol("BRCA1")


async def _cleanup_services():
    """Clean up aiohttp sessions from data access objects to avoid 'Unclosed client session' warnings."""
    for service_name in ["data_access", "data_access_coloc", "data_access_expression", "data_access_chromatin_peaks"]:
        if container.is_initialized(service_name):
            service = container.get(service_name)
            # clean up any cached resource access objects that have aiohttp sessions
            if hasattr(service, "_resource_access_objects"):
                for obj in service._resource_access_objects.values():
                    if hasattr(obj, "cleanup"):
                        await obj.cleanup()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_server.py <port>")
        sys.exit(1)
    port = int(sys.argv[1])
    try:
        # remove all .tbi files to force re-download in case they have been downloaded but have changed in the cloud since
        for file in glob.glob("*.tbi"):
            os.remove(file)
        _validate_metadata_files()
        asyncio.run(_validata_example_phenos())
        asyncio.run(_validate_range())
        asyncio.run(_validate_qtl_gene())
        asyncio.run(_validate_coloc())
        _validate_gene_disease()
        # clean up aiohttp sessions before resetting container
        asyncio.run(_cleanup_services())
        # use asyncio event loop instead of uvloop - uvloop uses sockets instead of pipes
        # for subprocess stdin, which can break tabix's -R /dev/stdin option (uvloop issue #532)
        uvicorn.run("app.server:app", host="0.0.0.0", port=port, reload=True, loop="asyncio")
    except Exception as e:
        # log error without exposing full traceback to stdout in production
        import logging

        logging.basicConfig(level=logging.ERROR)
        logging.error(f"Server startup failed: {e}", exc_info=True)
        sys.exit(1)
