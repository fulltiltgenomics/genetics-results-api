import asyncio
import logging
from typing import AsyncGenerator
from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import StreamingResponse
from app.dependencies import get_gene_name_mapping, ensure_gcs_token
from app.core.exceptions import GeneNotFoundException
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
from app.config.gene_based_results import gene_based_data_files

logger = logging.getLogger(__name__)

router = APIRouter()


async def _run_tabix(data_file: dict, gene_coords: list[dict]) -> bytes:
    """Run tabix for a single data file and return the raw output."""
    file_path = data_file["gene_based"]["file"]
    regions = "\n".join(
        f"{c['chrom']}\t{c['gene_start']}\t{c['gene_end']}" for c in gene_coords
    )
    process = await asyncio.create_subprocess_exec(
        "tabix",
        "-h",
        "-R",
        "/dev/stdin",
        file_path,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd="/tmp/tbi_cache",
    )
    stdout, stderr = await process.communicate(regions.encode())
    if process.returncode != 0:
        raise RuntimeError(
            f"Tabix failed for {data_file['id']} with return code {process.returncode}: {stderr.decode()}"
        )
    return stdout


async def _merge_results(
    data_files: list[dict], results: list[bytes]
) -> AsyncGenerator[bytes, None]:
    """Merge tabix results from multiple data files, emitting header once."""
    header_emitted = False
    for data_file, result in zip(data_files, results):
        if not result:
            continue
        lines = result.split(b"\n")
        for line in lines:
            if not line:
                continue
            if line.startswith(b"#"):
                if not header_emitted:
                    yield line[1:] + b"\n"
                    header_emitted = True
                continue
            yield line + b"\n"


@router.get(
    "/gene_based/{gene}",
    include_in_schema=False,
    responses={
        200: {
            "description": "Successful response",
            "content": {"application/octet-stream": {"schema": {"type": "string"}}},
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Gene not found"},
        500: {"description": "Internal server error"},
    },
)
async def gene_based(
    gene: str = Path(..., description="Gene symbol or comma-separated list of gene symbols", example="BRCA1"),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
    _=Depends(ensure_gcs_token),
):
    """
    Get gene-based burden results for a specific gene or comma-separated list of genes.
    Queries all configured gene-based data sources in parallel and merges results.
    """
    genes = [g.strip() for g in gene.split(",") if g.strip()]
    if not genes:
        raise HTTPException(status_code=422, detail="No valid gene names provided")

    # resolve gene coordinates across all gencode versions
    coords_by_version: dict[int, list[dict]] = {}
    for g in genes:
        try:
            coords = gene_name_and_position_mapping.get_coordinates_by_gene_name(g)
            for version, version_coords in coords.items():
                if version_coords:
                    if version not in coords_by_version:
                        coords_by_version[version] = []
                    coords_by_version[version].extend(version_coords)
                    logger.debug(
                        f"Gene based results for gene {g} (gencode v{version}): {version_coords}"
                    )
        except GeneNotFoundException as e:
            raise HTTPException(status_code=404, detail=str(e))

    # build tabix tasks for each data file that has matching coordinates
    tasks = []
    task_data_files = []
    for data_file in gene_based_data_files:
        gencode_version = data_file["gencode_version"]
        gene_coords = coords_by_version.get(gencode_version, [])
        if not gene_coords:
            continue
        tasks.append(_run_tabix(data_file, gene_coords))
        task_data_files.append(data_file)

    if not tasks:
        raise HTTPException(
            status_code=404,
            detail=f"No gene coordinates found for the given gene(s) in any configured gencode version",
        )

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # check for errors - fail the request if any tabix query failed
    for data_file, result in zip(task_data_files, results):
        if isinstance(result, BaseException):
            logger.error(f"Tabix query failed for {data_file['id']}: {result}")
            raise HTTPException(
                status_code=500,
                detail=f"Error querying gene-based data from {data_file['id']}",
            )

    return StreamingResponse(
        _merge_results(task_data_files, results),
        media_type="application/octet-stream",
    )
