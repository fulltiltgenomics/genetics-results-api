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
    """
    data_file = gene_based_data_files[0]
    gencode_version = data_file["gencode_version"]
    file_path = data_file["gene_based"]["file"]

    genes = [g.strip() for g in gene.split(",") if g.strip()]
    if not genes:
        raise HTTPException(status_code=422, detail="No valid gene names provided")

    all_gene_coords = []
    for g in genes:
        try:
            coords = gene_name_and_position_mapping.get_coordinates_by_gene_name(g)
            if gencode_version not in coords or not coords[gencode_version]:
                raise GeneNotFoundException(
                    f"Gene {g} not found for gencode version {gencode_version}"
                )
            all_gene_coords.extend(coords[gencode_version])
            logger.debug(
                f"Gene based results for gene {g} (gencode v{gencode_version}): {coords[gencode_version]}"
            )
        except GeneNotFoundException as e:
            raise HTTPException(status_code=404, detail=str(e))

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
    # write regions for all matching gene coordinates
    regions = "\n".join(
        f"{c['chrom']}\t{c['gene_start']}\t{c['gene_end']}" for c in all_gene_coords
    )
    process.stdin.write(regions.encode())
    process.stdin.close()

    async def tabix_iterator() -> AsyncGenerator[bytes, None]:
        first_line_processed = False
        try:
            while True:
                chunk = await process.stdout.read(1024 * 8)
                if not chunk:
                    break

                if not first_line_processed:
                    # remove the # character from the beginning of the first line
                    if chunk.startswith(b"#"):
                        chunk = chunk[1:]
                    first_line_processed = True

                if chunk:
                    yield chunk

            await process.wait()
            if process.returncode != 0:
                stderr = await process.stderr.read()
                raise RuntimeError(
                    f"Tabix failed with return code {process.returncode}: {stderr.decode()}"
                )

        finally:
            if process.returncode is None:
                process.terminate()
                await process.wait()

    return StreamingResponse(tabix_iterator(), media_type="application/octet-stream")
