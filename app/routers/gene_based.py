import asyncio
import logging
import time
from typing import AsyncGenerator, Literal
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from fastapi.responses import StreamingResponse
from app.dependencies import get_gene_name_mapping, ensure_gcs_token, get_data_access
from app.core.exceptions import GeneNotFoundException, NotFoundException
from app.core.responses import (
    TimedStreamingResponse,
    TimedJSONResponse,
    columns_header,
)
from app.services.data_access import DataAccess
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
from app.config.gene_based_results import (
    gene_based_data_files,
    gene_based_header_schema,
    resource_to_gene_based_data_file_ids,
)
import app.config.common as config_common

logger = logging.getLogger(__name__)

router = APIRouter()

# each requested trait costs one tabix subprocess per gene-based dataset that has it,
# and genebass alone has 4.5k traits — bound the fan-out of a single request
MAX_TRAITS = 50

_EXAMPLE_TSV_HEADER = "dataset\ttrait\tgene\tgene_id\tgene_chr\tgene_start_pos\tgene_end_pos\tannotation\tmlog10p_burden\tbeta\tse\ttotal_variants\ttotal_variants_pheno\tn_cases\tn_controls\ttrait_original\tflags"
_EXAMPLE_TSV_ROW = "genebass\tOperative procedures - main OPCS4 | S06.8 Other specified other excision of skin\tNOC2L\tENSG00000188976\t1\t944203\t959309\tmissense|LC\t4.83494e+00\t5.95443e-02\t1.37380e-02\t608\t608\t195\t394646\tcategorical_41210_both_sexes_S068_\tNA"

_EXAMPLE_JSON_ROW = {
    "dataset": "genebass",
    "trait": "Operative procedures - main OPCS4 | S06.8 Other specified other excision of skin",
    "gene": "NOC2L",
    "gene_id": "ENSG00000188976",
    "gene_chr": 1,
    "gene_start_pos": 944203,
    "gene_end_pos": 959309,
    "annotation": "missense|LC",
    "mlog10p_burden": 4.83494,
    "beta": 0.0595443,
    "se": 0.013738,
    "total_variants": 608,
    "total_variants_pheno": 608,
    "n_cases": 195,
    "n_controls": 394646,
    "trait_original": "categorical_41210_both_sexes_S068_",
    "flags": None,
}


async def _run_tabix(data_file: dict, gene_coords: list[dict], file_path: str | None = None) -> bytes:
    """Run tabix for a single data file and return the raw output.

    `file_path` overrides the data file's combined all-traits file, so the same
    query can run against one per-trait file instead.
    """
    file_path = file_path or data_file["gene_based"]["file"]
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
    gene: str = Path(..., description="Gene symbol or comma-separated list of gene symbols", examples=["BRCA1"]),
    traits: str | None = Query(
        default=None,
        description=(
            "Comma-separated trait_original values. Without it the combined per-dataset "
            "files are queried, where genebass carries only its mlog10p_burden > 4 hits. "
            "With it the unfiltered per-trait files are queried instead, so a gene with "
            "no significant result in a named trait still comes back."
        ),
    ),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
    data_access: DataAccess = Depends(get_data_access),
    _=Depends(ensure_gcs_token),
):
    """
    Get gene-based burden results for a specific gene or comma-separated list of genes.
    Queries all configured gene-based data sources in parallel and merges results.
    """
    genes = [g.strip() for g in gene.split(",") if g.strip()]
    if not genes:
        raise HTTPException(status_code=422, detail="No valid gene names provided")

    trait_list = [t.strip() for t in traits.split(",") if t.strip()] if traits else []
    if traits is not None and not trait_list:
        raise HTTPException(status_code=422, detail="No valid traits provided")
    if len(trait_list) > MAX_TRAITS:
        raise HTTPException(
            status_code=422,
            detail=f"At most {MAX_TRAITS} traits per request, got {len(trait_list)}",
        )

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
    seen_paths: set[str] = set()
    for data_file in gene_based_data_files:
        gencode_version = data_file["gencode_version"]
        gene_coords = coords_by_version.get(gencode_version, [])
        if not gene_coords:
            continue
        if not trait_list:
            tasks.append(_run_tabix(data_file, gene_coords))
            task_data_files.append(data_file)
            continue
        # one file per requested trait, from whichever resource actually has it.
        # data files of one resource can share a per-trait directory (the three
        # IBD entries do), so the same path must not be queried twice
        cfg = data_file["gene_based"]
        if "prefix" not in cfg:
            continue
        for trait in trait_list:
            path = f"{cfg['prefix']}{trait}{cfg['suffix']}"
            if path in seen_paths:
                continue
            if not await data_access.check_phenotype_exists(
                data_file["resource"], trait, None, "gene_based"
            ):
                continue
            seen_paths.add(path)
            tasks.append(_run_tabix(data_file, gene_coords, path))
            task_data_files.append(data_file)

    if not tasks:
        detail = (
            f"No gene-based data for trait(s) {', '.join(trait_list)}"
            if trait_list
            else "No gene coordinates found for the given gene(s) in any configured gencode version"
        )
        raise HTTPException(status_code=404, detail=detail)

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


@router.get(
    "/gene_based_results_by_phenotype/{resource}/{phenotype_or_study}",
    summary="Get gene burden results for a phenotype or study",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": f"{_EXAMPLE_TSV_HEADER}\n{_EXAMPLE_TSV_ROW}\n...",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {"type": "object"},
                    },
                    "example": [_EXAMPLE_JSON_ROW],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Resource or phenotype not found"},
        422: {"description": "Invalid format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def gene_based_results_by_phenotype(
    request: Request,
    resource: str = Path(..., description="Data resource", examples=["genebass"]),
    phenotype_or_study: str = Path(
        ...,
        description="Phenotype or study code",
        examples=["categorical_41210_both_sexes_S068_"],
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access: DataAccess = Depends(get_data_access),
) -> Response:
    """
    Get every gene burden test result for one phenotype or study.

    Unlike /gene_based/{gene}, which returns the significant hits across all
    traits of a resource, this returns the trait's results for every gene and
    annotation without a p-value cutoff.
    """
    start_time = time.time()
    if resource not in resource_to_gene_based_data_file_ids:
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource {resource}. Available gene-based resources: "
            + ", ".join(resource_to_gene_based_data_file_ids.keys()),
        )

    logger.info(
        f"Getting gene burden results for phenotype or study: {phenotype_or_study} from resource: {resource}"
    )
    try:
        if not await data_access.check_phenotype_exists(
            resource, phenotype_or_study, None, "gene_based"
        ):
            # a client asking for a phenotype we don't have is a 404, not a server fault
            raise NotFoundException(f"File not found: {phenotype_or_study}")
        if format == "tsv":
            stream = await data_access.stream_phenotype(
                resource,
                phenotype_or_study,
                None,
                config_common.read_chunk_size,
                "gene_based",
            )
            return TimedStreamingResponse(
                stream, request.url, start_time, media_type="text/tab-separated-values"
            )
        # the file's own header line, not `gene_based_header_schema` (a validating
        # superset), so the advertised columns are the ones these rows are actually keyed
        # by — the same ground truth range_response uses (genetics-results-suite-8a1)
        header, rows = await data_access.json_phenotype_with_header(
            resource,
            phenotype_or_study,
            None,
            gene_based_header_schema,
            "gene_based",
            config_common.read_chunk_size,
        )
        return TimedJSONResponse(
            rows, request.url, start_time, headers=columns_header(header)
        )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(
            f"Error getting gene burden results for {phenotype_or_study} from {resource}: {e}"
        )
        raise HTTPException(status_code=500, detail="Error streaming gene burden data")
